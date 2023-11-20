package scenarioManager

import (
	"encoding/json"
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/google/uuid"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"scenario-manager/config"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"time"
)

const (
	LoggerTagEBPF = "worker-ebpf"
)

type QueueWorkerEBPF struct {
	id string
	//ebpfConsumer            *stores.TraceQueue
	traceRawDataCollector   *vzReader.VzReader
	eBPFSchemaVersion       string
	traceStore              *stores.TraceStore
	tracePersistenceService *tracePersistence.TracePersistenceService
}

type QueueWorkerGroupEBPF struct {
	worker                []QueueWorkerEBPF
	ebpfConsumer          *stores.TraceQueue
	traceRawDataCollector *vzReader.VzReader
}

func (workers QueueWorkerGroupEBPF) Close() {
	workers.ebpfConsumer.Close()
	workers.traceRawDataCollector.Close()
}

func GetQueueWorkerGroupEBPF(cfg config.AppConfigs, tps *tracePersistence.TracePersistenceService, workerCount int) *QueueWorkerGroupEBPF {

	queueWorkerGroupEBPF := QueueWorkerGroupEBPF{}

	var err error
	workers := make([]QueueWorkerEBPF, 0)
	consumers := make([]rmq.Consumer, 0)

	// initialize raw data collector
	if queueWorkerGroupEBPF.traceRawDataCollector, err = GetNewVZReader(cfg); err != nil {
		zkLogger.Error(LoggerTagEBPF, "Error getting new VZ reader:", err)
		return nil
	}

	// initialize workers
	for i := 0; i < workerCount; i++ {
		// initialize worker
		worker := QueueWorkerEBPF{
			id:                      "E" + uuid.New().String(),
			eBPFSchemaVersion:       cfg.ScenarioConfig.EBPFSchemaVersion,
			tracePersistenceService: tps,
			traceStore:              stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
		}
		worker.traceRawDataCollector = queueWorkerGroupEBPF.traceRawDataCollector

		workers = append(workers, worker)
		consumers = append(consumers, worker)
	}

	// initialize ebpf trace consumer queue
	if queueWorkerGroupEBPF.ebpfConsumer, err = stores.GetTraceConsumer(cfg.Redis, consumers, ebpfConsumerName); err != nil {
		queueWorkerGroupEBPF.traceRawDataCollector.Close()
		return nil
	}

	return &queueWorkerGroupEBPF
}

func (worker QueueWorkerEBPF) handleMessage(traceMessage EBPFTraceMessage) bool {

	timeRange := timeRangeForRawDataQuery

	// get all the traceIds from the message and put them in a map and an array for processing
	traceIds := make([]string, 0)
	tracesFromOTel := make(map[string]TraceFromOTel)
	for _, trace := range traceMessage.Traces {
		traceIds = append(traceIds, trace.TraceId)
		tracesFromOTel[trace.TraceId] = trace
	}

	unprocessedIds := worker.getUnprocessedTraces(traceIds)
	if len(unprocessedIds) == 0 {
		zkLogger.DebugF(LoggerTagEBPF, "Got %d traces which were already processed. Not processing them again. TraceIds :", len(unprocessedIds), unprocessedIds)
		return true
	}

	spansFromEBPFStore := worker.collectHTTPRawData(unprocessedIds, timeRange)
	if len(spansFromEBPFStore) == 0 {
		zkLogger.DebugF(LoggerTagEBPF, "No raw data received from vzReader for %v spans. TraceIds :", len(unprocessedIds), unprocessedIds)
		return true
	}

	spansForUpdatingEBPFData := make([]tracePersistenceModel.Span, 0)
	spansToAddInOTel := make([]tracePersistenceModel.Span, 0)
	spansUpdateIsRootOTel := make([]tracePersistenceModel.Span, 0)

	for _, spanWithRawDataFromPixie := range spansFromEBPFStore {

		traceIdEbpfSpan := spanWithRawDataFromPixie.TraceId
		spanIdEbpfSpan := spanWithRawDataFromPixie.SpanId

		traceOTel, ok := tracesFromOTel[traceIdEbpfSpan]
		if !ok {
			continue
		}

		spanForPersistence := worker.getSpanForPersistence(spanWithRawDataFromPixie)

		// MISSING CLIENT SPAN FOR THE ROOT SERVER SPAN:
		//----------------------------------------------
		// there could be new span present in the ebpf store which is not present in the oTel store. This is the span
		// which is the root span for the trace. It should be of the type `client` while the current root span in the
		// oTel store is of type `server`. We need to update the root span in the oTel store to be of type client.
		// This client was not captured by oTel because it didn't have the trace id in the request header. The traceId
		// was added later in the header by oTel and hence EBPF layer captured it while the response was going out.
		if traceOTel.RootSpanId != "" && traceOTel.RootSpanKind == SERVER && traceOTel.RootSpanParent == spanIdEbpfSpan {

			// mark the current root span as non-root
			spansUpdateIsRootOTel = append(spansUpdateIsRootOTel, *worker.getSpanForUpdateRoot(traceIdEbpfSpan, traceOTel.RootSpanId, false))

			// add the new span as root
			spanForPersistence.IsRoot = true
			spanForPersistence.Kind = CLIENT
			spansToAddInOTel = append(spansToAddInOTel, spanForPersistence)
		}

		spansForUpdatingEBPFData = append(spansForUpdatingEBPFData, spanForPersistence)
	}

	// save spans in trace persistence store
	tps := *worker.tracePersistenceService

	// a. save new spans
	if len(spansToAddInOTel) > 0 {
		tps.SaveSpan(spansToAddInOTel)
	} else {
		zkLogger.Info(LoggerTagEBPF, "No new spans to save")
	}

	// b. update existing spans to remove the isRoot flag
	if len(spansUpdateIsRootOTel) > 0 {
		tps.UpdateIsRootSpan(spansUpdateIsRootOTel)
	} else {
		zkLogger.Info(LoggerTagEBPF, "No new root spans to update")
	}

	// c. store request-response headers and body
	saveError := tps.SaveEBPFData(spansForUpdatingEBPFData)
	if saveError != nil {
		zkLogger.Error(LoggerTagEBPF, "Error saving EBPF data", saveError)
		return false
	}

	worker.setProcessedTraces(unprocessedIds)

	return true
}

func (worker QueueWorkerEBPF) getSpanForUpdateRoot(traceId, spanId string, isRoot bool) *tracePersistenceModel.Span {
	oTelSpanForPersistence := tracePersistenceModel.Span{
		TraceID: traceId,
		SpanID:  spanId,
		IsRoot:  isRoot,
	}
	return &oTelSpanForPersistence
}

func (worker QueueWorkerEBPF) getSpanForPersistence(ebpfSpan models.HttpRawDataModel) tracePersistenceModel.Span {
	oTelSpanForPersistence := &tracePersistenceModel.Span{
		TraceID: ebpfSpan.TraceId,
		SpanID:  ebpfSpan.SpanId,
	}
	oTelSpanForPersistence = enrichSpanFromHTTPRawData(oTelSpanForPersistence, &ebpfSpan, worker.eBPFSchemaVersion)

	return *oTelSpanForPersistence
}

func (worker QueueWorkerEBPF) collectRawData(traceIds []string, startTime string) []models.HttpRawDataModel {
	rawData, err := worker.traceRawDataCollector.GetHTTPRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTagEBPF, "Error getting raw spans for http traces ", traceIds, err)
		return make([]models.HttpRawDataModel, 0)
	}
	return rawData.Results
}

func (worker QueueWorkerEBPF) collectHTTPRawData(traceIds []string, startTime string) []models.HttpRawDataModel {
	rawData, err := worker.traceRawDataCollector.GetHTTPRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTagEBPF, "Error getting raw spans for http traces ", traceIds, err)
		return make([]models.HttpRawDataModel, 0)
	}
	return rawData.Results
}

func (worker QueueWorkerEBPF) Consume(delivery rmq.Delivery) {

	zkLogger.DebugF(LoggerTagEBPF, "ebpf worker %v got a message", worker.id)
	var traceMessage EBPFTraceMessage
	if err := json.Unmarshal([]byte(delivery.Payload()), &traceMessage); err != nil {
		// handle json error
		if err = delivery.Reject(); err != nil {
			// not sure what to do here
		}
		return
	}

	// perform task
	zkLogger.DebugF(LoggerTagEBPF, "got message %v", traceMessage)
	handled := worker.handleMessage(traceMessage)
	if !handled {
		if err := delivery.Reject(); err != nil {
			// not sure what to do here
		}
		return
	}

	if err := delivery.Ack(); err != nil {
		// handle ack error
	}
}

func (worker QueueWorkerEBPF) getUnprocessedTraces(tracesToProcess []string) []string {

	// mark all traceIds as processed in redis
	tempSetName := fmt.Sprintf("%s_%s_%d", SetPrefixEBPFTemp, worker.id, time.Now().UnixMilli())
	defer worker.traceStore.DeleteSets([]string{tempSetName})

	tracesToProcessI := make([]interface{}, 0)
	for _, traceId := range tracesToProcess {
		tracesToProcessI = append(tracesToProcessI, traceId)
	}

	// create this set with all the traceIds
	if err := worker.traceStore.Add(tempSetName, tracesToProcessI); err != nil {
		return tracesToProcess
	}

	// remove all the already processed traces from the set of traces to process
	if keys, err := worker.traceStore.GetAllKeysWithPrefixAndRegex(SetPrefixEBPFProcessed, ".*"); err == nil || len(keys) > 0 {
		processedTracesAggregationKey := fmt.Sprintf("%s_%s", SetPrefixEBPFProcessedAggregate, worker.id)
		if ok := worker.traceStore.NewUnionSet(processedTracesAggregationKey, keys...); ok {
			// delete the temporary set after the completing of this process
			defer worker.traceStore.DeleteSets([]string{tempSetName})
			tracesToProcess = worker.traceStore.GetValuesAfterSetDiff(tempSetName, processedTracesAggregationKey)
		}
	}

	return tracesToProcess
}

func (worker QueueWorkerEBPF) setProcessedTraces(tracesProcessed []string) {

	// mark all traceIds as processed in redis
	setName := fmt.Sprintf("%s_%s_%d", SetPrefixEBPFProcessed, worker.id, time.Now().UnixMilli())

	membersToAdd := make([]interface{}, 0)
	for _, traceId := range tracesProcessed {
		membersToAdd = append(membersToAdd, string(traceId))
	}
	err := worker.traceStore.Add(setName, membersToAdd)
	if err != nil {
		zkLogger.DebugF(LoggerTagEBPF, "Error marking traceIds as processed in set %s : %v", setName, err)
	}
	worker.traceStore.SetExpiryForSet(setName, TTLForScenarioSets)
}
