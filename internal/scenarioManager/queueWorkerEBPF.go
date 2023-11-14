package scenarioManager

import (
	"encoding/json"
	"github.com/adjust/rmq/v5"
	"github.com/google/uuid"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"scenario-manager/config"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
)

type QueueWorkerEBPF struct {
	id                      string
	ebpfConsumer            *stores.TraceQueue
	traceRawDataCollector   *vzReader.VzReader
	eBPFSchemaVersion       string
	tracePersistenceService *tracePersistence.TracePersistenceService
}

func GetQueueWorkerEBPF(cfg config.AppConfigs, tps *tracePersistence.TracePersistenceService) *QueueWorkerEBPF {

	var err error

	// initialize worker
	worker := QueueWorkerEBPF{
		id:                      "E" + uuid.New().String(),
		eBPFSchemaVersion:       cfg.ScenarioConfig.EBPFSchemaVersion,
		tracePersistenceService: tps,
	}
	worker.traceRawDataCollector, err = GetNewVZReader(cfg)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting new VZ reader:", err)
		return nil
	}

	// oTel consumer and error store
	worker.ebpfConsumer, err = stores.GetTraceConsumer(cfg.Redis, &worker, ebpfConsumerName)
	if err != nil {
		return nil
	}

	return &worker
}

func (worker *QueueWorkerEBPF) Close() {
	worker.ebpfConsumer.Close()
	worker.traceRawDataCollector.Close()
}

func (worker *QueueWorkerEBPF) handleMessage(traceMessage EBPFTraceMessage) {

	timeRange := timeRangeForRawDataQuery

	tracesFromOTel := make(map[string]TraceFromOTel)
	traceIds := make([]string, 0)
	for _, trace := range traceMessage.Traces {
		traceIds = append(traceIds, trace.TraceId)
		tracesFromOTel[trace.TraceId] = trace
	}

	spansFromEBPFStore := worker.collectHTTPRawData(traceIds, timeRange)
	zkLogger.InfoF(LoggerTag, "Number of spans with HTTP raw data: %v", len(spansFromEBPFStore))

	spansToUpdateInOTel := make([]tracePersistenceModel.Span, 0)

	for _, spanWithRawDataFromPixie := range spansFromEBPFStore {

		traceId := spanWithRawDataFromPixie.TraceId
		spanId := spanWithRawDataFromPixie.SpanId

		trace, ok := tracesFromOTel[traceId]
		if !ok {
			continue
		}

		if trace.RootSpanId != "" && trace.RootSpanKind == SERVER && trace.RootSpanParent == spanId {
			spansToUpdateInOTel = append(spansToUpdateInOTel, worker.getSpanForPersistence(spanWithRawDataFromPixie))
		}
	}

	//TODO: 1. save new spans 2. update existing spans so as to remove the isRoot flag

	// store request-response headers and body
	saveError := (*worker.tracePersistenceService).SaveEBPFData(spansToUpdateInOTel)
	if saveError != nil {
		zkLogger.Error(LoggerTag, "Error saving EBPF data", saveError)
		return
	}
}

func (worker *QueueWorkerEBPF) getSpanForPersistence(ebpfSpan models.HttpRawDataModel) tracePersistenceModel.Span {
	oTelSpanForPersistence := &tracePersistenceModel.Span{
		TraceID:      ebpfSpan.TraceId,
		SpanID:       ebpfSpan.SpanId,
		IsRoot:       true,
		ParentSpanID: "",
		Kind:         CLIENT,
	}
	oTelSpanForPersistence = enrichSpanFromHTTPRawData(oTelSpanForPersistence, &ebpfSpan, worker.eBPFSchemaVersion)

	return *oTelSpanForPersistence
}

func (worker *QueueWorkerEBPF) collectRawData(traceIds []string, startTime string) []models.HttpRawDataModel {
	rawData, err := worker.traceRawDataCollector.GetHTTPRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for http traces ", traceIds, err)
		return make([]models.HttpRawDataModel, 0)
	}
	return rawData.Results
}

func (worker *QueueWorkerEBPF) collectHTTPRawData(traceIds []string, startTime string) []models.HttpRawDataModel {
	rawData, err := worker.traceRawDataCollector.GetHTTPRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for http traces ", traceIds, err)
		return make([]models.HttpRawDataModel, 0)
	}
	return rawData.Results
}

func (worker *QueueWorkerEBPF) Consume(delivery rmq.Delivery) {

	zkLogger.DebugF(LoggerTag, "ebpf worker %v got a message", worker.id)
	var traceMessage EBPFTraceMessage
	if err := json.Unmarshal([]byte(delivery.Payload()), &traceMessage); err != nil {
		// handle json error
		if err = delivery.Reject(); err != nil {
			// not sure what to do here
		}
		return
	}

	// perform task
	zkLogger.DebugF(LoggerTag, "got message %v", traceMessage)
	worker.handleMessage(traceMessage)

	if err := delivery.Ack(); err != nil {
		// handle ack error
	}
}
