package scenarioManager

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/google/uuid"
	zkUtilsCommonModel "github.com/zerok-ai/zk-utils-go/common"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/proto/enrichedSpan"
	"github.com/zerok-ai/zk-utils-go/scenario"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	zkRedis "github.com/zerok-ai/zk-utils-go/storage/redis"
	pb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	otlpCommonV1 "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/resource/v1"
	otlpTraceV1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"scenario-manager/config"
	typedef "scenario-manager/internal"
	promMetrics "scenario-manager/internal/prometheusMetrics"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"sync"
	"time"
)

const (
	TitleDelimiter = "¦"
	LoggerTagOTel  = "worker-oTel"
)

var ctx = context.Background()

type TMapOfSpanIdToSpan map[typedef.TSpanId]*stores.SpanFromOTel

type QueueWorkerOTel struct {
	id             string
	oTelStore      *stores.OTelDataHandler
	attributeStore *stores.AttributesStore
	errorStore     *stores.ErrorStore
	traceStore     *stores.TraceStore
	oTelConsumer   *stores.TraceQueue
	ebpfProducer   *stores.TraceQueue
	scenarioStore  *zkRedis.VersionedStore[model.Scenario]
	exporter       config.Exporter
}

func GetQueueWorkerOTel(cfg config.AppConfigs, scenarioStore *zkRedis.VersionedStore[model.Scenario]) *QueueWorkerOTel {

	var workers []*QueueWorkerOTel
	numOtelQueueWorkers := cfg.OtelQueueWorkerCount
	if numOtelQueueWorkers <= 0 {
		numOtelQueueWorkers = 1
	}

	for i := 0; i < numOtelQueueWorkers; i++ {
		worker := QueueWorkerOTel{
			id:             fmt.Sprintf("%d%s", i, uuid.New().String()),
			oTelStore:      stores.GetOTelStore(cfg.Redis),
			traceStore:     stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
			attributeStore: stores.GetAttributesStore(cfg.Redis),
			errorStore:     stores.GetErrorStore(cfg.Redis),
			scenarioStore:  scenarioStore,
			exporter:       cfg.Exporter,
		}
		workers = append(workers, &worker)
	}

	// Otel consumer and error store
	var err error
	workerInterfaces := make([]rmq.Consumer, len(workers))
	for i, worker := range workers {
		workerInterfaces[i] = worker
	}

	firstWorker := workers[0]

	firstWorker.oTelConsumer, err = stores.GetTraceConsumer(cfg.Redis, workerInterfaces, OTelQueue)
	if err != nil {
		return nil
	}

	return firstWorker
}

func (worker *QueueWorkerOTel) Close() {
	worker.oTelStore.Close()
	worker.attributeStore.Close()
	worker.errorStore.Close()
	worker.traceStore.Close()
	worker.oTelConsumer.Close()
}

func (worker *QueueWorkerOTel) handleMessage(oTelMessage OTELTraceMessage) {

	//zkLogger.Error(LoggerTagOTel, "11111111 oTelWorker got a message", oTelMessage)
	//zkLogger.DebugF(LoggerTagOTel, "oTelWorker %v got a message", worker.id)

	// 1. Collect span relation and span data for the traceIDs
	tracesFromOTelStore, resourceHashToInfoMap, scopeHashToInfoMap := worker.getDataFromOTelStore(oTelMessage.Traces)
	if tracesFromOTelStore == nil || len(tracesFromOTelStore) == 0 {
		return
	}

	// 2. Process each trace against all the scenarioWithTraces
	incidents := worker.buildIncidentsForPersistence(&oTelMessage.Scenario, tracesFromOTelStore)
	if incidents == nil || len(incidents) == 0 {
		zkLogger.ErrorF(LoggerTagOTel, "no incidents to save")
		return
	}

	//total traces received for scenario to process metric
	promMetrics.TotalTracesReceivedForScenarioToProcess.WithLabelValues(oTelMessage.Scenario.Title).Add(float64(len(incidents)))

	//TODO :: rate limit logic should be written before fetching the span data
	// 3. rate limit incidents
	//newIncidentList := worker.rateLimitIncidents(incidents, oTelMessage.Scenario)
	newIncidentList := incidents
	totalTracesRateLimited := len(incidents) - len(newIncidentList)
	promMetrics.RateLimitedTotalIncidentsPerScenario.WithLabelValues(oTelMessage.Scenario.Title).Add(float64(totalTracesRateLimited))

	if len(newIncidentList) == 0 {
		zkLogger.InfoF(LoggerTagOTel, "rate limited %d incidents. nothing to save", len(incidents))
		return
	}

	for _, incident := range newIncidentList {
		if len(tracesFromOTelStore[typedef.TTraceid(incident.Incident.TraceId)].Spans) != len(incident.Incident.Spans) {
			promMetrics.SpanCountMismatchTotal.WithLabelValues(oTelMessage.Scenario.Title, incident.Incident.TraceId).Inc()
			zkLogger.ErrorF(LoggerTagOTel, "span count mismatch for incident %v", incident)
			zkLogger.ErrorF(LoggerTagOTel, "OtelCount: %d, newIncidentCount: %d", len(tracesFromOTelStore[typedef.TTraceid(incident.Incident.TraceId)].Spans), len(incident.Incident.Spans))
		}
	}

	//for _, incident := range newIncidentList {
	//	isRootSpanPresent := false
	//	for _, span := range incident.Incident.Spans {
	//		if span.IsRoot {
	//			isRootSpanPresent = true
	//			break
	//		}
	//	}
	//	if !isRootSpanPresent {
	//		zkLogger.ErrorF(LoggerTagOTel, "no root span found for incident %v", incident)
	//	}
	//}

	// 3.1 move scenario id to root span
	for incidentIndex := 0; incidentIndex < len(newIncidentList); incidentIndex++ {
		incident := &newIncidentList[incidentIndex]
		var rootSpan *stores.SpanFromOTel
		allWorkloadIdsInTrace := make(ds.Set[string])
		allGroupByTitleSet := make(ds.Set[string])
		for spanIndex := 0; spanIndex < len(incident.Incident.Spans); spanIndex++ {
			span := incident.Incident.Spans[spanIndex]
			if span.WorkloadIDList != nil && len(span.WorkloadIDList) != 0 {
				workloadIdSet := make(ds.Set[string])
				for _, workloadId := range span.WorkloadIDList {
					workloadIdSet.Add(workloadId)
				}
				span.SpanAttributes["workload_id_list"] = workloadIdSet
				allWorkloadIdsInTrace = allWorkloadIdsInTrace.Union(workloadIdSet)
				allGroupByTitleSet = allGroupByTitleSet.Union(span.GroupByTitleSet)
			}

			if span.IsRoot {
				rootSpan = span
			}
		}

		if rootSpan == nil {
			promMetrics.RootSpanNotFoundTotal.WithLabelValues(oTelMessage.Scenario.Title, incident.Incident.TraceId).Inc()
			zkLogger.ErrorF(LoggerTagOTel, "no root span found for incident %v", incident)
			continue
		}

		scenarios := worker.scenarioStore.GetAllValues()
		probesName, err := scenario.FindMatchingScenarios(allWorkloadIdsInTrace.GetAll(), scenarios)
		if err != nil {
			zkLogger.Error(LoggerTagOTel, "error in getting scenario ids for workload ids", err)
		} else {
			rootSpan.SpanAttributes["probes"] = probesName
		}

		rootSpan.SpanAttributes["GroupByTitleSet"] = allGroupByTitleSet
	}

	// 4. save attributes details
	spanBuffer := make([]*stores.SpanFromOTel, 0)
	for _, incident := range newIncidentList {
		for _, span := range incident.Incident.Spans {
			span.Span.Attributes = enrichedSpan.ConvertMapToKVList(span.SpanAttributes).KeyValueList
			spanBuffer = append(spanBuffer, span)
		}
	}
	resourceBuffer := ConvertOtelSpanToResourceSpan(spanBuffer, resourceHashToInfoMap, scopeHashToInfoMap)

	var wg sync.WaitGroup
	batchSize := 20
	bufferLen := len(resourceBuffer)
	totalSpans := 0
	// count total spans in the resourceBuffer
	for _, resourceSpan := range resourceBuffer {
		for _, scopeSpan := range resourceSpan.ScopeSpans {
			totalSpans += len(scopeSpan.Spans)
		}
	}
	promMetrics.TotalSpansSentToCollector.Add(float64(totalSpans))

	for i := 0; i < bufferLen; i += batchSize {
		wg.Add(1)

		go func(startIndex, endIndex int) {
			defer wg.Done()
			sendDataToCollector(resourceBuffer[startIndex:endIndex], worker, oTelMessage)
		}(i, getMin(i+batchSize, bufferLen))
	}
	wg.Wait()

	//total traces processed by scenario manager for scenario
	promMetrics.TotalTracesProcessedForScenario.WithLabelValues(oTelMessage.Scenario.Title).Add(float64(len(newIncidentList)))
	//total spans processed by scenario manager for scenario
	promMetrics.TotalSpansProcessedForScenario.WithLabelValues(oTelMessage.Scenario.Title).Add(float64(len(spanBuffer)))
}

// TODO :: should move to utils repo
func getMin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func sendDataToCollector(resourceSpans []*otlpTraceV1.ResourceSpans, worker *QueueWorkerOTel, oTelMessage OTELTraceMessage) {
	var tracesData pb.ExportTraceServiceRequest
	tracesData.ResourceSpans = resourceSpans
	fmt.Println("Testing")
	// Set up a connection to the server
	url := fmt.Sprintf("%s:%s", worker.exporter.Host, worker.exporter.Port)
	zkLogger.Info(LoggerTagOTel, "Connecting to ", url)
	conn, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	//conn, err := grpc.Dial("localhost:4319", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()
	c := pb.NewTraceServiceClient(conn)

	// Contact the server and print out its response.
	response, err := c.Export(context.Background(), &tracesData)
	if err != nil {
		//total call to export data failed for scenario
		promMetrics.TotalExportDataFailedForScenario.WithLabelValues(oTelMessage.Scenario.Title).Inc()
		//log.Fatalf("could not send trace data: %v", err)
		zkLogger.Error(LoggerTagOTel, "could not send trace data to otel from sm", err)
	}
	log.Printf("Response: %v", response)
}

func ConvertOtelSpanToResourceSpan(spans []*stores.SpanFromOTel, resourceHashToInfoMap, scopeHashToInfoMap map[string]map[string]interface{}) []*otlpTraceV1.ResourceSpans {

	resourceMap := make(map[string]map[string][]stores.SpanFromOTel)

	// make a map from resourceHash to scopeHash to spans
	for _, span := range spans {
		if scopeMap, resourceHashFound := resourceMap[span.ResourceAttributesHash]; resourceHashFound {
			spanList := make([]stores.SpanFromOTel, 0)
			if s, scopeHashFound := scopeMap[span.ScopeAttributesHash]; scopeHashFound {
				spanList = s
			} else {
				spanList = make([]stores.SpanFromOTel, 0)
			}
			scopeMap[span.ScopeAttributesHash] = append(spanList, *span)
		} else {
			spanList := make([]stores.SpanFromOTel, 0)
			resourceMap[span.ResourceAttributesHash] = map[string][]stores.SpanFromOTel{
				span.ScopeAttributesHash: append(spanList, *span),
			}
		}
	}

	resourceSpans := make([]*otlpTraceV1.ResourceSpans, 0)

	resourceHashToAttr := make(map[string][]*otlpCommonV1.KeyValue)
	scopeHashToAttr := make(map[string][]*otlpCommonV1.KeyValue)

	for key, value := range resourceHashToInfoMap {
		if value["attributes_map"] == nil {
			continue
		}
		resourceHashToAttr[key] = enrichedSpan.ConvertMapToKVList(value["attributes_map"].(map[string]interface{})).KeyValueList
	}

	for key, value := range scopeHashToInfoMap {
		if value["attributes_map"] == nil {
			continue
		}
		scopeHashToAttr[key] = enrichedSpan.ConvertMapToKVList(value["attributes_map"].(map[string]interface{})).KeyValueList
	}

	for resourceHash, scopeMap := range resourceMap {
		scopeSpansList := make([]*otlpTraceV1.ScopeSpans, 0)
		if resourceHashToInfoMap[resourceHash]["schema_url"] == nil {
			zkLogger.ErrorF(LoggerTagOTel, "schema_url not found for resourceHash %s", resourceHash)
		}

		resource := otlpTraceV1.ResourceSpans{
			Resource: &v1.Resource{
				Attributes: resourceHashToAttr[resourceHash],
			},
			SchemaUrl: resourceHashToInfoMap[resourceHash]["schema_url"].(string),
		}
		for scopeHash, spanList := range scopeMap {
			scopeSpans := otlpTraceV1.ScopeSpans{
				Scope: &otlpCommonV1.InstrumentationScope{
					Attributes: scopeHashToAttr[scopeHash],
					//TODO :: revert below line
					//Name:       scopeHashToInfoMap[scopeHash]["name"].(string),
					//Version:    scopeHashToInfoMap[scopeHash]["version"].(string),
					Name:    "scopeHashToInfoMap[scopeHash][name].(string)",
					Version: "scopeHashToInfoMap[scopeHash][version].(string)",
				},
				//SchemaUrl: scopeHashToInfoMap[scopeHash]["schema_url"].(string),
				SchemaUrl: "scopeHashToInfoMap[scopeHash][schema_url].(string)",
			}
			scopeSpans.Spans = make([]*otlpTraceV1.Span, 0)
			for _, span := range spanList {
				scopeSpans.Spans = append(scopeSpans.Spans, span.Span)
			}
			scopeSpansList = append(scopeSpansList, &scopeSpans)
		}
		resource.ScopeSpans = scopeSpansList
		resourceSpans = append(resourceSpans, &resource)
	}

	return resourceSpans
}

func (worker *QueueWorkerOTel) getDataFromOTelStore(traceIds []typedef.TTraceid) (map[typedef.TTraceid]*stores.TraceFromOTel, map[string]map[string]interface{}, map[string]map[string]interface{}) {
	tracesFromOTelStore, err := worker.oTelStore.GetSpansForTracesFromDB(traceIds)
	if err != nil {
		zkLogger.Error(LoggerTagOTel, "error in getting data from OTel zkRedis", err)
	}

	resourceHashToInfoMap, scopeHashToInfoMap := worker.attributeStore.GetResourceScopeInfoForHashes(tracesFromOTelStore)
	tracesFromOTelStore = worker.errorStore.GetExceptionDataForHashes(tracesFromOTelStore)

	return tracesFromOTelStore, resourceHashToInfoMap, scopeHashToInfoMap
}

func (worker *QueueWorkerOTel) buildIncidentsForPersistence(scenario *model.Scenario, tracesFromOTel map[typedef.TTraceid]*stores.TraceFromOTel) []tracePersistenceModel.IncidentWithIssues {

	//zkLogger.DebugF(LoggerTagOTel, "Building scenario for persistence, trace_count=%d", len(tracesFromOTel))

	// a. iterate through the trace data from OTelDataHandler and build the structure which can be saved in DB
	incidentsWithIssues := make([]tracePersistenceModel.IncidentWithIssues, 0)
	traceTreeForPersistence := make(map[typedef.TTraceid]*TMapOfSpanIdToSpan)
	for traceId, traceFromOTel := range tracesFromOTel {
		spanMapOfPersistentSpans := make(TMapOfSpanIdToSpan)
		for _, spanFromOTel := range traceFromOTel.Spans {
			spanMapOfPersistentSpans[spanFromOTel.SpanID] = spanFromOTel
		}
		traceTreeForPersistence[traceId] = &spanMapOfPersistentSpans

		// evaluate this trace
		incidents := worker.evaluateIncidents(traceId, scenario, spanMapOfPersistentSpans)
		incidentsWithIssues = append(incidentsWithIssues, incidents)
	}

	return incidentsWithIssues
}

func (worker *QueueWorkerOTel) evaluateIncidents(traceId typedef.TTraceid, scenario *model.Scenario, spansOfTrace TMapOfSpanIdToSpan) tracePersistenceModel.IncidentWithIssues {

	spans := make([]*stores.SpanFromOTel, 0)
	for key := range spansOfTrace {
		spans = append(spans, spansOfTrace[key])
	}

	issueGroupList := make([]tracePersistenceModel.IssueGroup, 0)

	listOfIssues := getListOfIssuesForScenario(scenario, spansOfTrace)
	if len(listOfIssues) > 0 {
		issueGroupList = append(issueGroupList, tracePersistenceModel.IssueGroup{
			ScenarioId:      scenario.Id,
			ScenarioVersion: scenario.Version,
			Issues:          listOfIssues,
		})
	}

	incidentWithIssues := tracePersistenceModel.IncidentWithIssues{
		IssueGroupList: issueGroupList,
		Incident: tracePersistenceModel.Incident{
			TraceId:                string(traceId),
			Spans:                  spans,
			IncidentCollectionTime: time.Now().UTC(),
		},
	}

	return incidentWithIssues
}

func getListOfIssuesForScenario(scenario *model.Scenario, spanMap TMapOfSpanIdToSpan) []tracePersistenceModel.Issue {

	/* Logic with example

	Scenario workloads
	---------------------------
	w1 - service `*` returns 5xx
	w2 - service `1` returns 400

	Group-by rules
	---------------------------
	 - W2.status-code
	 - W1.size
	 - w1.service-name

	Situation for the trace
	---------------------------
	spans:		 			 s1,  s2,  s3
	status-code				400, 500, 501
	workload satisfied		 w2,  w1,  w1


	Logic to solve
	---------------------------
	1. create a set of workload ids used in `group-by`. If no `group-by` is present, use all the workload ids??
	2. loop over spans and create a map of workload to []span
			w2[s1]
			W1[s2, s3]

	3. cross product to create an array of workload-span map
			[w2:s1,w1:s2][w2:s1,w1:s3]
			 --- map ---  --- map ---
			-------array--------------

	4. loop over each element of the array from the previous step and use Group-by rules to create map of issues
	*/

	// 1. create a set of used workload ids
	workloadIdListInGroup := make(ds.Set[typedef.TWorkloadId])

	// this if condition is for the cases where there is no group by, so we take all the workload ids in the scenario and add it to the list
	if scenario.GroupBy == nil || len(scenario.GroupBy) == 0 {
		if scenario.Workloads != nil || len(*scenario.Workloads) != 0 {
			for k := range *scenario.Workloads {
				workloadIdListInGroup.Add(typedef.TWorkloadId(k))
			}
		}
	} else {
		for _, group := range scenario.GroupBy {
			workloadIdListInGroup.Add(typedef.TWorkloadId(group.WorkloadId))
		}
	}

	// 2. create a set of workloadIds vs spans
	workloadIdToSpanArrayMap := make(stores.TWorkLoadIdToSpanArray)
	for _, span := range spanMap {
		workloadIdList := span.WorkloadIDList
		for _, workloadId := range workloadIdList {
			spans, ok := workloadIdToSpanArrayMap[typedef.TWorkloadId(workloadId)]
			if !ok {
				spans = make([]*stores.SpanFromOTel, 0)
			}
			workloadIdToSpanArrayMap[typedef.TWorkloadId(workloadId)] = append(spans, span)
		}
	}

	// 3. do a cartesian product of all the elements in workloadIdSet. This gives us all the possible combinations of
	//	workloads-span groups that can be give rise to a unique issues
	issueSource := make([]map[typedef.TWorkloadId]*stores.SpanFromOTel, 0)
	for workloadId := range workloadIdListInGroup {
		arrSpans := workloadIdToSpanArrayMap[workloadId]
		issueSource = getCartesianProductOfSpans(issueSource, arrSpans, workloadId)
	}

	// 4. iterate through issueSource and evaluate each groupBy clause
	issueMap := make(map[typedef.TIssueHash]tracePersistenceModel.Issue)
	for _, mapOfWorkloadIdToSpans := range issueSource {

		// 4.a Initialize with default values
		hash := scenario.Id + scenario.Version
		title := scenario.Title
		for index, group := range scenario.GroupBy {

			// 4.a.1. get workload id from scenario object
			workloadId := group.WorkloadId

			// 4.a.2. get span for the workloadId
			span, ok := mapOfWorkloadIdToSpans[typedef.TWorkloadId(workloadId)]
			if !ok {
				continue
			}

			// 4.a.3. get the group_by object from span for the current scenario
			groupByForScenario, ok := span.GroupByMap[zkUtilsCommonModel.ScenarioId(scenario.Id)]
			if !ok {
				//not sure why would this happen
				continue
			}

			// 4.a.4. The indices of groupByForScenario and scenario.GroupBy should be same.
			// 			get the group_by object from the span at the current index.
			groupBy := groupByForScenario[index]
			hash += TitleDelimiter
			title += TitleDelimiter
			if groupBy == nil {
				// not sure why this will happen
			} else {
				hash += groupBy.Hash
				title += groupBy.Title
			}

		}

		// 5. add the new issue to the issueMap (this data will be returned from the function)
		md5OfHash := md5.Sum([]byte(hash))
		hash = hex.EncodeToString(md5OfHash[:])
		issueMap[typedef.TIssueHash(hash)] = tracePersistenceModel.Issue{
			IssueHash:  hash,
			IssueTitle: title,
		}

		// 5. add issue-hash of the new issue to span in the current issue source
		for _, span_ := range mapOfWorkloadIdToSpans {
			// take the span id and change the original object
			span, ok := spanMap[typedef.TSpanId(span_.SpanID)]
			if !ok {
				continue
			}
			if span.GroupByTitleSet == nil {
				span.GroupByTitleSet = make(ds.Set[string])
			}
			span.GroupByTitleSet.Add(title)
		}
	}

	// 5. iterate through the issueMap and create a list of issues
	issues := make([]tracePersistenceModel.Issue, 0)
	for _, issue := range issueMap {
		issues = append(issues, issue)
	}

	return issues
}

func getCartesianProductOfSpans(previousResult []map[typedef.TWorkloadId]*stores.SpanFromOTel, multiplier []*stores.SpanFromOTel, workload typedef.TWorkloadId) []map[typedef.TWorkloadId]*stores.SpanFromOTel {

	if len(multiplier) == 0 {
		return previousResult
	}

	if len(previousResult) == 0 {
		previousResult = append(previousResult, make(map[typedef.TWorkloadId]*stores.SpanFromOTel))
	}

	//e.g. [w2:s1,w1:s2] x w3:[s3,s4] = [w2:s1,w1:s2,w3:s3] [w2:s1,w1:s2,w3:s4]
	result := make([]map[typedef.TWorkloadId]*stores.SpanFromOTel, 0)
	for _, s1 := range previousResult {
		for _, s2 := range multiplier {
			newMap := s1
			newMap[workload] = s2
			result = append(result, newMap)
		}
	}
	return result
}

func (worker *QueueWorkerOTel) Consume(delivery rmq.Delivery) {
	var oTelMessage OTELTraceMessage
	if err := json.Unmarshal([]byte(delivery.Payload()), &oTelMessage); err != nil {
		// handle json error
		if err = delivery.Reject(); err != nil {
			// not sure what to do here
		}
		return
	}

	// perform task
	//zkLogger.DebugF(LoggerTagOTel, "got message %v", oTelMessage)

	startTime := time.Now()
	worker.handleMessage(oTelMessage)
	endTime := time.Now()
	zkLogger.InfoF(LoggerTagOTel, "Time taken to to process %s scenario oTel message with trace count: %d  is: %s  ", oTelMessage.Scenario.Title, len(oTelMessage.Traces), endTime.Sub(startTime))
	timeTakenForNTraces := endTime.Sub(startTime).Seconds()
	// Calculate and publish the average time taken per trace
	averageTimePerTrace := timeTakenForNTraces / float64(len(oTelMessage.Traces))
	promMetrics.TimeTakenByOtelWorkerToProcessATrace.WithLabelValues(oTelMessage.Scenario.Title).Observe(averageTimePerTrace)

	if err := delivery.Ack(); err != nil {
		// handle ack error
	}
}
