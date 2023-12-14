package scenarioManager

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/google/uuid"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	pb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	otlpCommonV1 "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/resource/v1"
	otlpTraceV1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"log"
	"scenario-manager/config"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
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
}

func GetQueueWorkerOTel(cfg config.AppConfigs) *QueueWorkerOTel {

	// initialize worker
	worker := QueueWorkerOTel{
		id:             "O" + uuid.New().String(),
		oTelStore:      stores.GetOTelStore(cfg.Redis),
		traceStore:     stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
		attributeStore: stores.GetAttributesStore(cfg.Redis),
		errorStore:     stores.GetErrorStore(cfg.Redis),
	}

	// oTel consumer and error store
	var err error
	worker.oTelConsumer, err = stores.GetTraceConsumer(cfg.Redis, []rmq.Consumer{&worker}, OTelQueue)
	if err != nil {
		return nil
	}

	return &worker
}

func (worker *QueueWorkerOTel) Close() {
	worker.oTelStore.Close()
	worker.attributeStore.Close()
	worker.errorStore.Close()
	worker.traceStore.Close()
	worker.oTelConsumer.Close()
}

func (worker *QueueWorkerOTel) handleMessage(oTelMessage OTELTraceMessage) {

	zkLogger.DebugF(LoggerTagOTel, "oTelWorker %v got a message", worker.id)

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

	// 3. rate limit incidents
	newIncidentList := worker.rateLimitIncidents(incidents, oTelMessage.Scenario)
	//newIncidentList := incidents

	if len(newIncidentList) == 0 {
		zkLogger.InfoF(LoggerTagOTel, "rate limited %d incidents. nothing to save", len(incidents))
		return
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
	for _, incident := range newIncidentList {
		for _, span := range incident.Incident.Spans {
			if span.WorkloadIDList != nil && len(span.WorkloadIDList) != 0 {
				var workloadIdList []*otlpCommonV1.AnyValue
				for _, workloadId := range span.WorkloadIDList {
					workloadIdList = append(workloadIdList, &otlpCommonV1.AnyValue{Value: &otlpCommonV1.AnyValue_StringValue{StringValue: workloadId}})
				}

				span.Span.Attributes = append(span.Span.Attributes, &otlpCommonV1.KeyValue{
					Key:   "workload_id_list",
					Value: &otlpCommonV1.AnyValue{Value: &otlpCommonV1.AnyValue_ArrayValue{ArrayValue: &otlpCommonV1.ArrayValue{Values: workloadIdList}}},
				})
			}

			if span.IsRoot {
				var scenarioIdList []*otlpCommonV1.AnyValue
				for _, issueGroup := range incident.IssueGroupList {
					scenarioIdList = append(scenarioIdList, &otlpCommonV1.AnyValue{Value: &otlpCommonV1.AnyValue_StringValue{StringValue: issueGroup.ScenarioId}})
				}

				span.Span.Attributes = append(span.Span.Attributes, &otlpCommonV1.KeyValue{
					Key:   "scenario_id",
					Value: &otlpCommonV1.AnyValue{Value: &otlpCommonV1.AnyValue_ArrayValue{ArrayValue: &otlpCommonV1.ArrayValue{Values: scenarioIdList}}},
				})
				break
			}
		}
	}

	// 4. save attributes details
	spanBuffer := make([]*stores.SpanFromOTel, 0)
	for _, incident := range newIncidentList {
		for _, span := range incident.Incident.Spans {
			span.Span.Attributes = typedef.ConvertMapToKVList(span.SpanAttributes)
			spanBuffer = append(spanBuffer, span)
		}
	}
	var tracesData pb.ExportTraceServiceRequest
	resourceBuffer := ConvertOtelSpanToResourceSpan(spanBuffer, resourceHashToInfoMap, scopeHashToInfoMap)
	tracesData.ResourceSpans = resourceBuffer

	//resourceBufferByteArr, _ := json.Marshal(resourceBuffer)
	//fmt.Printf("resourceBufferByteArr: %v", resourceBufferByteArr)

	fmt.Println("Testing")
	// Set up a connection to the server
	conn, err := grpc.Dial("opentelemetry-collector.monitoring.svc.cluster.local:4319", grpc.WithInsecure())
	//conn, err := grpc.Dial("localhost:4319", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewTraceServiceClient(conn)

	// Contact the server and print out its response.
	response, err := c.Export(context.Background(), &tracesData)
	if err != nil {
		log.Fatalf("could not send trace data: %v", err)
	}
	log.Printf("Response: %v", response)
}

func ConvertOtelSpanToResourceSpan(spans []*stores.SpanFromOTel, resourceHashToInfoMap, scopeHashToInfoMap map[string]map[string]interface{}) []*otlpTraceV1.ResourceSpans {

	resourceMap := make(map[string]map[string][]stores.SpanFromOTel)

	for _, span := range spans {
		if scopeMap, ok := resourceMap[span.ResourceAttributesHash]; ok {
			if spans, ok2 := scopeMap[span.ScopeAttributesHash]; ok2 {
				spans = append(spans, *span)
			} else {
				spanList := make([]stores.SpanFromOTel, 0)
				scopeMap[span.ScopeAttributesHash] = append(spanList, *span)
			}
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
		resourceHashToAttr[key] = typedef.ConvertMapToKVList(value["attributes_map"].(map[string]interface{}))
	}

	for key, value := range scopeHashToInfoMap {
		if value["attributes_map"] == nil {
			continue
		}
		scopeHashToAttr[key] = typedef.ConvertMapToKVList(value["attributes_map"].(map[string]interface{}))
	}

	for resourceHash, scopeMap := range resourceMap {
		scopeSpansList := make([]*otlpTraceV1.ScopeSpans, 0)
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
					Name:       scopeHashToInfoMap[scopeHash]["name"].(string),
					Version:    scopeHashToInfoMap[scopeHash]["version"].(string),
				},
				SchemaUrl: scopeHashToInfoMap[scopeHash]["schema_url"].(string),
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

	zkLogger.DebugF(LoggerTagOTel, "Building scenario for persistence, trace_count=%d", len(tracesFromOTel))

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
			groupByForScenario, ok := span.GroupByMap[stores.ScenarioId(scenario.Id)]
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
			if span.IssueHashList == nil {
				span.IssueHashList = make([]string, 0)
			}

			// iterate over span.IssueHashList and check for the existence of duplicate issueHash. If not present, add.
			isIssueHashPresent := false
			for _, issueHash := range span.IssueHashList {
				if issueHash == hash {
					isIssueHashPresent = true
					break
				}
			}
			if !isIssueHashPresent {
				span.IssueHashList = append(span.IssueHashList, hash)
			}
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
	zkLogger.DebugF(LoggerTagOTel, "got message %v", oTelMessage)

	startTime := time.Now()
	worker.handleMessage(oTelMessage)
	endTime := time.Now()
	zkLogger.Info(LoggerTagOTel, "Time taken to to process oTel message ", endTime.Sub(startTime))

	if err := delivery.Ack(); err != nil {
		// handle ack error
	}
}
