package scenarioManager

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/adjust/rmq/v5"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	zkRedis "github.com/zerok-ai/zk-utils-go/storage/redis"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
	"log"
	"scenario-manager/config"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"time"
)

const (
	TitleDelimiter = "¦"
)

type TMapOfSpanIdToSpan map[typedef.TSpanId]*tracePersistenceModel.Span

type OTelMessage struct {
	Scenario   model.Scenario     `json:"scenario"`
	Traces     []typedef.TTraceid `json:"traces"`
	ProducerId string             `json:"producer_id"`
}

type OTelMessageConsumer struct {
	oTelStore               *stores.OTelDataHandler
	errorCacheSaveHooks     ErrorCacheSaveHooks[string]
	errorStoreReader        *zkRedis.LocalCacheKVStore[string]
	traceStore              *stores.TraceStore
	tracePersistenceService *tracePersistence.TracePersistenceService
}

func GetOTelMessageConsumer(cfg config.AppConfigs, tps *tracePersistence.TracePersistenceService) *OTelMessageConsumer {

	omc := OTelMessageConsumer{
		oTelStore:               stores.GetOTelStore(cfg.Redis),
		tracePersistenceService: tps,
		traceStore:              stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
	}
	omc.errorCacheSaveHooks = ErrorCacheSaveHooks[string]{oTelMessageConsumer: &omc}
	omc.errorStoreReader = GetLRUCacheStore(cfg.Redis, &omc.errorCacheSaveHooks, ctx)

	return &omc
}

func (consumer *OTelMessageConsumer) handleMessage(oTelMessage OTelMessage) {
	consumer.processTraceIDsAgainstScenarios(&oTelMessage.Scenario, oTelMessage.Traces)
}

// processTraceIDsAgainstScenarios processes all the traceIds against all the scenarios and saves the incidents in the persistence zkRedis
// The traces are processed in the batches of size batchSizeForRawDataCollector
func (consumer *OTelMessageConsumer) processTraceIDsAgainstScenarios(scenario *model.Scenario, traceIds []typedef.TTraceid) {

	// 1. Collect span relation and span data for the traceIDs
	tracesFromOTelStore := consumer.getDataFromOTelStore(traceIds)
	if tracesFromOTelStore == nil || len(tracesFromOTelStore) == 0 {
		return
	}

	// 2. Process each trace against all the scenarioWithTraces
	incidents := consumer.buildIncidentsForPersistence(scenario, tracesFromOTelStore)
	if incidents == nil || len(incidents) == 0 {
		zkLogger.ErrorF(LoggerTag, "no incidents to save")
		return
	}

	// 3. rate limit incidents
	newIncidentList := consumer.rateLimitIncidents(incidents, *scenario)

	if len(newIncidentList) == 0 {
		zkLogger.InfoF(LoggerTag, "rate limited %d incidents. nothing to save", len(incidents))
		return
	}

	// 4. zkRedis the trace data in the persistence zkRedis
	zkLogger.InfoF(LoggerTag, "Sending incidents for persistence, incident count: %d", len(newIncidentList))
	startTime := time.Now()
	saveError := (*consumer.tracePersistenceService).SaveIncidents(newIncidentList)
	if saveError != nil {
		zkLogger.Error(LoggerTag, "Error saving incidents", saveError)
	}
	endTime := time.Now()
	zkLogger.Info(LoggerTag, "Time taken to save zkRedis data in persistent storage ", endTime.Sub(startTime))
}

func (consumer *OTelMessageConsumer) getDataFromOTelStore(traceIds []typedef.TTraceid) map[typedef.TTraceid]*stores.TraceFromOTel {
	tracesFromOTelStore, oTelErrors, err := consumer.oTelStore.GetSpansForTracesFromDB(traceIds)
	if err != nil {
		zkLogger.Error(LoggerTag, "error in getting data from OTel zkRedis", err)
	}

	if oTelErrors != nil && len(oTelErrors) > 0 {
		errorData := make([]tracePersistenceModel.ErrorData, 0)
		for _, errorID := range oTelErrors {
			expStrPtr, isFromCache := consumer.errorStoreReader.Get(errorID)
			if !isFromCache {
				expData := tracePersistenceModel.ErrorData{Id: errorID, Data: *expStrPtr}
				errorData = append(errorData, expData)
			}
		}
		if len(errorData) > 0 {
			(*consumer.tracePersistenceService).SaveErrors(errorData)
		}
	}

	return tracesFromOTelStore
}

func (consumer *OTelMessageConsumer) buildIncidentsForPersistence(scenario *model.Scenario, tracesFromOTel map[typedef.TTraceid]*stores.TraceFromOTel) []tracePersistenceModel.IncidentWithIssues {

	zkLogger.DebugF(LoggerTag, "Building scenario for persistence, trace_count=%d", len(tracesFromOTel))

	// a. iterate through the trace data from OTelDataHandler and build the structure which can be saved in DB
	incidentsWithIssues := make([]tracePersistenceModel.IncidentWithIssues, 0)
	traceTreeForPersistence := make(map[typedef.TTraceid]*TMapOfSpanIdToSpan)
	for traceId, traceFromOTel := range tracesFromOTel {
		spanMapOfPersistentSpans := make(TMapOfSpanIdToSpan)
		for _, spanFromOTel := range traceFromOTel.Spans {
			spanMapOfPersistentSpans[spanFromOTel.SpanID] = spanFromOTel.SpanForPersistence
		}
		traceTreeForPersistence[traceId] = &spanMapOfPersistentSpans

		// evaluate this trace
		incidents := consumer.evaluateIncidents(traceId, scenario, spanMapOfPersistentSpans)
		incidentsWithIssues = append(incidentsWithIssues, incidents)
	}

	return incidentsWithIssues
}

func (consumer *OTelMessageConsumer) evaluateIncidents(traceId typedef.TTraceid, scenario *model.Scenario, spansOfTrace TMapOfSpanIdToSpan) tracePersistenceModel.IncidentWithIssues {

	spans := make([]*tracePersistenceModel.Span, 0)
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
				spans = make([]*tracePersistenceModel.Span, 0)
			}
			workloadIdToSpanArrayMap[typedef.TWorkloadId(workloadId)] = append(spans, span)
		}
	}

	// 3. do a cartesian product of all the elements in workloadIdSet. This gives us all the possible combinations of
	//	workloads-span groups that can be give rise to a unique issues
	issueSource := make([]map[typedef.TWorkloadId]*tracePersistenceModel.Span, 0)
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
			groupByForScenario, ok := span.GroupByMap[tracePersistenceModel.ScenarioId(scenario.Id)]
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

func getCartesianProductOfSpans(previousResult []map[typedef.TWorkloadId]*tracePersistenceModel.Span, multiplier []*tracePersistenceModel.Span, workload typedef.TWorkloadId) []map[typedef.TWorkloadId]*tracePersistenceModel.Span {

	if len(multiplier) == 0 {
		return previousResult
	}

	if len(previousResult) == 0 {
		previousResult = append(previousResult, make(map[typedef.TWorkloadId]*tracePersistenceModel.Span))
	}

	//e.g. [w2:s1,w1:s2] x w3:[s3,s4] = [w2:s1,w1:s2,w3:s3] [w2:s1,w1:s2,w3:s4]
	result := make([]map[typedef.TWorkloadId]*tracePersistenceModel.Span, 0)
	for _, s1 := range previousResult {
		for _, s2 := range multiplier {
			newMap := s1
			newMap[workload] = s2
			result = append(result, newMap)
		}
	}
	return result
}

type ErrorCacheSaveHooks[T any] struct {
	oTelMessageConsumer *OTelMessageConsumer
}

func (errorCacheSaveHooks *ErrorCacheSaveHooks[T]) PreCacheSaveHookAsync(key string, value *T) *zkErrors.ZkError {

	if value != nil {
		strToSave := fmt.Sprintf("%v", *value)
		return (*errorCacheSaveHooks.oTelMessageConsumer.tracePersistenceService).SaveErrors([]tracePersistenceModel.ErrorData{{Id: key, Data: strToSave}})
	}

	return nil
}

func (consumer *OTelMessageConsumer) Consume(delivery rmq.Delivery) {
	var oTelMessage OTelMessage
	if err := json.Unmarshal([]byte(delivery.Payload()), &oTelMessage); err != nil {
		// handle json error
		if err = delivery.Reject(); err != nil {
			// not sure what to do here
		}
		return
	}

	// perform task
	log.Printf("got message %v", oTelMessage)
	consumer.handleMessage(oTelMessage)

	if err := delivery.Ack(); err != nil {
		// handle ack error
	}
}
