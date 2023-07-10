package filters

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/jmespath/go-jmespath"
	"github.com/pkg/errors"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"

	"github.com/zerok-ai/zk-utils-go/common"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	store "github.com/zerok-ai/zk-utils-go/storage/redis"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"log"
	"scenario-manager/internal/config"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"strings"
	"time"
)

const (
	ScenarioSetPrefix = "scenario:"
)

type ScenarioManager struct {
	scenarioStore *store.VersionedStore[scenarioGeneratorModel.Scenario]

	traceStore *stores.TraceStore
	oTelStore  *stores.OTelStore

	traceRawDataCollector *vzReader.VzReader

	tracePersistenceService tracePersistence.TracePersistenceService
}

func getNewVZReader() (*vzReader.VzReader, error) {
	reader := vzReader.VzReader{
		CloudAddr:  "px.avinpx07.getanton.com:443",
		ClusterId:  "94711f31-f693-46be-91c3-832c0f64b12f",
		ClusterKey: "px-api-ce1bbae5-49c7-4d81-99e2-0d11865bb5df",
	}

	err := reader.Init()
	if err != nil {
		fmt.Printf("Failed to init reader, err: %v\n", err)
		return nil, err
	}

	return &reader, nil
}

func NewScenarioManager(cfg config.AppConfigs, tps tracePersistence.TracePersistenceService) (*ScenarioManager, error) {
	reader, err := getNewVZReader()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get new VZ reader")
	}

	vs, err := store.GetVersionedStore[scenarioGeneratorModel.Scenario](cfg.Redis, "scenarios", ScenarioRefreshInterval)
	if err != nil {
		return nil, err
	}

	fp := ScenarioManager{
		scenarioStore:           vs,
		traceStore:              stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
		oTelStore:               stores.GetOTelStore(cfg.Redis),
		traceRawDataCollector:   reader,
		tracePersistenceService: tps,
	}
	return &fp, nil
}

func (scenarioManager ScenarioManager) Init() ScenarioManager {
	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", FilterProcessingTickInterval, scenarioManager.processAllScenarios)
	tickerTask.Start()
	return scenarioManager
}

func (scenarioManager ScenarioManager) Close() {

	scenarioManager.scenarioStore.Close()
	scenarioManager.traceStore.Close()
	scenarioManager.oTelStore.Close()

	scenarioManager.traceRawDataCollector.Close()
	err := scenarioManager.tracePersistenceService.Close()
	if err != nil {
		zkLogger.Error(LoggerTag, "Error closing tracePersistenceService")
		return
	}

}

func (scenarioManager ScenarioManager) processAllScenarios() {

	// 1. get all scenarios
	scenarios := scenarioManager.scenarioStore.GetAllValues()

	// 2. get all traceId sets from traceStore
	namesOfAllSets, err := scenarioManager.traceStore.GetAllKeys()
	zkLogger.DebugF(LoggerTag, "Number of available scenarios: %d", len(scenarios))
	zkLogger.DebugF(LoggerTag, "Number of keys in traceStore: %d", len(namesOfAllSets))
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting all keys from traceStore ", err)
		return
	}

	// 3. evaluate scenario filters on traceIdSets and attach the full traces against the scenarios
	traceIncidentsMap := make(map[string]tracePersistenceModel.IncidentWithIssues, 0)
	for _, scenario := range scenarios {
		incidentsOfScenario := scenarioManager.processScenario(scenario, namesOfAllSets)

		for traceId, incident := range incidentsOfScenario {
			oldIncidence, ok := traceIncidentsMap[traceId]
			if ok {
				// merge oldIncidence with incident
				incident.IssueGroupList = append(incident.IssueGroupList, oldIncidence.IssueGroupList...)
			}
			traceIncidentsMap[traceId] = incident
		}
	}
	// get the array of incidents from the map
	incidents := make([]tracePersistenceModel.IncidentWithIssues, 0)
	for _, incident := range traceIncidentsMap {
		incidents = append(incidents, incident)
	}

	// 4. store the scenario in the persistence store along with its traces
	saveError := scenarioManager.tracePersistenceService.SaveIncidents(incidents)
	if saveError != nil {
		zkLogger.Error(LoggerTag, "Error saving scenario", saveError)
	}
}

func (scenarioManager ScenarioManager) processScenario(scenario *scenarioGeneratorModel.Scenario, namesOfAllSets []string) map[string]tracePersistenceModel.IncidentWithIssues {

	if scenario == nil {
		log.Println("Found nil scenario")
		return nil
	}

	// a. evaluate the scenario and get the qualified traceIDs
	traceEvaluator := NewTraceEvaluator(scenario, scenarioManager.traceStore, namesOfAllSets, TTLForScenarioSets)
	if traceEvaluator == nil {
		return nil
	}
	traceIds, err := traceEvaluator.EvalScenario(ScenarioSetPrefix)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error evaluating scenario", err)
		return nil
	}

	// b. collect trace and span raw data for the qualified traceIDs
	rawSpans := scenarioManager.collectFullSpanDataForTraces(traceIds)
	if rawSpans == nil || len(*rawSpans) == 0 {
		zkLogger.Debug(LoggerTag, "no spans found")
		return nil
	}

	// c. get span co-relation data from the OTel store for the qualified for the spans for which we have raw data
	traceIdsOfRawSpans := make([]string, 0)
	for _, value := range *rawSpans {
		traceId := value.TraceId
		traceIdsOfRawSpans = append(traceIdsOfRawSpans, traceId)
	}
	traceFromOTelStore, err := scenarioManager.oTelStore.GetSpansForTracesFromDB(traceIdsOfRawSpans)
	if err != nil {
		zkLogger.Error(LoggerTag, "error processing trace from OTel", err)
		return nil
	}

	// d. Feed the span co-relation to raw span data and build the scenario model for storage
	incidents := buildScenarioForPersistence(scenario, traceFromOTelStore, rawSpans)

	return incidents
}

func buildScenarioForPersistence(scenario *scenarioGeneratorModel.Scenario, tracesFromOTel map[string]*stores.TraceFromOTel, httpRawData *[]models.HttpRawDataModel) map[string]tracePersistenceModel.IncidentWithIssues {

	zkLogger.DebugF(LoggerTag, "Building scenario for persistence, scenario: %v for %d number of traces", scenario.Id, len(tracesFromOTel))

	// process all the spans in httpRawData and build the traceIdToSpansArrayMap
	traceTreeForPersistence := make(map[string]*map[string]*tracePersistenceModel.Span, 0)

	// iterate through the trace data from OTelStore and build the traceIdToSpansMap
	for traceId, traceFromOTel := range tracesFromOTel {
		spanMapOfPersistentSpans := make(map[string]*tracePersistenceModel.Span, 0)
		for _, spanFromOTel := range traceFromOTel.Spans {
			spanForPersistence := &tracePersistenceModel.Span{
				SpanId:       spanFromOTel.SpanID,
				ParentSpanId: spanFromOTel.ParentSpanID,
				Protocol:     "unknown",
			}
			spanMapOfPersistentSpans[spanFromOTel.SpanID] = spanForPersistence
		}
		traceTreeForPersistence[traceId] = &spanMapOfPersistentSpans
	}

	// process all the spans in httpRawData and set data in traceIdToSpansMap
	for _, fullSpan := range *httpRawData {

		trace, ok := traceTreeForPersistence[fullSpan.TraceId]
		if !ok || trace == nil {
			continue
		}
		spanForPersistence, ok := (*trace)[fullSpan.SpanId]
		if !ok || spanForPersistence == nil {
			continue
		}

		(*trace)[fullSpan.SpanId] = populateHttpSpan(fullSpan, *spanForPersistence, "http")
	}

	traceIdToSpansArrayMap := make(map[string][]tracePersistenceModel.Span, 0)

	zkLogger.DebugF(LoggerTag, "1. Building scenario for persistence, traceIdToSpansArrayMap count: %d", len(traceIdToSpansArrayMap))

	// iterate through the trace data and create IncidentWithIssues for each trace
	incidentsWithIssues := make(map[string]tracePersistenceModel.IncidentWithIssues, 0)
	for traceId, spanMap := range traceTreeForPersistence {
		incidentsWithIssues[traceId] = evaluateIncidents(scenario, traceId, spanMap)
	}
	return incidentsWithIssues
}

func evaluateIncidents(scenario *scenarioGeneratorModel.Scenario, traceId string, spanMap *map[string]*tracePersistenceModel.Span) tracePersistenceModel.IncidentWithIssues {

	spans := make([]tracePersistenceModel.Span, 0)
	for _, span := range *spanMap {
		spans = append(spans, *span)
	}

	issueGroupList := make([]tracePersistenceModel.IssueGroup, 0)
	issueGroupList = append(issueGroupList, tracePersistenceModel.IssueGroup{
		ScenarioId:      scenario.Id,
		ScenarioVersion: scenario.Version,
		Issues:          getListOfIssues(scenario, spanMap),
	})

	return tracePersistenceModel.IncidentWithIssues{
		IssueGroupList: issueGroupList,
		Incident: tracePersistenceModel.Incident{
			TraceId:                traceId,
			Spans:                  spans,
			IncidentCollectionTime: time.Now(),
		},
	}
}

func getListOfIssues(scenario *scenarioGeneratorModel.Scenario, spanMap *map[string]*tracePersistenceModel.Span) []tracePersistenceModel.Issue {

	// 1. create a set of workspaceIds vs spans
	workloadIdToSpansMap := make(map[string][]tracePersistenceModel.Span, 0)
	for _, span := range *spanMap {
		workloadIdList := span.WorkloadIdList
		for _, workloadId := range workloadIdList {
			spans, ok := workloadIdToSpansMap[workloadId]
			if !ok {
				spans = make([]tracePersistenceModel.Span, 0)
			}
			workloadIdToSpansMap[workloadId] = append(spans, *span)
		}
	}

	// 2. iterate through the `GroupBy` construct in scenario and evaluate each `groupBy` clause
	issues := make([]tracePersistenceModel.Issue, 0)
	for _, group := range scenario.GroupBy {

		// 2.1 get the list of spans for each workloadId
		spans, ok := workloadIdToSpansMap[group.WorkloadId]
		if !ok {
			continue
		}

		// 2.2 create title and hash by iterating through the spans
		issuesForGroup := make([]tracePersistenceModel.Issue, 0)
		for _, span := range spans {

			// get hash
			hashBytes := md5.Sum([]byte(scenario.Id + scenario.Version + getTextFromStructMembers(group.Hash, span)))
			hash := hex.EncodeToString(hashBytes[:])

			// get title
			title := getTextFromStructMembers(group.Title, span)
			
			issuesForGroup = append(issuesForGroup, tracePersistenceModel.Issue{
				IssueHash:  hash,
				IssueTitle: title,
			})

			if span.IssueHashList == nil {
				span.IssueHashList = make([]string, 0)
			}
			span.IssueHashList = append(span.IssueHashList, hash)
		}

		// 3. do a cartesian product of all the groups
		issues = getCartesianProductOfIssues(issues, issuesForGroup)
	}

	return issues
}

func getTextFromStructMembers(path string, span interface{}) string {
	result, err := jmespath.Search(path, span)
	if err == nil {
		str, ok := result.(string)
		if ok {
			return str
		}
	} else {
		zkLogger.Error(LoggerTag, "Error evaluating jmespath for span ", span)
	}
	return ""
}

// function to get cartesian product of two string slices
func getCartesianProductOfIssues(slice1 []tracePersistenceModel.Issue, slice2 []tracePersistenceModel.Issue) []tracePersistenceModel.Issue {

	if len(slice1) == 0 && len(slice2) == 0 {
		return slice1
	}

	if len(slice1) != 0 && len(slice2) == 0 {
		return slice1
	}

	if len(slice1) == 0 && len(slice2) != 0 {
		return slice2
	}

	result := make([]tracePersistenceModel.Issue, 0)
	for _, s1 := range slice1 {
		for _, s2 := range slice2 {
			issue := tracePersistenceModel.Issue{
				IssueHash:  s1.IssueHash + "¦" + s2.IssueHash,
				IssueTitle: s1.IssueTitle + "¦" + s2.IssueTitle,
			}
			result = append(result, issue)
		}
	}
	return result
}

func (scenarioManager ScenarioManager) collectFullSpanDataForTraces(traceIds []string) *[]models.HttpRawDataModel {

	// get the raw data for traces from vizier
	startTime := timeRangeForRawDataQuery

	results := make([]models.HttpRawDataModel, 0)
	traceIdCount := len(traceIds)

	for startIndex := 0; startIndex < traceIdCount; {
		endIndex := startIndex + batchSizeForRawDataCollector
		if endIndex > traceIdCount {
			endIndex = traceIdCount
		}
		zkLogger.DebugF(LoggerTag, "calling traceRawDataCollector for %d traces", len(traceIds[startIndex:endIndex]))
		rawData, err := scenarioManager.traceRawDataCollector.GetHTTPRawData(traceIds[startIndex:endIndex], startTime)
		if err != nil {
			zkLogger.Error(LoggerTag, "Error getting raw data for traces ", traceIds, err)
			return nil
		}
		results = append(results, rawData.Results...)
		startIndex = endIndex
	}

	zkLogger.Debug(LoggerTag, "Number of raw values from traceRawDataCollector = ", len(results))
	return &results
}

func getHttpRequestData(value models.HttpRawDataModel) tracePersistenceModel.HTTPRequestPayload {
	req := tracePersistenceModel.HTTPRequestPayload{
		ReqPath:    value.ReqPath,
		ReqMethod:  value.ReqMethod,
		ReqHeaders: value.ReqHeaders,
		ReqBody:    value.ReqBody,
	}
	return req
}

func getHttpResponseData(value models.HttpRawDataModel) tracePersistenceModel.HTTPResponsePayload {
	res := tracePersistenceModel.HTTPResponsePayload{
		RespStatus:  value.RespStatus,
		RespMessage: value.RespMessage,
		RespHeaders: value.RespHeaders,
		RespBody:    value.RespBody,
	}
	return res
}

func populateHttpSpan(fullSpan models.HttpRawDataModel, spanForStorage tracePersistenceModel.Span, protocol string) *tracePersistenceModel.Span {
	spanForStorage.Source = fullSpan.Source
	spanForStorage.Destination = fullSpan.Destination
	spanForStorage.WorkloadIdList = strings.Split(fullSpan.WorkloadIds, ",")
	spanForStorage.Metadata = map[string]interface{}{}
	spanForStorage.LatencyMs = getLatencyPtr(fullSpan.Latency)
	spanForStorage.Protocol = protocol
	spanForStorage.RequestPayload = getHttpRequestData(fullSpan)
	spanForStorage.ResponsePayload = getHttpResponseData(fullSpan)

	return &spanForStorage
}

func getLatencyPtr(latencyStr string) *float32 {
	latency32, err := common.ToFloat32(latencyStr)
	if err == nil {
		return &latency32
	}
	return nil
}
