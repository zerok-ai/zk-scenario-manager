package filters

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/jmespath/go-jmespath"
	"github.com/pkg/errors"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	store "github.com/zerok-ai/zk-utils-go/storage/redis"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"log"
	"scenario-manager/internal/config"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"time"
)

const (
	ScenarioSetPrefix = "scenario:"
	TitleDelimiter    = "¦"
)

type ScenarioManager struct {
	cfg           config.AppConfigs
	scenarioStore *store.VersionedStore[scenarioGeneratorModel.Scenario]

	traceStore *stores.TraceStore
	oTelStore  *stores.OTelStore

	traceRawDataCollector *vzReader.VzReader

	tracePersistenceService tracePersistence.TracePersistenceService
}

func getNewVZReader(cfg config.AppConfigs) (*vzReader.VzReader, error) {
	reader := vzReader.VzReader{
		CloudAddr:  cfg.ScenarioConfig.VZCloudAddr,
		ClusterId:  cfg.ScenarioConfig.VZClusterId,
		ClusterKey: cfg.ScenarioConfig.VZClusterKey,
	}

	err := reader.Init()
	if err != nil {
		fmt.Printf("Failed to init reader, err: %v\n", err)
		return nil, err
	}

	return &reader, nil
}

func NewScenarioManager(cfg config.AppConfigs, tps tracePersistence.TracePersistenceService) (*ScenarioManager, error) {
	reader, err := getNewVZReader(cfg)
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
		cfg:                     cfg,
	}
	return &fp, nil
}

func (scenarioManager ScenarioManager) Init() ScenarioManager {

	duration := time.Duration(scenarioManager.cfg.ScenarioConfig.ProcessingIntervalInSeconds) * time.Second

	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", duration, scenarioManager.processAllScenarios)
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
	zkLogger.DebugF(LoggerTag, "Number of available scenarios: %d, traceStore: %d", len(scenarios), len(namesOfAllSets))
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting all keys from traceStore ", err)
		return
	}

	// 3. evaluate scenario filters on traceIdSets and attach the full traces against the scenarios
	traceIncidentsMap := make(map[string]tracePersistenceModel.IncidentWithIssues, 0)
	for _, scenario := range scenarios {
		incidentsOfScenario := scenarioManager.processScenario(scenario, namesOfAllSets)
		if incidentsOfScenario == nil {
			continue
		}

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

	if len(incidents) == 0 {
		zkLogger.Debug(LoggerTag, "No incidents to save")
		return
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
	traceIds, err := scenarioManager.evaluateScenario(scenario, namesOfAllSets)
	if err != nil || traceIds == nil || len(traceIds) == 0 {
		return nil
	}

	// b. collect trace and span raw data for the qualified traceIDs
	tracesFromOTelStore, tracesForProtocol, err := scenarioManager.oTelStore.GetSpansForTracesFromDB(traceIds)
	if err != nil || tracesForProtocol == nil {
		zkLogger.Error(LoggerTag, "error processing trace from OTel", err)
		return nil
	}

	// c. get the raw data for traces from vizier
	rawSpans := scenarioManager.getAllRawSpans(tracesForProtocol)

	// d. Feed the span co-relation to raw span data and build the scenario model for storage
	incidents := buildScenarioForPersistence(scenario, tracesFromOTelStore, rawSpans)

	return incidents
}

func (scenarioManager ScenarioManager) evaluateScenario(scenario *scenarioGeneratorModel.Scenario, namesOfAllSets []string) ([]string, error) {
	traceEvaluator := NewTraceEvaluator(scenario, scenarioManager.traceStore, namesOfAllSets, TTLForScenarioSets)
	if traceEvaluator == nil {
		return nil, errors.New("failed to create trace evaluator")
	}
	traceIds, err := traceEvaluator.EvalScenario()
	if err != nil {
		return nil, err
	}
	// delete the sets from traceStore except for the latest one
	traceEvaluator.DeleteOldSets(namesOfAllSets, scenarioManager.cfg.ScenarioConfig.RedisRuleSetCount)
	return traceIds, nil
}

func (scenarioManager ScenarioManager) getAllRawSpans(tracesForProtocol *map[string][]string) []tracePersistenceModel.Span {
	rawSpans := make([]tracePersistenceModel.Span, 0)

	// get the raw data for traces for each protocol
	for protocol, traceArray := range *tracesForProtocol {
		startTime := timeRangeForRawDataQuery
		spans := make([]tracePersistenceModel.Span, 0)
		traceIdCount := len(traceArray)
		for startIndex := 0; startIndex < traceIdCount; {
			endIndex := startIndex + batchSizeForRawDataCollector
			if endIndex > traceIdCount {
				endIndex = traceIdCount
			}
			zkLogger.DebugF(LoggerTag, "calling traceRawDataCollector for %d traces", len(traceArray[startIndex:endIndex]))

			// collect raw data for protocol spans
			if protocol == "http" {
				spans = append(spans, scenarioManager.collectHTTPRawData(traceArray[startIndex:endIndex], startTime)...)
			} else if protocol == "mysql" {
				spans = append(spans, scenarioManager.collectMySQLRawData(traceArray[startIndex:endIndex], startTime)...)
			} else if protocol == "postgresql" {
				spans = append(spans, scenarioManager.collectPostgresRawData(traceArray[startIndex:endIndex], startTime)...)
			}

			startIndex = endIndex
			rawSpans = append(rawSpans, spans...)
		}
	}
	return rawSpans
}

func buildScenarioForPersistence(scenario *scenarioGeneratorModel.Scenario, tracesFromOTel map[string]*stores.TraceFromOTel, spans []tracePersistenceModel.Span) map[string]tracePersistenceModel.IncidentWithIssues {

	zkLogger.DebugF(LoggerTag, "Building scenario for persistence, scenario: %v,  trace_count=%d, span_count=%d", scenario.Id, len(tracesFromOTel), len(spans))

	if len(spans) == 0 {
		return nil
	}

	// process all the spans in httpRawData and build the traceIdToSpansArrayMap
	traceTreeForPersistence := make(map[string]*map[string]*tracePersistenceModel.Span, 0)

	// iterate through the trace data from OTelStore and build the traceIdToSpansMap
	for traceId, traceFromOTel := range tracesFromOTel {
		spanMapOfPersistentSpans := make(map[string]*tracePersistenceModel.Span, 0)
		for _, spanFromOTel := range traceFromOTel.Spans {
			spanForPersistence := &tracePersistenceModel.Span{
				SpanId:       spanFromOTel.SpanID,
				ParentSpanId: spanFromOTel.ParentSpanID,
				Protocol:     spanFromOTel.Protocol,
			}
			spanMapOfPersistentSpans[spanFromOTel.SpanID] = spanForPersistence
		}
		traceTreeForPersistence[traceId] = &spanMapOfPersistentSpans
	}

	// process all the elements of `spans`
	for _, fullSpan := range spans {

		trace, ok := traceTreeForPersistence[fullSpan.TraceId]
		if !ok || trace == nil {
			continue
		}
		spanForPersistence, ok := (*trace)[fullSpan.SpanId]
		if !ok || spanForPersistence == nil {
			continue
		}

		fullSpan.ParentSpanId = spanForPersistence.ParentSpanId
		(*trace)[fullSpan.SpanId] = &fullSpan
	}

	// iterate through the trace data and create IncidentWithIssues for each trace
	incidentsWithIssues := make(map[string]tracePersistenceModel.IncidentWithIssues, 0)
	for traceId, spanMap := range traceTreeForPersistence {
		incidentsWithIssues[traceId] = evaluateIncidents(scenario, traceId, spanMap)
	}

	zkLogger.DebugF(LoggerTag, "Building incidentsWithIssues for persistence, count: %d", len(incidentsWithIssues))
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
			hash := scenario.Id + scenario.Version + TitleDelimiter + getTextFromStructMembers(group.Hash, span)

			// get title
			title := scenario.Title + TitleDelimiter + getTextFromStructMembers(group.Title, span)

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

	// 4. hash the id
	for i, issue := range issues {
		hash := md5.Sum([]byte(scenario.Id + scenario.Version + issue.IssueHash))
		issues[i].IssueHash = hex.EncodeToString(hash[:])
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
				IssueHash:  s1.IssueHash + TitleDelimiter + s2.IssueHash,
				IssueTitle: s1.IssueTitle + TitleDelimiter + s2.IssueTitle,
			}
			result = append(result, issue)
		}
	}
	return result
}

func (scenarioManager ScenarioManager) collectHTTPRawData(traceIds []string, startTime string) []tracePersistenceModel.Span {
	spans := make([]tracePersistenceModel.Span, 0)
	rawData, err := scenarioManager.traceRawDataCollector.GetHTTPRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for http traces ", traceIds, err)
		return spans
	}
	for _, span := range rawData.Results {
		spans = append(spans, transformHTTPSpan(span))
	}
	return spans
}

func (scenarioManager ScenarioManager) collectMySQLRawData(traceIds []string, startTime string) []tracePersistenceModel.Span {
	spans := make([]tracePersistenceModel.Span, 0)
	rawData, err := scenarioManager.traceRawDataCollector.GetMySQLRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for mysql traces ", traceIds, err)
		return spans
	}
	for _, span := range rawData.Results {
		spans = append(spans, transformMySQLSpan(span))
	}
	return spans
}

func (scenarioManager ScenarioManager) collectPostgresRawData(traceIds []string, startTime string) []tracePersistenceModel.Span {
	spans := make([]tracePersistenceModel.Span, 0)
	rawData, err := scenarioManager.traceRawDataCollector.GetPgSQLRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for postgres traces ", traceIds, err)
		return spans
	}
	for _, span := range rawData.Results {
		spans = append(spans, transformPGSpan(span))
	}
	return spans
}
