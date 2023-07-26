package filters

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/jmespath/go-jmespath"
	"github.com/pkg/errors"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	store "github.com/zerok-ai/zk-utils-go/storage/redis"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/config"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ScenarioSetPrefix = "scenario:"
	TitleDelimiter    = "¦"
)

var issueRateMap typedef.IssueRateMap

type ScenarioManager struct {
	cfg           config.AppConfigs
	scenarioStore *store.VersionedStore[scenarioGeneratorModel.Scenario]

	traceStore *stores.TraceStore
	oTelStore  *stores.OTelStore

	traceRawDataCollector *vzReader.VzReader

	tracePersistenceService tracePersistence.TracePersistenceService
	mutex                   sync.Mutex
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

func (scenarioManager ScenarioManager) Init() ScenarioManager {

	rateLimitTickerDuration := time.Duration(60) * time.Second
	rateLimitTickerTask := ticker.GetNewTickerTask("rate-limit-processor", rateLimitTickerDuration, scenarioManager.processRateLimit)
	rateLimitTickerTask.Start()

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

func (scenarioManager ScenarioManager) processRateLimit() {
	currTime := time.Now()
	if issueRateMap == nil {
		issueRateMap = make(typedef.IssueRateMap, 0)
	}
	scenarioManager.mutex.Lock()
	defer scenarioManager.mutex.Unlock()
	for k, v := range issueRateMap {
		for i, issueRate := range v {
			if currTime.After(issueRate.ExpiryTime) {
				issueRateMap[k][i] = returnNewRateLimitObject(currTime, issueRate)
			}
		}
	}
}

func returnNewRateLimitObject(currTime time.Time, issueRate typedef.IssuesCounter) typedef.IssuesCounter {
	issueRate.IssueCountMap = make(map[typedef.TIssueHash]int, 0)
	issueRate.ExpiryTime = currTime.Add(issueRate.TickDuration)
	return issueRate
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

	// 3. get all the traceIds from the traceStore for all the scenarios
	allTraceIds, scenarioWithTraces := scenarioManager.getAllTraceIDs(scenarios, namesOfAllSets)

	// 4. process all traces against all scenarios
	scenarioManager.processTraceIDsAgainstScenarios(allTraceIds, scenarioWithTraces)
}

// processTraceIDsAgainstScenarios processes all the traceIds against all the scenarios and saves the incidents in the persistence store
// The traces are processed in the batches of size batchSizeForRawDataCollector
func (scenarioManager ScenarioManager) processTraceIDsAgainstScenarios(traceIds []typedef.TTraceid, scenarioWithTraces typedef.ScenarioToScenarioTracesMap) {
	batch := 0
	traceIdCount := len(traceIds)
	for startIndex := 0; startIndex < traceIdCount; {

		// a. create the batch
		endIndex := startIndex + batchSizeForRawDataCollector
		if endIndex > traceIdCount {
			endIndex = traceIdCount
		}
		traceIdSubSet := traceIds[startIndex:endIndex]

		// b. collect span relation and span raw data for the traceIDs
		tracesFromOTelStore, rawSpans, err1 := scenarioManager.getDataForTraces(traceIdSubSet)
		if err1 != nil {
			zkLogger.ErrorF(LoggerTag, "Error processing batch %d of trace ids", batch)
		}

		// c. Process each trace against all the scenarioWithTraces
		incidents := buildIncidentsForPersistence(scenarioWithTraces, tracesFromOTelStore, rawSpans)
		if len(incidents) == 0 {
			zkLogger.ErrorF(LoggerTag, "no incidents to save")
			continue
		}

		// d. rate limit issues
		newIncidentList := make([]tracePersistenceModel.IncidentWithIssues, 0)
		for _, incident := range incidents {
			i := rateLimitIssue(scenarioManager, incident, scenarioWithTraces)
			if len(i.IssueGroupList) == 0 {
				continue
			}
			newIncidentList = append(newIncidentList, i)
		}

		// e. store the trace data in the persistence store
		saveError := scenarioManager.tracePersistenceService.SaveIncidents(newIncidentList)
		if saveError != nil {
			zkLogger.Error(LoggerTag, "Error saving scenario", saveError)
		}

		startIndex = endIndex
		batch += 1
	}
}

func parseTimeString(input string) (time.Duration, error) {
	var duration time.Duration
	var multiplier time.Duration

	switch {
	case strings.HasSuffix(input, "m"):
		multiplier = time.Minute
	case strings.HasSuffix(input, "h"):
		multiplier = time.Hour
	case strings.HasSuffix(input, "d"):
		multiplier = 24 * time.Hour
	default:
		return 0, fmt.Errorf("unsupported input format")
	}

	numericPart := strings.TrimSuffix(input, string(input[len(input)-1]))
	val, err := strconv.Atoi(numericPart)
	if err != nil {
		return 0, err
	}

	duration = time.Duration(val) * multiplier
	return duration, nil
}

func rateLimitIssue(scenarioManager ScenarioManager, incident tracePersistenceModel.IncidentWithIssues, scenarioWithTraces typedef.ScenarioToScenarioTracesMap) tracePersistenceModel.IncidentWithIssues {
	// create a list to store the issues to be removed
	issuesToRemove := make(map[typedef.TIssueHash]bool, 0)

	// acquire the lock
	scenarioManager.mutex.Lock()
	defer scenarioManager.mutex.Unlock()

	// for each issueGroup in the incident, there is a list of issues
	for _, issue := range incident.IssueGroupList {
		tScenarioId := typedef.TScenarioID(issue.ScenarioId)
		// if the scenario is not present in the issueRateMap or if the scenario is updated and a new rate limit is added, then len of previous rate limit and current rate limit will be different
		if v, ok := issueRateMap[tScenarioId]; !ok || len(v) != len(scenarioWithTraces[tScenarioId].Scenario.RateLimit) {
			// for every rateLimit object in scenario, create a new entry in the issueRateMap
			rateLimiters := make([]typedef.IssuesCounter, 0)
			for _, rateLimit := range scenarioWithTraces[tScenarioId].Scenario.RateLimit {
				m := make(typedef.IssueBucket, 0)
				duration, err := parseTimeString(rateLimit.TickDuration)
				if err != nil {
					zkLogger.ErrorF(LoggerTag, "Error parsing time string %s", rateLimit.TickDuration)
					continue
				}
				t := typedef.IssuesCounter{
					IssueCountMap:    m,
					ExpiryTime:       time.Now().Add(duration),
					BucketMaxSize:    rateLimit.BucketMaxSize,
					BucketRefillSize: rateLimit.BucketRefillSize,
					TickDuration:     duration,
				}
				rateLimiters = append(rateLimiters, t)
			}
			issueRateMap[tScenarioId] = rateLimiters
		}

		// update the count of issues in the rateLimitMap only if the value is less than the bucketMaxSize, else add the issue to the issuesToRemove list
		for _, rateLimiter := range issueRateMap[tScenarioId] {
			for _, i := range issue.Issues {
				if _, ok := rateLimiter.IssueCountMap[typedef.TIssueHash(i.IssueHash)]; !ok {
					rateLimiter.IssueCountMap[typedef.TIssueHash(i.IssueHash)] = 1
				} else if rateLimiter.IssueCountMap[typedef.TIssueHash(i.IssueHash)] < rateLimiter.BucketMaxSize {
					rateLimiter.IssueCountMap[typedef.TIssueHash(i.IssueHash)] += 1
				} else {
					issuesToRemove[typedef.TIssueHash(i.IssueHash)] = true
				}
			}
		}
	}

	// if there are issues to be removed, then remove them from the incident
	if len(issuesToRemove) != 0 {
		newIssueGroupList := make([]tracePersistenceModel.IssueGroup, 0)
		for _, issueGroup := range incident.IssueGroupList {
			newIssuesList := make([]tracePersistenceModel.Issue, 0)
			for _, issue := range issueGroup.Issues {
				if _, ok := issuesToRemove[typedef.TIssueHash(issue.IssueHash)]; ok {
					continue
				}
				newIssuesList = append(newIssuesList, issue)
			}
			issueGroup.Issues = newIssuesList
			newIssueGroupList = append(newIssueGroupList, issueGroup)
		}
		incident.IssueGroupList = sanitizeIssueGroupList(newIssueGroupList)
		return incident
	}
	return incident
}

// sanitizeIssueGroupList removes the issueGroup from the list if the issueGroup has no issues
func sanitizeIssueGroupList(issueGroupList []tracePersistenceModel.IssueGroup) []tracePersistenceModel.IssueGroup {
	l := make([]tracePersistenceModel.IssueGroup, 0)
	for _, v := range issueGroupList {
		if len(v.Issues) == 0 {
			continue
		}
		l = append(l, v)
	}
	return l
}

// getAllTraceIDs gets all the traceIds from the traceStore for all the scenarios
func (scenarioManager ScenarioManager) getAllTraceIDs(scenarios map[string]*scenarioGeneratorModel.Scenario, namesOfAllSets []string) ([]typedef.TTraceid, typedef.ScenarioToScenarioTracesMap) {
	allTraceIds := make(ds.Set[typedef.TTraceid], 0)
	scenarioWithTraces := make(typedef.ScenarioToScenarioTracesMap, 0)
	for _, scenario := range scenarios {
		if scenario == nil {
			zkLogger.Debug(LoggerTag, "Found nil scenario")
			continue
		}

		// a. evaluate the scenario and get the qualified traceIDs
		traceEvaluator := NewTraceEvaluator(scenarioManager.cfg, scenario, scenarioManager.traceStore, namesOfAllSets, TTLForScenarioSets)
		if traceEvaluator == nil {
			zkLogger.Error(LoggerTag, "failed to create trace evaluator")
		}
		tIds, err := traceEvaluator.EvalScenario()
		if err != nil || tIds == nil || len(tIds) == 0 {
			continue
		}
		allTraceIds.AddBulk(tIds)
		scenarioWithTraces[typedef.TScenarioID(scenario.Id)] = typedef.ScenarioTraces{Scenario: scenario, Traces: ds.Set[typedef.TTraceid]{}.AddBulk(tIds)}
	}
	return allTraceIds.GetAll(), scenarioWithTraces
}

// getDataForTraces collects span relation and span raw data for the traceIDs. The relation is collected from OTel store and raw data is collected from raw data store
func (scenarioManager ScenarioManager) getDataForTraces(traceIds []typedef.TTraceid) (map[typedef.TTraceid]*stores.TraceFromOTel, []*tracePersistenceModel.Span, error) {

	// a. collect trace and span raw data for the traceIDs
	tracesFromOTelStore, err := scenarioManager.oTelStore.GetSpansForTracesFromDB(traceIds)
	if err != nil {
		return nil, nil, err
	}

	// b. create a map of protocol to set of traceIds
	tracesPerProtocol := make(map[typedef.TProtocol]ds.Set[typedef.TTraceid], 0)
	for traceId, trace := range tracesFromOTelStore {

		for _, span := range trace.Spans {
			protocol := typedef.TProtocol(span.Protocol)
			traceSet, ok := tracesPerProtocol[protocol]
			if !ok {
				traceSet = ds.Set[typedef.TTraceid]{}
			}
			tracesPerProtocol[protocol] = traceSet.Add(traceId)
		}
	}

	// c. get the raw data for traces from vizier
	rawSpans := scenarioManager.getAllRawSpans(tracesPerProtocol)
	return tracesFromOTelStore, rawSpans, nil
}

func (scenarioManager ScenarioManager) getAllRawSpans(tracesForProtocol map[typedef.TProtocol]ds.Set[typedef.TTraceid]) []*tracePersistenceModel.Span {
	rawSpans := make([]*tracePersistenceModel.Span, 0)

	startTime := timeRangeForRawDataQuery

	// get the raw data for traces for each protocol
	for protocol, traceSet := range tracesForProtocol {

		traceIds := traceSet.GetAll()
		traceArray := make([]string, 0)
		for _, traceId := range traceIds {
			traceArray = append(traceArray, string(traceId))
		}

		zkLogger.DebugF(LoggerTag, "calling traceRawDataCollector for %d traces", len(traceArray))

		// collect raw data for protocol spans
		spanForBatch := make([]tracePersistenceModel.Span, 0)
		if protocol == PHTTP || protocol == PException {
			spanForBatch = scenarioManager.collectHTTPRawData(traceArray, startTime)
		} else if protocol == PMySQL {
			spanForBatch = scenarioManager.collectMySQLRawData(traceArray, startTime)
		} else if protocol == PPostgresql {
			spanForBatch = scenarioManager.collectPostgresRawData(traceArray, startTime)
		}

		for index := range spanForBatch {
			span := &spanForBatch[index]
			span.Protocol = string(protocol)
			rawSpans = append(rawSpans, span)
		}
	}
	return rawSpans
}

func buildIncidentsForPersistence(scenariosWithTraces typedef.ScenarioToScenarioTracesMap, tracesFromOTel map[typedef.TTraceid]*stores.TraceFromOTel, spans []*tracePersistenceModel.Span) []tracePersistenceModel.IncidentWithIssues {

	zkLogger.DebugF(LoggerTag, "Building scenario for persistence, trace_count=%d, span_count=%d", len(tracesFromOTel), len(spans))

	if len(spans) == 0 {
		return nil
	}

	// a. Add raw spans to `tracesFromOTel`
	for _, fullSpan := range spans {

		trace, ok := tracesFromOTel[typedef.TTraceid(fullSpan.TraceId)]
		if !ok || trace == nil {
			continue
		}

		spanFromOTelTree, ok := trace.Spans[typedef.TSpanId(fullSpan.SpanId)]
		if ok && spanFromOTelTree != nil {
			spanFromOTelTree.RawSpan = fullSpan
		}
	}

	// b. iterate through the trace data from OTelStore and build the structure which can be saved in DB
	traceTreeForPersistence := make(map[typedef.TTraceid]*typedef.TMapOfSpanIdToSpan, 0)
	for traceId, traceFromOTel := range tracesFromOTel {
		traceTreeForPersistence[traceId] = buildTraceForStorage(traceFromOTel)
	}

	// c. iterate through the trace data and create IncidentWithIssues for each trace
	incidentsWithIssues := make([]tracePersistenceModel.IncidentWithIssues, 0)
	for traceId, spanMapForTrace := range traceTreeForPersistence {

		// find all the scenarios this trace belongs to
		scenarioMap := make(map[typedef.TScenarioID]*scenarioGeneratorModel.Scenario, 0)
		for scenarioId, scenarioWithTraces := range scenariosWithTraces {
			if scenarioWithTraces.Traces.Contains(traceId) {
				scenarioMap[scenarioId] = scenarioWithTraces.Scenario
			}
		}

		// evaluate this trace
		incidents := evaluateIncidents(traceId, scenarioMap, *spanMapForTrace)
		incidentsWithIssues = append(incidentsWithIssues, incidents)
	}

	zkLogger.DebugF(LoggerTag, "Building incidentsWithIssues for persistence, count: %d", len(incidentsWithIssues))
	return incidentsWithIssues
}

func buildTraceForStorage(traceFromOTel *stores.TraceFromOTel) *typedef.TMapOfSpanIdToSpan {

	// set the destination doesn't exist for the root, add it
	setDestinationForRoot(traceFromOTel.Spans[traceFromOTel.RootSpanID])

	// remove spans which are not needed
	prune(traceFromOTel.Spans, traceFromOTel.RootSpanID, true)

	spanMapOfPersistentSpans := make(typedef.TMapOfSpanIdToSpan, 0)
	for _, spanFromOTel := range traceFromOTel.Spans {

		spanForPersistence := spanFromOTel.RawSpan

		// if raw span is not present (possible for unsupported protocols), then create a new span
		if spanForPersistence == nil {
			spanForPersistence = createSpanForPersistence(spanFromOTel)
		}

		// treat the value of `spanFromOTel.Protocol` as the protocol, if exists. `spanFromOTel.Protocol` won't be
		// available for `INTERNAL` spans
		if len(spanFromOTel.Protocol) > 0 {
			spanForPersistence.Protocol = spanFromOTel.Protocol
		}
		spanForPersistence.ParentSpanId = string(spanFromOTel.ParentSpanID)

		spanMapOfPersistentSpans[spanFromOTel.SpanID] = spanForPersistence
	}

	return &spanMapOfPersistentSpans
}

func setDestinationForRoot(root *stores.SpanFromOTel) {

	// if root is null or destination is already set, return
	if root == nil || (root.RawSpan != nil && root.RawSpan.Destination != "") {
		return
	}

	for _, child := range root.Children {
		rawChildSpan := child.RawSpan
		if rawChildSpan == nil {
			continue
		}
		if sourceOfChild := rawChildSpan.Source; len(sourceOfChild) > 0 {
			if root.RawSpan == nil {
				root.RawSpan = createSpanForPersistence(root)
			}
			root.RawSpan.Destination = sourceOfChild
			break
		}
	}
}

func createSpanForPersistence(spanFromOTel *stores.SpanFromOTel) *tracePersistenceModel.Span {
	return &tracePersistenceModel.Span{
		TraceId:  string(spanFromOTel.TraceID),
		SpanId:   string(spanFromOTel.SpanID),
		Protocol: spanFromOTel.Protocol,
	}
}

func evaluateIncidents(traceId typedef.TTraceid, scenarios map[typedef.TScenarioID]*scenarioGeneratorModel.Scenario, spansOfTrace typedef.TMapOfSpanIdToSpan) tracePersistenceModel.IncidentWithIssues {

	spans := make([]*tracePersistenceModel.Span, 0)
	for key := range spansOfTrace {
		spans = append(spans, spansOfTrace[key])
	}

	issueGroupList := make([]tracePersistenceModel.IssueGroup, 0)

	for _, scenario := range scenarios {
		issueGroupList = append(issueGroupList, tracePersistenceModel.IssueGroup{
			ScenarioId:      scenario.Id,
			ScenarioVersion: scenario.Version,
			Issues:          getListOfIssues(scenario, spansOfTrace),
		})
	}

	incidentWithIssues := tracePersistenceModel.IncidentWithIssues{
		IssueGroupList: issueGroupList,
		Incident: tracePersistenceModel.Incident{
			TraceId:                string(traceId),
			Spans:                  spans,
			IncidentCollectionTime: time.Now(),
		},
	}

	return incidentWithIssues
}

func getListOfIssues(scenario *scenarioGeneratorModel.Scenario, spanMap typedef.TMapOfSpanIdToSpan) []tracePersistenceModel.Issue {

	// 1. create a set of used workload ids
	workloadIdListInGroup := make(ds.Set[typedef.TWorkspaceID], 0)

	// this if condition is for the cases where there is no group by, so we take all the workload ids in the scenario and add it to the list
	if scenario.GroupBy == nil || len(scenario.GroupBy) == 0 {
		for k := range *scenario.Workloads {
			workloadIdListInGroup.Add(typedef.TWorkspaceID(k))
		}
	} else {
		for _, group := range scenario.GroupBy {
			workloadIdListInGroup.Add(typedef.TWorkspaceID(group.WorkloadId))
		}
	}

	// 2. create a set of workspaceIds vs spans
	workloadIdToSpansMap := make(map[typedef.TWorkspaceID][]*tracePersistenceModel.Span, 0)
	for _, span := range spanMap {
		workloadIdList := span.WorkloadIdList
		for _, workloadId := range workloadIdList {
			spans, ok := workloadIdToSpansMap[typedef.TWorkspaceID(workloadId)]
			if !ok {
				spans = make([]*tracePersistenceModel.Span, 0)
			}
			newSpans := append(spans, span)
			workloadIdToSpansMap[typedef.TWorkspaceID(workloadId)] = newSpans
		}
	}

	// 3. do a cartesian product of all the elements in workloadIdSet
	spansForGrpBy := make([]map[typedef.TWorkspaceID]*tracePersistenceModel.Span, 0)
	for workloadId, _ := range workloadIdListInGroup {
		arrSpans := workloadIdToSpansMap[typedef.TWorkspaceID(workloadId)]
		spansForGrpBy = getCartesianProductOfSpans(spansForGrpBy, workloadId, arrSpans)
	}

	// 4. iterate through spansForGrpBy and evaluate each groupBy clause
	issueMap := make(map[typedef.TIssueHash]tracePersistenceModel.Issue, 0)
	for _, mapOfIssueSpans := range spansForGrpBy {

		hash := scenario.Id + scenario.Version
		title := scenario.Title
		for _, group := range scenario.GroupBy {
			span, ok := mapOfIssueSpans[typedef.TWorkspaceID(group.WorkloadId)]
			if !ok {
				continue
			}

			// get hash and title
			hash = hash + TitleDelimiter + getTextFromStructMembers(group.Hash, span)
			title = title + TitleDelimiter + getTextFromStructMembers(group.Title, span)
		}
		md5OfHash := md5.Sum([]byte(hash))
		hash = hex.EncodeToString(md5OfHash[:])
		issueMap[typedef.TIssueHash(hash)] = tracePersistenceModel.Issue{
			IssueHash:  hash,
			IssueTitle: title,
		}

		// iterate over mapOfIssueSpans
		for _, span_ := range mapOfIssueSpans {
			span, ok := spanMap[typedef.TSpanId(span_.SpanId)]
			if !ok {
				continue
			}
			if span.IssueHashList == nil {
				span.IssueHashList = make([]string, 0)
			}
			set := ds.Set[string]{}.AddBulk(span.IssueHashList).Add(hash)
			span.IssueHashList = set.GetAll()
		}
	}

	// 5. iterate through the issueMap and create a list of issues
	issues := make([]tracePersistenceModel.Issue, 0)
	for _, issue := range issueMap {
		issues = append(issues, issue)
	}

	return issues
}

func getCartesianProductOfSpans(arrOfWorkLoadSpanMap []map[typedef.TWorkspaceID]*tracePersistenceModel.Span, workload typedef.TWorkspaceID, arrOfSpans []*tracePersistenceModel.Span) []map[typedef.TWorkspaceID]*tracePersistenceModel.Span {

	if len(arrOfSpans) == 0 {
		return arrOfWorkLoadSpanMap
	}

	if len(arrOfWorkLoadSpanMap) == 0 {
		arrOfWorkLoadSpanMap = append(arrOfWorkLoadSpanMap, make(map[typedef.TWorkspaceID]*tracePersistenceModel.Span, 0))
	}

	result := make([]map[typedef.TWorkspaceID]*tracePersistenceModel.Span, 0)
	for _, s1 := range arrOfWorkLoadSpanMap {
		for _, s2 := range arrOfSpans {
			newMap := s1
			newMap[workload] = s2
			result = append(result, newMap)
		}
	}
	return result
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

// prune removes the Spans that are not required - typedef Spans and server Spans that are not the root span
func prune(spans map[typedef.TSpanId]*stores.SpanFromOTel, currentSpanID typedef.TSpanId, isRootNode bool) ([]typedef.TSpanId, bool) {
	currentSpan := spans[currentSpanID]

	// call prune on the children
	newChildSpansArray := make([]stores.SpanFromOTel, 0)
	newChildIdsArray := make([]typedef.TSpanId, 0)
	for _, child := range currentSpan.Children {
		newChildIds, pruned := prune(spans, child.SpanID, false)
		if pruned {
			delete(spans, child.SpanID)
		}
		for _, spId := range newChildIds {

			span := spans[spId]
			span.ParentSpanID = currentSpan.SpanID

			// update the span in the map
			spans[span.SpanID] = span

			newChildSpansArray = append(newChildSpansArray, *span)
		}

		newChildIdsArray = append(newChildIdsArray, newChildIds...)
	}
	currentSpan.Children = newChildSpansArray
	spans[currentSpanID] = currentSpan

	parentSpan, isParentSpanPresent := spans[currentSpan.ParentSpanID]
	skipCurrentChild := false
	if currentSpan.Kind == INTERNAL && currentSpan.RawSpan == nil {
		skipCurrentChild = true
	} else if currentSpan.Kind == SERVER && isParentSpanPresent && parentSpan.Kind == CLIENT && currentSpan.RawSpan == nil {
		skipCurrentChild = true
	} else if currentSpan.Kind == CLIENT && currentSpan.RawSpan == nil {
		skipCurrentChild = true
	}

	if skipCurrentChild && !isRootNode {
		return newChildIdsArray, true
	}
	return []typedef.TSpanId{currentSpanID}, false
}
