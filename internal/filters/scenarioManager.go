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
	"regexp"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/config"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"strings"
	"sync"
	"time"
)

const (
	ScenarioSetPrefix = "scenario:"
	TitleDelimiter    = "¦"
)

var queriesIgnoreList = []string{
	"commit",
	"rollback",
	"SET autocommit=0",
	"SET autocommit=1",
	"set session transaction read only",
	"set session transaction read write",
}

type ScenarioManager struct {
	cfg           config.AppConfigs
	scenarioStore *store.VersionedStore[scenarioGeneratorModel.Scenario]

	traceStore *stores.TraceStore
	oTelStore  *stores.OTelStore

	traceRawDataCollector *vzReader.VzReader

	tracePersistenceService tracePersistence.TracePersistenceService
	issueRateMap            typedef.IssueRateMap
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

func (scenarioManager *ScenarioManager) Init() *ScenarioManager {
	rateLimitTickerTask := ticker.GetNewTickerTask("rate-limit-processor", RateLimitTickerDuration, scenarioManager.processRateLimit)
	rateLimitTickerTask.Start()

	duration := time.Duration(scenarioManager.cfg.ScenarioConfig.ProcessingIntervalInSeconds) * time.Second

	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", duration, scenarioManager.processAllScenarios)
	tickerTask.Start()
	return scenarioManager
}

func (scenarioManager *ScenarioManager) Close() {

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

func (scenarioManager *ScenarioManager) processAllScenarios() {

	zkLogger.Info(LoggerTag, "Starting to process all scenarios")

	// 1. get all scenarios
	scenarios := scenarioManager.scenarioStore.GetAllValues()

	// 2. get all traceId sets from traceStore
	namesOfAllSets, err := scenarioManager.traceStore.GetAllKeys()
	zkLogger.DebugF(LoggerTag, "Number of available scenarios: %d, traceSets in db: %d", len(scenarios), len(namesOfAllSets))
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting all keys from traceStore ", err)
		return
	}

	// 3. get all the traceIds from the traceStore for all the scenarios
	allTraceIds, scenarioWithTraces := scenarioManager.getAllTraceIDs(scenarios, namesOfAllSets)
	zkLogger.DebugF(LoggerTag, "Number of available traceIds: %d", len(allTraceIds))

	// 4. process all traces against all scenarios
	scenarioManager.processTraceIDsAgainstScenarios(allTraceIds, scenarioWithTraces)

	zkLogger.Info(LoggerTag, "Finished processing all scenarios \n")
}

// processTraceIDsAgainstScenarios processes all the traceIds against all the scenarios and saves the incidents in the persistence store
// The traces are processed in the batches of size batchSizeForRawDataCollector
func (scenarioManager *ScenarioManager) processTraceIDsAgainstScenarios(traceIds []typedef.TTraceid, scenarioWithTraces typedef.ScenarioToScenarioTracesMap) {
	batch := 0
	traceIdCount := len(traceIds)
	for startIndex := 0; startIndex < traceIdCount; {
		batch += 1
		zkLogger.Info(LoggerTag, "processing batch", batch)

		// a. create the batch
		endIndex := startIndex + batchSizeForRawDataCollector
		if endIndex > traceIdCount {
			endIndex = traceIdCount
		}
		traceIdSubSet := traceIds[startIndex:endIndex]
		startIndex = endIndex

		// b. collect span relation and span raw data for the traceIDs
		tracesFromOTelStore := scenarioManager.getDataForTraces(traceIdSubSet)
		if tracesFromOTelStore == nil || len(tracesFromOTelStore) == 0 {
			continue
		}

		// c. Process each trace against all the scenarioWithTraces
		incidents := buildIncidentsForPersistence(scenarioWithTraces, tracesFromOTelStore)
		if incidents == nil || len(incidents) == 0 {
			zkLogger.ErrorF(LoggerTag, "no incidents to save")
			continue
		}

		// d. rate limit issues
		newIncidentList := make([]tracePersistenceModel.IncidentWithIssues, 0)
		for _, incident := range incidents {
			i := scenarioManager.rateLimitIssue(incident, scenarioWithTraces)
			if len(i.IssueGroupList) == 0 {
				continue
			}
			newIncidentList = append(newIncidentList, i)
		}
		if len(newIncidentList) == 0 {
			zkLogger.InfoF(LoggerTag, "processed batch %d. rate limited %d incidents. nothing to save", batch, len(incidents))
			continue
		}

		// e. store the trace data in the persistence store
		zkLogger.DebugF(LoggerTag, "Before sending incidents for persistence, incident count: %d", len(newIncidentList))
		startTime := time.Now()
		saveError := scenarioManager.tracePersistenceService.SaveIncidents(newIncidentList)
		if saveError != nil {
			zkLogger.Error(LoggerTag, "Error saving incidents", saveError)
		}
		endTime := time.Now()
		zkLogger.Info(LoggerTag, "Time taken to store data in persistent storage ", endTime.Sub(startTime))

		zkLogger.Info(LoggerTag, "processed batch", batch)
	}
}

// getAllTraceIDs gets all the traceIds from the traceStore for all the scenarios
func (scenarioManager *ScenarioManager) getAllTraceIDs(scenarios map[string]*scenarioGeneratorModel.Scenario, namesOfAllSets []string) ([]typedef.TTraceid, typedef.ScenarioToScenarioTracesMap) {
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
func (scenarioManager *ScenarioManager) getDataForTraces(traceIds []typedef.TTraceid) map[typedef.TTraceid]*stores.TraceFromOTel {

	// a. collect trace and span raw data for the traceIDs
	startTime := time.Now()
	tracesFromOTelStore, err := scenarioManager.oTelStore.GetSpansForTracesFromDB(traceIds)
	if err != nil {
		zkLogger.Error(LoggerTag, "error in getting data from OTel store", err)
	}
	if tracesFromOTelStore == nil || len(tracesFromOTelStore) == 0 {
		return nil
	}
	endTime := time.Now()
	zkLogger.InfoF(LoggerTag, "Time taken to fetch OTel spans[%v] ", endTime.Sub(startTime))

	// b. get the raw data for traces from vizier
	startTime = time.Now()
	scenarioManager.setRawSpans(tracesFromOTelStore)
	endTime = time.Now()
	zkLogger.Info(LoggerTag, "Time taken to fetch raw spans ", endTime.Sub(startTime))

	return tracesFromOTelStore
}

func (scenarioManager *ScenarioManager) setRawSpans(tracesFromOTelStore map[typedef.TTraceid]*stores.TraceFromOTel) {

	// b. create a map of protocol to set of traceIds
	tracesPerProtocol := make(typedef.TTraceIdSetPerProtocol, 0)
	for traceId, trace := range tracesFromOTelStore {

		for _, span := range trace.Spans {
			protocol := span.Protocol
			traceSet, ok := tracesPerProtocol[span.Protocol]
			if !ok {
				traceSet = make(ds.Set[string], 0)
			}
			tracesPerProtocol[protocol] = traceSet.Add(string(traceId))
		}
	}

	startTime := timeRangeForRawDataQuery

	// handle http and exception
	protocol := PHTTP
	spanForBatch := scenarioManager.getRawDataForHTTPAndException(tracesPerProtocol, startTime)
	processRawSpans(protocol, spanForBatch, tracesFromOTelStore)

	// mysql
	protocol = PMySQL
	mySqlSet, ok := tracesPerProtocol[protocol]
	if ok && mySqlSet != nil {
		spanForBatch = scenarioManager.collectMySQLRawData(mySqlSet.GetAll(), startTime)
		processRawSpans(protocol, spanForBatch, tracesFromOTelStore)
	}

	// postgres
	protocol = PPostgresql
	postGresSet, ok := tracesPerProtocol[protocol]
	if ok && postGresSet != nil {
		spanForBatch = scenarioManager.collectMySQLRawData(postGresSet.GetAll(), startTime)
		processRawSpans(protocol, spanForBatch, tracesFromOTelStore)
	}

}

func (scenarioManager *ScenarioManager) getRawDataForHTTPAndException(tracesPerProtocol typedef.TTraceIdSetPerProtocol, startTime string) []tracePersistenceModel.Span {
	// get the raw data for traces for HTTP and Exception
	httpSet, ok := tracesPerProtocol[PHTTP]
	if !ok || httpSet == nil {
		httpSet = make(ds.Set[string], 0)
	}

	mySqlSet := tracesPerProtocol[PMySQL]
	if ok || mySqlSet != nil {
		httpSet.Union(mySqlSet)
	}

	return scenarioManager.collectHTTPRawData(httpSet.GetAll(), startTime)
}

func processRawSpans(protocol string, spanForBatch []tracePersistenceModel.Span, tracesFromOTelStore map[typedef.TTraceid]*stores.TraceFromOTel) {
	for index := range spanForBatch {
		span := &spanForBatch[index]

		// 1. set the protocol of the current span if it is not present in otel data
		traceFromOTel, ok := tracesFromOTelStore[typedef.TTraceid(span.TraceId)]
		if !ok || traceFromOTel == nil {
			continue
		}
		spanFromOTel, ok := traceFromOTel.Spans[typedef.TSpanId(span.SpanId)]
		if !ok || spanFromOTel == nil {
			continue
		}
		if spanFromOTel.Protocol == "" {
			span.Protocol = protocol
		} else {
			span.Protocol = spanFromOTel.Protocol
		}

		// 2. do protocol specific validations
		if (span.Protocol == PHTTP || span.Protocol == PException) && span.RequestPayload != nil {
			httpRequestPayload := span.RequestPayload.(tracePersistenceModel.HTTPRequestPayload)
			span.Metadata["request_path"] = httpRequestPayload.ReqPath
			span.Metadata["method"] = httpRequestPayload.ReqMethod

			if span.Source == "" && spanFromOTel.Kind == SERVER {
				continue
			}
		}
		//else if span.Protocol == PMySQL && !isQuerySpan(span) {
		//	// don't consider this span if it doesn't contain the actual query
		//	continue
		//}

		// 3. set the current span as the raw span of OTel span
		spanFromOTel.RawSpan = span
	}
}

func isQuerySpan(span *tracePersistenceModel.Span) bool {
	for _, query := range queriesIgnoreList {
		body := span.RequestPayload.(tracePersistenceModel.HTTPRequestPayload).ReqBody
		if strings.HasSuffix(body, query) {
			return false
		}
	}
	return true
}

func buildIncidentsForPersistence(scenariosWithTraces typedef.ScenarioToScenarioTracesMap, tracesFromOTel map[typedef.TTraceid]*stores.TraceFromOTel) []tracePersistenceModel.IncidentWithIssues {

	zkLogger.DebugF(LoggerTag, "Building scenario for persistence, trace_count=%d", len(tracesFromOTel))

	// a. iterate through the trace data from OTelStore and build the structure which can be saved in DB
	traceTreeForPersistence := make(map[typedef.TTraceid]*typedef.TMapOfSpanIdToSpan, 0)
	for traceId, traceFromOTel := range tracesFromOTel {
		traceTreeForPersistence[traceId] = buildTraceForStorage(traceFromOTel)
	}

	// b. iterate through the trace data and create IncidentWithIssues for each trace
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
		incidents := evaluateIncidents(traceId, tracesFromOTel[traceId].RootSpanID, scenarioMap, *spanMapForTrace)
		incidentsWithIssues = append(incidentsWithIssues, incidents)
	}

	return incidentsWithIssues
}

func buildTraceForStorage(traceFromOTel *stores.TraceFromOTel) *typedef.TMapOfSpanIdToSpan {

	// set the destination doesn't exist for the root, add it
	setDestinationForRoot(traceFromOTel.Spans[traceFromOTel.RootSpanID])

	// remove spans which are not needed
	//prune(traceFromOTel.Spans, traceFromOTel.RootSpanID, true)

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

func evaluateIncidents(traceId typedef.TTraceid, rootSpanId typedef.TSpanId, scenarios map[typedef.TScenarioID]*scenarioGeneratorModel.Scenario, spansOfTrace typedef.TMapOfSpanIdToSpan) tracePersistenceModel.IncidentWithIssues {

	spans := make([]*tracePersistenceModel.Span, 0)
	for key := range spansOfTrace {
		spans = append(spans, spansOfTrace[key])
	}

	issueGroupList := make([]tracePersistenceModel.IssueGroup, 0)

	for _, scenario := range scenarios {
		listOfIssues := getListOfIssues(scenario, spansOfTrace)
		if len(listOfIssues) > 0 {
			issueGroupList = append(issueGroupList, tracePersistenceModel.IssueGroup{
				ScenarioId:      scenario.Id,
				ScenarioVersion: scenario.Version,
				Issues:          listOfIssues,
			})
		}
	}

	rootSpan := spansOfTrace[rootSpanId]
	var reqPath string
	if (rootSpan.Protocol == PHTTP || rootSpan.Protocol == PException) && rootSpan.RequestPayload != nil {
		httpRequestPayload := rootSpan.RequestPayload.(tracePersistenceModel.HTTPRequestPayload)
		reqPath = httpRequestPayload.ReqPath
	}

	incidentWithIssues := tracePersistenceModel.IncidentWithIssues{
		IssueGroupList: issueGroupList,
		Incident: tracePersistenceModel.Incident{
			TraceId:                string(traceId),
			Spans:                  spans,
			IncidentCollectionTime: time.Now().UTC(),
			EntryService:           rootSpan.Destination,
			LatencyNs:              rootSpan.LatencyNs,
			RootSpanTime:           rootSpan.Time,
			Protocol:               rootSpan.Protocol,
			EndPoint:               reqPath,
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

func extractStrings(input string) (string, string) {
	// Define regular expressions to match the patterns
	toJSONPattern := `#toJSON\((.*?)\)#`
	jsonExtractPattern := `#jsonExtract\((.*?)\)`

	// Compile the regular expressions
	toJSONRegex := regexp.MustCompile(toJSONPattern)
	jsonExtractRegex := regexp.MustCompile(jsonExtractPattern)

	// Extract response_payload and message using regular expressions
	responsePayload := toJSONRegex.FindStringSubmatch(input)
	message := jsonExtractRegex.FindStringSubmatch(input)

	// If the patterns are found, return the matched values
	if len(responsePayload) > 1 && len(message) > 1 {
		return responsePayload[1], message[1]
	}

	// If the patterns are not found, return empty strings
	return "", ""
}

func extractFromException(dump string) string {
	dump = strings.Trim(dump, "{}")
	parts := strings.Split(dump, "message=")
	if len(parts) > 1 {
		return parts[1]
	}
	parts = strings.Split(dump, ", stacktrace=")
	return parts[0]

}

func getTextFromStructMembers(path string, span *tracePersistenceModel.Span) string {
	var result interface{}
	var err error
	if strings.Contains(path, "toJSON") {
		path, extractValue := extractStrings(path)

		result, err = jmespath.Search("RequestPayload.ReqBody", span)
		if err == nil {
			str, ok := result.(string)
			if ok {
				return extractFromException(str)
			}
		}
		zkLogger.Error(LoggerTag, "Error evaluating jmespath for span ", path+extractValue)

	} else if strings.Contains(path, "response_payload.resp_status") {
		x := span.ResponsePayload.(tracePersistenceModel.HTTPResponsePayload)
		return x.RespStatus
	} else {
		result, err = jmespath.Search(path, span)
	}
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

func (scenarioManager *ScenarioManager) collectHTTPRawData(traceIds []string, startTime string) []tracePersistenceModel.Span {
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

func (scenarioManager *ScenarioManager) collectMySQLRawData(traceIds []string, startTime string) []tracePersistenceModel.Span {
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

func (scenarioManager *ScenarioManager) collectPostgresRawData(traceIds []string, startTime string) []tracePersistenceModel.Span {
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
