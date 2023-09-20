package filters

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/jmespath/go-jmespath"
	"github.com/pkg/errors"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
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
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	TitleDelimiter = "¦"
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

	vs, err := store.GetVersionedStore[scenarioGeneratorModel.Scenario](&cfg.Redis, "scenarios", ScenarioRefreshInterval)
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
	if len(scenarios) == 0 {
		zkLogger.Error(LoggerTag, "Error getting all scenarios")
		return
	}

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
	scenarioManager.initRateLimit(scenarioWithTraces)

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
			rateLimitedIncident := scenarioManager.rateLimitIssue(incident, scenarioWithTraces)
			if rateLimitedIncident == nil {
				continue
			}
			newIncidentList = append(newIncidentList, *rateLimitedIncident)
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
	scenarioManager.addRawDataToSpans(tracesFromOTelStore)
	endTime = time.Now()
	zkLogger.Info(LoggerTag, "Time taken to fetch raw spans ", endTime.Sub(startTime))

	return tracesFromOTelStore
}

// Enrich with the raw data for traces from vizier
func (scenarioManager *ScenarioManager) addRawDataToSpans(tracesFromOTelStore map[typedef.TTraceid]*stores.TraceFromOTel) {

	// create a map of protocol to set of traceIds
	tracesPerProtocol := make(typedef.TTraceIdSetPerProtocol, 0)
	for traceId, trace := range tracesFromOTelStore {

		for _, span := range trace.Spans {
			protocol := span.SpanForPersistence.Protocol
			traceSet, ok := tracesPerProtocol[protocol]
			if !ok {
				traceSet = make(ds.Set[string], 0)
			}
			tracesPerProtocol[protocol] = traceSet.Add(string(traceId))
		}
	}

	timeRange := timeRangeForRawDataQuery

	// handle http and exception
	protocol := stores.PHttp
	spansWithHTTPRawData := scenarioManager.getRawDataForHTTPAndException(timeRange, tracesPerProtocol)

	// we are sorting this slice so that the root is discovered as soon as possible. by doing this
	// we will ensure that the isRoot block in enrichWithRawDataForHTTPAndException is executed only for the root span and just once.
	// if we do not do this and the otel root span is not the pixie root span, then we will end up executing the isRoot block twice
	sort.Slice(spansWithHTTPRawData, func(i, j int) bool {
		return spansWithHTTPRawData[i].SpanId < spansWithHTTPRawData[j].SpanId
	})

	processedSpans := ds.Set[string]{}

	for _, spanWithRawDataFromPixie := range spansWithHTTPRawData {
		spanFromOTel := getSpanFromOTel(spanWithRawDataFromPixie.TraceId, spanWithRawDataFromPixie.SpanId, tracesFromOTelStore)
		if spanFromOTel == nil {
			pixieSpanId := spanWithRawDataFromPixie.SpanId
			traceId := spanWithRawDataFromPixie.TraceId
			traceFromOtel := tracesFromOTelStore[typedef.TTraceid(traceId)]
			rootSpanIdFromOtel := traceFromOtel.RootSpanID
			otelRootSpan := traceFromOtel.Spans[rootSpanIdFromOtel]

			if otelRootSpan != nil && otelRootSpan.Kind == SERVER && string(otelRootSpan.ParentSpanID) == pixieSpanId &&
				spanWithRawDataFromPixie.SourceIp == otelRootSpan.SourceIP && spanWithRawDataFromPixie.DestIp == otelRootSpan.DestIP {

				rootSpanByteArr, err := json.Marshal(otelRootSpan)
				if err != nil {
					zkLogger.Error(LoggerTag, "Error marshalling otelRootSpan:", err)
					continue
				}

				var rootClient stores.SpanFromOTel
				err = json.Unmarshal(rootSpanByteArr, &rootClient)
				if err != nil {
					zkLogger.Error(LoggerTag, "Error marshalling otelRootSpan:", err)
					continue
				}

				//rootClient.TraceID = otelRootSpan.TraceID
				rootClient.ParentSpanID = ""
				rootClient.SpanID = otelRootSpan.ParentSpanID
				rootClient.Kind = CLIENT
				rootClient.Children = []stores.SpanFromOTel{*otelRootSpan}
				rootClient.SpanForPersistence.SpanID = string(rootClient.SpanID)
				rootClient.SpanForPersistence.Kind = rootClient.Kind
				rootClient.SpanForPersistence.ParentSpanID = string(rootClient.ParentSpanID)
				rootClient.SpanForPersistence.Destination = spanWithRawDataFromPixie.Destination
				rootClient.SpanForPersistence.Source = ""
				rootClient.StartTime = otelRootSpan.StartTime

				otelRootSpan.SpanForPersistence.IsRoot = false

				// set protocol
				if protocol, ok := rootClient.GetStringAttribute(stores.OTelAttrProtocol); ok {
					rootClient.SpanForPersistence.Protocol = protocol
				}

				traceFromOtel.Spans[otelRootSpan.ParentSpanID] = &rootClient
				traceFromOtel.RootSpanID = rootClient.SpanID
				otelRootSpan = &rootClient
				spanFromOTel = &rootClient

				spanFromOTel.SpanForPersistence = enrichSpanFromHTTPRawData(rootClient.SpanForPersistence, &spanWithRawDataFromPixie)
				tracesFromOTelStore[typedef.TTraceid(traceId)] = traceFromOtel
				processedSpans.Add(spanWithRawDataFromPixie.SpanId)

			} else {
				continue
			}
		} else {
			if processedSpans.Contains(spanWithRawDataFromPixie.SpanId) {
				continue
			}
			spanFromOTel.SpanForPersistence = enrichSpanFromHTTPRawData(spanFromOTel.SpanForPersistence, &spanWithRawDataFromPixie)
			processedSpans.Add(spanWithRawDataFromPixie.SpanId)
		}
	}

	// mysql
	protocol = stores.PMySQL
	mySqlSet, ok := tracesPerProtocol[protocol]
	if ok && mySqlSet != nil {
		spansWithMySQLRawData := scenarioManager.collectMySQLRawData(timeRange, mySqlSet.GetAll())
		for index := range spansWithMySQLRawData {
			spanWithRawData := &spansWithMySQLRawData[index]
			spanFromOTel := getSpanFromOTel(spanWithRawData.TraceId, spanWithRawData.SpanId, tracesFromOTelStore)
			if spanFromOTel == nil {
				continue
			}
			spanFromOTel.SpanForPersistence = enrichSpanFromMySQLRawData(spanFromOTel.SpanForPersistence, spanWithRawData)
		}
	}

	// postgre
	protocol = stores.PPostgresql
	postGreSet, ok := tracesPerProtocol[protocol]
	if ok && postGreSet != nil {
		spansWithPostgresRawData := scenarioManager.collectPostgresRawData(timeRange, postGreSet.GetAll())
		for index := range spansWithPostgresRawData {
			spanWithRawData := &spansWithPostgresRawData[index]
			spanFromOTel := getSpanFromOTel(spanWithRawData.TraceId, spanWithRawData.SpanId, tracesFromOTelStore)
			if spanFromOTel == nil {
				continue
			}
			spanFromOTel.SpanForPersistence = enrichSpanFromPostgresRawData(spanFromOTel.SpanForPersistence, spanWithRawData)
		}

	}

}

func (scenarioManager *ScenarioManager) getRawDataForHTTPAndException(timeRange string, tracesPerProtocol typedef.TTraceIdSetPerProtocol) []models.HttpRawDataModel {
	// get the raw data for traces for HTTP and Exception
	httpSet, ok := tracesPerProtocol[stores.PHttp]
	if !ok || httpSet == nil {
		httpSet = make(ds.Set[string], 0)
	}

	mySqlSet := tracesPerProtocol[stores.PException]
	if ok || mySqlSet != nil {
		httpSet.Union(mySqlSet)
	}

	return scenarioManager.collectHTTPRawData(httpSet.GetAll(), timeRange)
}

func getSpanFromOTel(traceId, spanId string, tracesFromOTelStore map[typedef.TTraceid]*stores.TraceFromOTel) *stores.SpanFromOTel {
	traceFromOTel, ok := tracesFromOTelStore[typedef.TTraceid(traceId)]
	if !ok || traceFromOTel == nil {
		return nil
	}
	spanFromOTel, ok := traceFromOTel.Spans[typedef.TSpanId(spanId)]
	if !ok || spanFromOTel == nil {
		return nil
	}
	return spanFromOTel
}

func buildIncidentsForPersistence(scenariosWithTraces typedef.ScenarioToScenarioTracesMap, tracesFromOTel map[typedef.TTraceid]*stores.TraceFromOTel) []tracePersistenceModel.IncidentWithIssues {

	zkLogger.DebugF(LoggerTag, "Building scenario for persistence, trace_count=%d", len(tracesFromOTel))

	// a. iterate through the trace data from OTelStore and build the structure which can be saved in DB
	incidentsWithIssues := make([]tracePersistenceModel.IncidentWithIssues, 0)
	traceTreeForPersistence := make(map[typedef.TTraceid]*typedef.TMapOfSpanIdToSpan, 0)
	for traceId, traceFromOTel := range tracesFromOTel {
		spanMapOfPersistentSpans := make(typedef.TMapOfSpanIdToSpan, 0)
		for _, spanFromOTel := range traceFromOTel.Spans {
			spanMapOfPersistentSpans[spanFromOTel.SpanID] = spanFromOTel.SpanForPersistence
		}
		traceTreeForPersistence[traceId] = &spanMapOfPersistentSpans

		// find all the scenarios this trace belongs to
		scenarioMap := make(map[typedef.TScenarioID]*scenarioGeneratorModel.Scenario, 0)
		for scenarioId, scenarioWithTraces := range scenariosWithTraces {
			if scenarioWithTraces.Traces.Contains(traceId) {
				scenarioMap[scenarioId] = scenarioWithTraces.Scenario
			}
		}

		// evaluate this trace
		incidents := evaluateIncidents(traceId, scenarioMap, spanMapOfPersistentSpans)
		incidentsWithIssues = append(incidentsWithIssues, incidents)
	}

	return incidentsWithIssues
}

func evaluateIncidents(traceId typedef.TTraceid, scenarios map[typedef.TScenarioID]*scenarioGeneratorModel.Scenario, spansOfTrace typedef.TMapOfSpanIdToSpan) tracePersistenceModel.IncidentWithIssues {

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

func getListOfIssues(scenario *scenarioGeneratorModel.Scenario, spanMap typedef.TMapOfSpanIdToSpan) []tracePersistenceModel.Issue {

	// 1. create a set of used workload ids
	workloadIdListInGroup := make(ds.Set[typedef.TWorkloadId], 0)

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
	workloadIdToSpansMap := make(map[typedef.TWorkloadId][]*tracePersistenceModel.Span, 0)
	for _, span := range spanMap {
		workloadIdList := span.WorkloadIDList
		for _, workloadId := range workloadIdList {
			spans, ok := workloadIdToSpansMap[typedef.TWorkloadId(workloadId)]
			if !ok {
				spans = make([]*tracePersistenceModel.Span, 0)
			}
			newSpans := append(spans, span)
			workloadIdToSpansMap[typedef.TWorkloadId(workloadId)] = newSpans
		}
	}

	// 3. do a cartesian product of all the elements in workloadIdSet
	spansForGrpBy := make([]map[typedef.TWorkloadId]*tracePersistenceModel.Span, 0)
	for workloadId, _ := range workloadIdListInGroup {
		arrSpans := workloadIdToSpansMap[workloadId]
		spansForGrpBy = getCartesianProductOfSpans(spansForGrpBy, workloadId, arrSpans)
	}

	// 4. iterate through spansForGrpBy and evaluate each groupBy clause
	issueMap := make(map[typedef.TIssueHash]tracePersistenceModel.Issue, 0)
	for _, mapOfIssueSpans := range spansForGrpBy {

		hash := scenario.Id + scenario.Version
		title := scenario.Title
		for _, group := range scenario.GroupBy {
			span, ok := mapOfIssueSpans[typedef.TWorkloadId(group.WorkloadId)]
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
			span, ok := spanMap[typedef.TSpanId(span_.SpanID)]
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

func getCartesianProductOfSpans(arrOfWorkLoadSpanMap []map[typedef.TWorkloadId]*tracePersistenceModel.Span, workload typedef.TWorkloadId, arrOfSpans []*tracePersistenceModel.Span) []map[typedef.TWorkloadId]*tracePersistenceModel.Span {

	if len(arrOfSpans) == 0 {
		return arrOfWorkLoadSpanMap
	}

	if len(arrOfWorkLoadSpanMap) == 0 {
		arrOfWorkLoadSpanMap = append(arrOfWorkLoadSpanMap, make(map[typedef.TWorkloadId]*tracePersistenceModel.Span, 0))
	}

	result := make([]map[typedef.TWorkloadId]*tracePersistenceModel.Span, 0)
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

// currently this is hardcoded to extract just message out of the req body
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

		result, err = jmespath.Search("ReqBody", span)
		if err == nil {
			str, ok := result.(string)
			if ok {
				return extractFromException(str)
			}
		}
		zkLogger.Error(LoggerTag, "Error evaluating jmespath for span ", path+extractValue)

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

func (scenarioManager *ScenarioManager) collectHTTPRawData(traceIds []string, startTime string) []models.HttpRawDataModel {
	rawData, err := scenarioManager.traceRawDataCollector.GetHTTPRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for http traces ", traceIds, err)
		return make([]models.HttpRawDataModel, 0)
	}
	return rawData.Results
}

func (scenarioManager *ScenarioManager) collectMySQLRawData(timeRange string, traceIds []string) []models.MySQLRawDataModel {
	rawData, err := scenarioManager.traceRawDataCollector.GetMySQLRawData(traceIds, timeRange)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for mysql traces ", traceIds, err)
		return make([]models.MySQLRawDataModel, 0)
	}
	return rawData.Results
}

func (scenarioManager *ScenarioManager) collectPostgresRawData(timeRange string, traceIds []string) []models.PgSQLRawDataModel {
	rawData, err := scenarioManager.traceRawDataCollector.GetPgSQLRawData(traceIds, timeRange)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for postgres traces ", traceIds, err)
		return make([]models.PgSQLRawDataModel, 0)
	}
	return rawData.Results
}
