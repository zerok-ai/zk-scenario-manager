package filters

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	zkRedis "github.com/zerok-ai/zk-utils-go/storage/redis"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
	"scenario-manager/config"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"sort"
	"sync"
	"time"
)

type TMapOfSpanIdToSpan map[typedef.TSpanId]*tracePersistenceModel.Span

const (
	TitleDelimiter     = "¦"
	cacheSize      int = 20
)

var ctx = context.Background()

type ScenarioManager struct {
	cfg                     config.AppConfigs
	scenarioStore           *zkRedis.VersionedStore[scenarioGeneratorModel.Scenario]
	tracePersistenceService *tracePersistence.TracePersistenceService
	traceStore              *stores.TraceStore
	oTelStore               *stores.OTelDataHandler
	errorCacheSaveHooks     ErrorCacheSaveHooks[string]
	errorStoreReader        *zkRedis.LocalCacheKVStore[string]
	traceRawDataCollector   *vzReader.VzReader
	issueRateMap            typedef.IssueRateMap
	mutex                   sync.Mutex
}

func NewScenarioManager(cfg config.AppConfigs, tps *tracePersistence.TracePersistenceService) (*ScenarioManager, error) {

	vs, err := zkRedis.GetVersionedStore[scenarioGeneratorModel.Scenario](&cfg.Redis, clientDBNames.ScenariosDBName, ScenarioRefreshInterval)
	if err != nil {
		return nil, err
	}

	fp := ScenarioManager{
		scenarioStore:           vs,
		traceStore:              stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
		tracePersistenceService: tps,
		cfg:                     cfg,
	}
	fp.errorCacheSaveHooks = ErrorCacheSaveHooks[string]{scenarioManager: &fp}

	fp.errorStoreReader = getLRUCacheStore(cfg.Redis, &fp.errorCacheSaveHooks)
	reader, err := getNewVZReader(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get new VZ reader")
	}
	fp.traceRawDataCollector = reader
	fp.oTelStore = stores.GetOTelStore(cfg.Redis)

	return &fp, nil
}

func (scenarioManager *ScenarioManager) Init() *ScenarioManager {
	rateLimitTickerTask := ticker.GetNewTickerTask("rate-limit-processor", RateLimitTickerDuration, scenarioManager.processRateLimit)
	rateLimitTickerTask.Start()

	duration := time.Duration(scenarioManager.cfg.ScenarioConfig.ProcessingIntervalInSeconds) * time.Second

	// trigger recurring processing of trace data against filters
	zkLogger.DebugF(LoggerTag, "Starting to process all scenarios every %v", duration)
	tickerTask := ticker.GetNewTickerTask("filter-processor", duration, scenarioManager.processAllScenarios)
	tickerTask.Start()
	return scenarioManager
}

func (scenarioManager *ScenarioManager) Close() {

	scenarioManager.scenarioStore.Close()
	scenarioManager.traceStore.Close()
	scenarioManager.oTelStore.Close()
	scenarioManager.errorStoreReader.Close()

	scenarioManager.traceRawDataCollector.Close()
	err := (*scenarioManager.tracePersistenceService).Close()
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
	zkLogger.DebugF(LoggerTag, "Number of available scenarios: %d, workloads in db: %d", len(scenarios), len(namesOfAllSets))
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting all keys from traceStore ", err)
		return
	}

	// 3. get all the traceIds from the traceStore for all the scenarios
	allTraceIds, scenarioWithTraces := scenarioManager.getAllTraceIDs(scenarios, namesOfAllSets)
	zkLogger.Info(LoggerTag, "TraceIds to be processed: %d", len(allTraceIds))

	// 4. process all traces against all scenarios
	scenarioManager.processTraceIDsAgainstScenarios(allTraceIds, scenarioWithTraces)

	zkLogger.Info(LoggerTag, "Finished processing all scenarios \n")
}

// processTraceIDsAgainstScenarios processes all the traceIds against all the scenarios and saves the incidents in the persistence zkRedis
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
		incidents := scenarioManager.buildIncidentsForPersistence(scenarioWithTraces, tracesFromOTelStore)
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

		// e. zkRedis the trace data in the persistence zkRedis
		zkLogger.Info(LoggerTag, "Before sending incidents for persistence, incident count: %d", len(newIncidentList))
		startTime := time.Now()
		saveError := (*scenarioManager.tracePersistenceService).SaveIncidents(newIncidentList)
		if saveError != nil {
			zkLogger.Error(LoggerTag, "Error saving incidents", saveError)
		}
		endTime := time.Now()
		zkLogger.Info(LoggerTag, "Time taken to save zkRedis data in persistent storage ", endTime.Sub(startTime))

		zkLogger.Info(LoggerTag, "processed batch", batch)
	}
}

// getAllTraceIDs gets all the traceIds from the traceStore for all the scenarios
func (scenarioManager *ScenarioManager) getAllTraceIDs(scenarios map[string]*scenarioGeneratorModel.Scenario, namesOfAllSets []string) ([]typedef.TTraceid, typedef.ScenarioToScenarioTracesMap) {
	allTraceIds := make(ds.Set[typedef.TTraceid])
	scenarioWithTraces := make(typedef.ScenarioToScenarioTracesMap)
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

		zkLogger.InfoF(LoggerTag, "No of trace ids for scenario %v is %v", scenario.Id, len(tIds))
		allTraceIds.AddBulk(tIds)
		scenarioWithTraces[typedef.TScenarioID(scenario.Id)] = typedef.ScenarioTraces{Scenario: scenario, Traces: ds.Set[typedef.TTraceid]{}.AddBulk(tIds)}
	}
	return allTraceIds.GetAll(), scenarioWithTraces
}

// getDataForTraces collects span relation and span raw data for the traceIDs. The relation is collected from OTel zkRedis and raw data is collected from raw data zkRedis
func (scenarioManager *ScenarioManager) getDataForTraces(traceIds []typedef.TTraceid) map[typedef.TTraceid]*stores.TraceFromOTel {

	// a. collect trace and span raw data for the traceIDs
	startTime := time.Now()
	tracesFromOTelStore := scenarioManager.getDataFromOTelStore(traceIds)
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

func (scenarioManager *ScenarioManager) getDataFromOTelStore(traceIds []typedef.TTraceid) map[typedef.TTraceid]*stores.TraceFromOTel {
	tracesFromOTelStore, oTelErrors, err := scenarioManager.oTelStore.GetSpansForTracesFromDB(traceIds)
	if err != nil {
		zkLogger.Error(LoggerTag, "error in getting data from OTel zkRedis", err)
	}

	if oTelErrors != nil && len(oTelErrors) > 0 {
		errorData := make([]tracePersistenceModel.ErrorData, 0)
		for _, errorID := range oTelErrors {
			expStrPtr, isFromCache := scenarioManager.errorStoreReader.Get(errorID)
			if !isFromCache {
				expData := tracePersistenceModel.ErrorData{Id: errorID, Data: *expStrPtr}
				errorData = append(errorData, expData)
			}
		}
		if len(errorData) > 0 {
			(*scenarioManager.tracePersistenceService).SaveErrors(errorData)
		}
	}

	return tracesFromOTelStore
}

// Enrich with the raw data for traces from vizier
func (scenarioManager *ScenarioManager) addRawDataToSpans(tracesFromOTelStore map[typedef.TTraceid]*stores.TraceFromOTel) {

	// create a map of protocol to set of traceIds
	tracesPerProtocol := make(typedef.TTraceIdSetPerProtocol)
	for traceId, trace := range tracesFromOTelStore {

		for _, span := range trace.Spans {
			protocol := span.SpanForPersistence.Protocol
			traceSet, ok := tracesPerProtocol[protocol]
			if !ok {
				traceSet = make(ds.Set[string])
			}
			tracesPerProtocol[protocol] = traceSet.Add(string(traceId))
		}
	}

	timeRange := timeRangeForRawDataQuery

	// handle http and exception
	spansWithHTTPRawData := scenarioManager.getRawDataForHTTPAndError(timeRange, tracesPerProtocol)
	zkLogger.Info(LoggerTag, "Number of spans with HTTP raw data: %v", len(spansWithHTTPRawData))

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
			traceFromOTel := tracesFromOTelStore[typedef.TTraceid(traceId)]
			rootSpanIdFromOTel := traceFromOTel.RootSpanID
			oTelRootSpan := traceFromOTel.Spans[rootSpanIdFromOTel]

			if oTelRootSpan != nil && oTelRootSpan.Kind == SERVER && string(oTelRootSpan.ParentSpanID) == pixieSpanId {

				rootSpanByteArr, err := json.Marshal(oTelRootSpan)
				if err != nil {
					zkLogger.Error(LoggerTag, "Error marshalling oTelRootSpan:", err)
					continue
				}

				var rootClient stores.SpanFromOTel
				err = json.Unmarshal(rootSpanByteArr, &rootClient)
				if err != nil {
					zkLogger.Error(LoggerTag, "Error marshalling oTelRootSpan:", err)
					continue
				}

				//rootClient.TraceID = oTelRootSpan.TraceID
				rootClient.ParentSpanID = ""
				rootClient.SpanID = oTelRootSpan.ParentSpanID
				rootClient.Kind = CLIENT
				rootClient.Children = []stores.SpanFromOTel{*oTelRootSpan}
				rootClient.SpanForPersistence.SpanID = string(rootClient.SpanID)
				rootClient.SpanForPersistence.Kind = rootClient.Kind
				rootClient.SpanForPersistence.ParentSpanID = string(rootClient.ParentSpanID)
				rootClient.SpanForPersistence.Destination = oTelRootSpan.Source
				rootClient.SpanForPersistence.Source = ""
				rootClient.StartTimeNS = oTelRootSpan.StartTimeNS

				oTelRootSpan.SpanForPersistence.IsRoot = false

				traceFromOTel.Spans[oTelRootSpan.ParentSpanID] = &rootClient
				traceFromOTel.RootSpanID = rootClient.SpanID
				oTelRootSpan = &rootClient
				spanFromOTel = &rootClient

				spanFromOTel.SpanForPersistence = enrichSpanFromHTTPRawData(rootClient.SpanForPersistence, &spanWithRawDataFromPixie, scenarioManager.cfg.ScenarioConfig.EBPFSchemaVersion)
				tracesFromOTelStore[typedef.TTraceid(traceId)] = traceFromOTel
				processedSpans.Add(spanWithRawDataFromPixie.SpanId)

			} else {
				continue
			}
		} else {
			if processedSpans.Contains(spanWithRawDataFromPixie.SpanId) {
				continue
			}
			spanFromOTel.SpanForPersistence = enrichSpanFromHTTPRawData(spanFromOTel.SpanForPersistence, &spanWithRawDataFromPixie, scenarioManager.cfg.ScenarioConfig.EBPFSchemaVersion)
			processedSpans.Add(spanWithRawDataFromPixie.SpanId)
		}
	}

	// mysql
	mySqlSet, ok := tracesPerProtocol[typedef.ProtocolTypeDB]
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
	postGreSet, ok := tracesPerProtocol[typedef.ProtocolTypeDB]
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

func (scenarioManager *ScenarioManager) getRawDataForHTTPAndError(timeRange string, tracesPerProtocol typedef.TTraceIdSetPerProtocol) []models.HttpRawDataModel {
	// get the raw data for traces for HTTP and Exception
	httpProtocolSet, ok := tracesPerProtocol[typedef.ProtocolTypeHTTP]
	if !ok || httpProtocolSet == nil {
		httpProtocolSet = make(ds.Set[string])
	}

	grpcProtocolSet, ok := tracesPerProtocol[typedef.ProtocolTypeGRPC]
	if !ok || grpcProtocolSet == nil {
		grpcProtocolSet = make(ds.Set[string])
	}

	httpSet := httpProtocolSet.Union(grpcProtocolSet)

	zkLogger.Info(LoggerTag, "Number of traceId requested: %v", len(httpSet))
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

func (scenarioManager *ScenarioManager) buildIncidentsForPersistence(scenariosWithTraces typedef.ScenarioToScenarioTracesMap, tracesFromOTel map[typedef.TTraceid]*stores.TraceFromOTel) []tracePersistenceModel.IncidentWithIssues {

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

		// find all the scenarios this trace belongs to
		scenarioMap := make(map[typedef.TScenarioID]*scenarioGeneratorModel.Scenario)
		for scenarioId, scenarioWithTraces := range scenariosWithTraces {
			if scenarioWithTraces.Traces.Contains(traceId) {
				scenarioMap[scenarioId] = scenarioWithTraces.Scenario
			}
		}

		// evaluate this trace
		incidents := scenarioManager.evaluateIncidents(traceId, scenarioMap, spanMapOfPersistentSpans)
		incidentsWithIssues = append(incidentsWithIssues, incidents)
	}

	return incidentsWithIssues
}

func (scenarioManager *ScenarioManager) evaluateIncidents(traceId typedef.TTraceid, scenarios map[typedef.TScenarioID]*scenarioGeneratorModel.Scenario, spansOfTrace TMapOfSpanIdToSpan) tracePersistenceModel.IncidentWithIssues {

	spans := make([]*tracePersistenceModel.Span, 0)
	for key := range spansOfTrace {
		spans = append(spans, spansOfTrace[key])
	}

	issueGroupList := make([]tracePersistenceModel.IssueGroup, 0)

	for _, scenario := range scenarios {
		listOfIssues := scenarioManager.getListOfIssuesForScenario(scenario, spansOfTrace)
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

func (scenarioManager *ScenarioManager) getListOfIssuesForScenario(scenario *scenarioGeneratorModel.Scenario, spanMap TMapOfSpanIdToSpan) []tracePersistenceModel.Issue {

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
				//TODO this data should come from ebpf. continue for now
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
				//TODO this data should come from ebpf. do nothing for now.
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
	scenarioManager *ScenarioManager
}

func (errorCacheSaveHooks *ErrorCacheSaveHooks[T]) PreCacheSaveHookAsync(key string, value *T) *zkErrors.ZkError {

	if value != nil {
		strToSave := fmt.Sprintf("%v", *value)
		return (*errorCacheSaveHooks.scenarioManager.tracePersistenceService).SaveErrors([]tracePersistenceModel.ErrorData{{Id: key, Data: strToSave}})
	}

	return nil
}
