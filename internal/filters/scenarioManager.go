package filters

//
//import (
//	"encoding/json"
//	"fmt"
//	"github.com/pkg/errors"
//	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
//	"github.com/zerok-ai/zk-utils-go/common"
//	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
//	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
//	store "github.com/zerok-ai/zk-utils-go/storage/redis"
//	ticker "github.com/zerok-ai/zk-utils-go/ticker"
//	"log"
//	"scenario-manager/internal/config"
//	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
//	tracePersistence "scenario-manager/internal/tracePersistence/service"
//	"strings"
//	"time"
//
//	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
//	_ "github.com/zerok-ai/zk-rawdata-reader/vzReader/pxl"
//)
//
//const (
//	FilterProcessingTickInterval = 10 * time.Second
//	ScenarioRefreshInterval      = 20 * time.Minute
//
//	TTLForTransientSets = 120 * time.Second
//	TTLForScenarioSets  = 5 * time.Minute
//
//	ScenarioSetPrefix = "scenario:"
//
//	LoggerTagScenarioManager = "scenario_manager"
//
//	batchSizeForRawDataCollector = 20
//
//	timeRangeForRawDataQuery = "-2h" // -5m, -10m, -1h etc
//)
//
//type ScenarioManager struct {
//	scenarioStore *store.VersionedStore[scenarioGeneratorModel.Scenario]
//
//	traceStore *TraceStore
//	oTelStore  *OtelStore
//
//	traceRawDataCollector *vzReader.VzReader
//
//	tracePersistenceService tracePersistence.TracePersistenceService
//}
//
//func getNewVZReader() (*vzReader.VzReader, error) {
//	reader := vzReader.VzReader{
//		CloudAddr:  "px.avinpx07.getanton.com:443",
//		ClusterId:  "94711f31-f693-46be-91c3-832c0f64b12f",
//		ClusterKey: "px-api-ce1bbae5-49c7-4d81-99e2-0d11865bb5df",
//	}
//
//	err := reader.Init()
//	if err != nil {
//		fmt.Printf("Failed to init reader, err: %v\n", err)
//		return nil, err
//	}
//
//	return &reader, nil
//}
//
//func NewScenarioManager(cfg config.AppConfigs, tps tracePersistence.TracePersistenceService) (*ScenarioManager, error) {
//	reader, err := getNewVZReader()
//	if err != nil {
//		return nil, errors.Wrap(err, "failed to get new VZ reader")
//	}
//
//	vs, err := store.GetVersionedStore[scenarioGeneratorModel.Scenario](cfg.Redis, "scenarios", ScenarioRefreshInterval)
//	if err != nil {
//		return nil, err
//	}
//
//	fp := ScenarioManager{
//		scenarioStore:           vs,
//		traceStore:              GetTraceStore(cfg.Redis, TTLForTransientSets),
//		oTelStore:               GetOtelStore(cfg.Redis),
//		traceRawDataCollector:   reader,
//		tracePersistenceService: tps,
//	}
//	return &fp, nil
//}
//
//func (scenarioManager ScenarioManager) Init() ScenarioManager {
//	// trigger recurring processing of trace data against filters
//	tickerTask := ticker.GetNewTickerTask("filter-processor", FilterProcessingTickInterval, scenarioManager.processAllScenarios)
//	tickerTask.Start()
//	return scenarioManager
//}
//
//func (scenarioManager ScenarioManager) Close() {
//
//	scenarioManager.scenarioStore.Close()
//	scenarioManager.traceStore.Close()
//	scenarioManager.oTelStore.Close()
//
//	scenarioManager.traceRawDataCollector.Close()
//	err := scenarioManager.tracePersistenceService.Close()
//	if err != nil {
//		zkLogger.Error(LoggerTagScenarioManager, "Error closing tracePersistenceService")
//		return
//	}
//
//}
//
//func (scenarioManager ScenarioManager) processAllScenarios() {
//
//	// 1. get all scenarios
//	scenarios := scenarioManager.scenarioStore.GetAllValues()
//
//	// 2. get all traceId sets from traceStore
//	namesOfAllSets, err := scenarioManager.traceStore.GetAllKeys()
//	zkLogger.DebugF(LoggerTagScenarioManager, "Number of available scenarios: %d", len(scenarios))
//	zkLogger.DebugF(LoggerTagScenarioManager, "Number of keys in traceStore: %d", len(namesOfAllSets))
//	if err != nil {
//		log.Println("Error getting all keys from traceStore ", err)
//		return
//	}
//
//	// 3. evaluate scenario filters on traceIdSets and attach the full traces against the scenarios
//	pScenarioArr := make([]tracePersistenceModel.IssuesDetail, 0)
//	for _, scenario := range scenarios {
//		// process each scenario
//		pScenario := scenarioManager.processScenario(scenario, namesOfAllSets)
//		if pScenario != nil {
//			pScenarioArr = append(pScenarioArr, *pScenario)
//		}
//	}
//	if len(pScenarioArr) == 0 {
//		zkLogger.DebugF(LoggerTagScenarioManager, "No scenarios to store")
//		return
//	}
//
//	// 4. store the scenario in the persistence store along with its traces
//	scenarioManager.tracePersistenceService.SaveIssues(pScenarioArr)
//}
//
//func (scenarioManager ScenarioManager) processScenario(scenario *scenarioGeneratorModel.Scenario, namesOfAllSets []string) *tracePersistenceModel.IssuesDetail {
//	if scenario == nil {
//		log.Println("Found nil scenario")
//		return nil
//	}
//
//	// a. evaluate the scenario and get the qualified traceIDs
//	traceEvaluator := NewTraceEvaluator(scenario, scenarioManager.traceStore, namesOfAllSets, TTLForScenarioSets)
//	if traceEvaluator == nil {
//		return nil
//	}
//	resultSetName, err := traceEvaluator.EvalScenario(ScenarioSetPrefix)
//	if err != nil {
//		zkLogger.Error(LoggerTagScenarioManager, "Error evaluating scenario", err)
//		return nil
//	}
//
//	// b. collect trace and span raw data for the qualified traceIDs
//	rawSpans := scenarioManager.collectFullTraces(*resultSetName)
//	if rawSpans == nil || len(*rawSpans) == 0 {
//		zkLogger.Debug(LoggerTagScenarioManager, "no spans found")
//		return nil
//	}
//
//	// c. get span co-relation data from the OTel store for the qualified traceIDs
//	traceIds := make([]string, 0)
//	for _, value := range *rawSpans {
//		traceId := value.TraceId
//		traceIds = append(traceIds, traceId)
//	}
//	traceFromOTelStore, err := scenarioManager.oTelStore.GetSpansForTracesFromDB(traceIds)
//	zkLogger.Debug(LoggerTagOtelStore, "Traces from OTel store: key count =", len(traceFromOTelStore))
//	if err != nil {
//		zkLogger.Error(LoggerTagScenarioManager, "error processing trace from OTel", err)
//		return nil
//	}
//
//	// d. Feed the span co-relation to raw data and build the scenario model for storage
//	dataForScenario := buildScenarioForPersistence(*scenario, *rawSpans, traceFromOTelStore)
//
//	return dataForScenario
//}
//
//func buildScenarioForPersistence(scenario scenarioGeneratorModel.Scenario, httpRawData []models.HttpRawDataModel, traceTree map[string]*TraceFromOTel) *tracePersistenceModel.IssuesDetail {
//
//	zkLogger.DebugF(LoggerTagScenarioManager, "Building scenario for persistence, scenario: %v", scenario.Id)
//
//	traceTreeForPersistence := make(map[string]map[string]*tracePersistenceModel.Span, 0)
//
//	// process all the spans in httpRawData and build the traceIdToSpansArrayMap
//	for _, value := range httpRawData {
//
//		span, err := createHttpSpan(value, traceTree)
//		// if complete span data can't be generated using data from OTelStore, don't save the complete trace
//		if err != nil {
//			continue
//		}
//
//		spanMapOfPersistentSpans, ok := traceTreeForPersistence[value.TraceId]
//		if !ok {
//			spanMapOfPersistentSpans = make(map[string]*tracePersistenceModel.Span, 0)
//			traceTreeForPersistence[value.TraceId] = spanMapOfPersistentSpans
//		}
//
//		spanMapOfPersistentSpans[span.SpanId] = span
//	}
//
//	traceIdToSpansArrayMap := make(map[string][]tracePersistenceModel.Span, 0)
//
//	// process the remaining members of traceTree and build the traceIdToSpansArrayMap
//	for traceId, traceFromOTel := range traceTree {
//
//		spanMapOfPersistentSpans, ok := traceTreeForPersistence[traceId]
//		if !ok {
//			spanMapOfPersistentSpans = make(map[string]*tracePersistenceModel.Span, 0)
//			traceTreeForPersistence[traceId] = spanMapOfPersistentSpans
//		}
//
//		spanArrOfPersistentSpans, ok := traceIdToSpansArrayMap[traceId]
//		if !ok {
//			spanArrOfPersistentSpans = make([]tracePersistenceModel.Span, 0)
//		}
//
//		for _, spanFromOTel := range traceFromOTel.spans {
//			spanForPersistence, ok := spanMapOfPersistentSpans[spanFromOTel.SpanID]
//			if !ok {
//				spanForPersistence = &tracePersistenceModel.Span{
//					SpanId:       spanFromOTel.SpanID,
//					ParentSpanId: spanFromOTel.ParentSpanID,
//				}
//				spanMapOfPersistentSpans[spanFromOTel.SpanID] = spanForPersistence
//			}
//			spanArrOfPersistentSpans = append(spanArrOfPersistentSpans, *spanForPersistence)
//		}
//		traceIdToSpansArrayMap[traceId] = spanArrOfPersistentSpans
//	}
//
//	zkLogger.DebugF(LoggerTagScenarioManager, "Building scenario for persistence, traceTree count: %v", traceTreeForPersistence)
//	zkLogger.DebugF(LoggerTagScenarioManager, "1. Building scenario for persistence, traceIdToSpansArrayMap count: %v", len(traceIdToSpansArrayMap))
//
//	//zkLogger.DebugF(LoggerTagScenarioManager, "2. Building scenario for persistence, traceIdToSpansArrayMap count: %v", len(traceIdToSpansArrayMap))
//	scenarioForPersistence := tracePersistenceModel.IssuesDetail{
//		ScenarioId:      scenario.Id,
//		ScenarioVersion: scenario.Version,
//		TraceToSpansMap: traceIdToSpansArrayMap,
//	}
//	return &scenarioForPersistence
//}
//
//func (scenarioManager ScenarioManager) collectFullTraces(name string) *[]models.HttpRawDataModel {
//
//	zkLogger.Debug(LoggerTagScenarioManager, "collectFullTraces from set", name)
//
//	// get all the traceIds from the traceStore
//	traceIds, err := scenarioManager.traceStore.GetAllValuesFromSet(name)
//	if err != nil {
//		zkLogger.Error(LoggerTagScenarioManager, "Error getting all values from set ", name, err)
//		return nil
//	}
//
//	// get the raw data for traces from vazir
//	startTime := timeRangeForRawDataQuery
//
//	results := make([]models.HttpRawDataModel, 0)
//	traceIdCount := len(traceIds)
//
//	for startIndex := 0; startIndex < traceIdCount; {
//		endIndex := startIndex + batchSizeForRawDataCollector
//		if endIndex > traceIdCount {
//			endIndex = traceIdCount
//		}
//		zkLogger.DebugF(LoggerTagScenarioManager, "calling traceRawDataCollector for %d traces", endIndex-startIndex)
//		zkLogger.DebugF(LoggerTagScenarioManager, "calling traceRawDataCollector for traces %v", traceIds[startIndex:endIndex])
//		rawData, err := scenarioManager.traceRawDataCollector.GetHTTPRawData(traceIds[startIndex:endIndex], startTime)
//		if err != nil {
//			zkLogger.Error(LoggerTagScenarioManager, "Error getting raw data for traces ", traceIds, err)
//			return nil
//		}
//		results = append(results, rawData.Results...)
//		startIndex = endIndex
//	}
//
//	zkLogger.Debug(LoggerTagScenarioManager, "Number of raw values from traceRawDataCollector = ", len(results))
//	return &results
//}
//
//type HttpRequest struct {
//	ReqPath    string `json:"req_path"`
//	ReqMethod  string `json:"req_method"`
//	ReqHeaders string `json:"req_headers"`
//	ReqBody    string `json:"req_body"`
//}
//
//func getHttpRequestData(value models.HttpRawDataModel) string {
//	req := HttpRequest{
//		ReqPath:    value.ReqPath,
//		ReqMethod:  value.ReqPath,
//		ReqHeaders: value.ReqMethod,
//		ReqBody:    value.ReqHeaders,
//	}
//	return jsonToString(req)
//}
//
//type HttpResponse struct {
//	RespStatus  string `json:"resp_status"`
//	RespMessage string `json:"resp_message"`
//	RespHeaders string `json:"resp_headers"`
//	RespBody    string `json:"resp_body"`
//}
//
//func getHttpResponseData(value models.HttpRawDataModel) string {
//	res := HttpResponse{
//		RespStatus:  value.RespStatus,
//		RespMessage: value.RespMessage,
//		RespHeaders: value.RespHeaders,
//		RespBody:    value.RespBody,
//	}
//	return jsonToString(res)
//}
//
//func createHttpSpan(value models.HttpRawDataModel, traceTree map[string]*TraceFromOTel) (*tracePersistenceModel.Span, error) {
//
//	trace, ok := traceTree[value.TraceId]
//	if !ok {
//		return nil, fmt.Errorf("trace not found in trace tree. traceId = %s", value.TraceId)
//	}
//
//	span, ok := trace.spans[value.SpanId]
//	if !ok {
//		return nil, fmt.Errorf("span not found in trace tree. traceId = %s, spanId = %s", value.TraceId, value.SpanId)
//	}
//
//	// value.WorkloadIds is a comma separated string. Convert it to a list
//	workloadIds := strings.Split(value.WorkloadIds, ",")
//
//	return &tracePersistenceModel.Span{
//		SpanId:          value.SpanId,
//		ParentSpanId:    span.ParentSpanID,
//		Source:          value.Source,
//		Destination:     value.Destination,
//		WorkloadIdList:  workloadIds,
//		Metadata:        map[string]interface{}{},
//		LatencyMs:       getLatencyPtr(value.Latency),
//		Protocol:        "http",
//		RequestPayload:  getHttpRequestData(value),
//		ResponsePayload: getHttpResponseData(value),
//	}, nil
//}
//
//func getLatencyPtr(latencyStr string) *float32 {
//	latency32, err := common.ToFloat32(latencyStr)
//	if err == nil {
//		return &latency32
//	}
//	return nil
//}
//
//func jsonToString(jsonObj interface{}) string {
//	jsonBytes, err := json.Marshal(jsonObj)
//	if err != nil {
//		return ""
//	}
//	return string(jsonBytes)
//}
