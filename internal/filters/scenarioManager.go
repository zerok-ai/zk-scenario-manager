package filters

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	"github.com/zerok-ai/zk-utils-go/common"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	store "github.com/zerok-ai/zk-utils-go/storage/redis"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"log"
	"scenario-manager/internal/config"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"strings"
	"time"

	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	_ "github.com/zerok-ai/zk-rawdata-reader/vzReader/pxl"
)

const (
	FilterProcessingTickInterval = 10 * time.Second
	TTLForTransientSets          = 30 * time.Second
	TTLForScenarioSets           = 5 * time.Minute

	ScenarioSetPrefix = "scenario:"

	LoggerTagScenarioManager = "scenario_manager"

	batchSizeForRawDataCollector = 20
)

type ScenarioManager struct {
	scenarioStore *store.VersionedStore[scenarioGeneratorModel.Scenario]

	traceStore  *TraceStore
	oTelStore   *OtelStore
	redisClient *redis.Client

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

	vs, err := store.GetVersionedStore[scenarioGeneratorModel.Scenario](cfg.Redis, "scenarios", time.Minute*2)
	if err != nil {
		return nil, err
	}

	fp := ScenarioManager{
		scenarioStore:           vs,
		traceStore:              GetTraceStore(cfg.Redis, TTLForTransientSets),
		oTelStore:               GetOtelStore(cfg.Redis),
		traceRawDataCollector:   reader,
		tracePersistenceService: tps,
	}
	return &fp, nil
}

func (scenarioManager ScenarioManager) Init() ScenarioManager {
	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", FilterProcessingTickInterval, scenarioManager.ProcessScenarios)
	tickerTask.Start()
	return scenarioManager
}

func (scenarioManager ScenarioManager) Close() {

	scenarioManager.scenarioStore.Close()
	scenarioManager.traceStore.Close()
	scenarioManager.oTelStore.Close()
	err := scenarioManager.redisClient.Close()
	if err != nil {
		zkLogger.Error(LoggerTagScenarioManager, "Error closing redis client")
	}

	scenarioManager.traceRawDataCollector.Close()
	err = scenarioManager.tracePersistenceService.Close()
	if err != nil {
		zkLogger.Error(LoggerTagScenarioManager, "Error closing tracePersistenceService")
		return
	}

}

func (scenarioManager ScenarioManager) ProcessScenarios() {

	// 1. get all scenarios
	scenarios := scenarioManager.scenarioStore.GetAllValues()

	// 2. get all traceId sets from traceStore
	namesOfAllSets, err := scenarioManager.traceStore.GetAllKeys()
	log.Printf("Number of available scenarios: %d", len(scenarios))
	log.Printf("Number of keys in traceStore: %d", len(namesOfAllSets))
	if err != nil {
		log.Println("Error getting all keys from traceStore ", err)
		return
	}

	// 3. evaluate scenario filters on traceIdSets and attach the full traces against the scenarios
	pScenarioArr := make([]tracePersistenceModel.Scenario, 0)
	for _, scenario := range scenarios {
		// process each scenario
		pScenario := scenarioManager.processScenario(scenario, namesOfAllSets)
		if pScenario != nil {
			pScenarioArr = append(pScenarioArr, *pScenario)
		}
	}

	// 4. store the scenario in the persistence store along with its traces
	scenarioManager.tracePersistenceService.SaveTraceList(pScenarioArr)
}

func (scenarioManager ScenarioManager) processScenario(scenario *scenarioGeneratorModel.Scenario, namesOfAllSets []string) *tracePersistenceModel.Scenario {
	if scenario == nil {
		log.Println("Found nil scenario")
		return nil
	}

	// a. evaluate the scenario
	te := TraceEvaluator{scenario, scenarioManager.traceStore, namesOfAllSets, TTLForScenarioSets}
	resultSetName, err := te.EvalScenario(ScenarioSetPrefix)
	if err != nil {
		zkLogger.Error(LoggerTagScenarioManager, "Error evaluating scenario", err)
		return nil
	}

	// b. collect full traces
	traceRawData := scenarioManager.collectFullTraces(*resultSetName)
	if traceRawData == nil {
		return nil
	}

	// c. get parent spanIds for the spanIds
	traceIds := make([]string, 0)
	for _, value := range *traceRawData {
		traceId := value.TraceId
		traceIds = append(traceIds, traceId)
	}
	//get trace data from the otel store
	traceTree, err := scenarioManager.oTelStore.GetTracesFromDBWithNonInternalSpans(traceIds)
	if err != nil {
		zkLogger.Error(LoggerTagScenarioManager, "error getting trace tree", err)
		return nil
	}

	// d. build the scenario for persistence
	return buildScenarioForPersistence(*scenario, *traceRawData, traceTree)
}

func buildScenarioForPersistence(scenario scenarioGeneratorModel.Scenario, httpRawData []models.HttpRawDataModel, traceTree map[string]*TraceFromOTel) *tracePersistenceModel.Scenario {

	// process all the spans in httpRawData and build the traceIdToSpansMap
	traceIdToSpansMap := map[string][]tracePersistenceModel.Span{}
	for _, value := range httpRawData {

		span, err := getHttpSpan(value, traceTree)
		// if complete span data can't be generated using data from OTelStore, don't save the complete trace
		if err != nil {
			continue
		}

		spanArr, ok := traceIdToSpansMap[value.TraceId]
		if !ok {
			spanArr = []tracePersistenceModel.Span{}
		}

		spanArr = append(spanArr, *span)
		traceIdToSpansMap[value.TraceId] = spanArr
	}

	// check whether we have all the spans of a trace. If not, we need to reject the trace
	tracesToRemove := make([]string, 0)
	for traceId, spans := range traceIdToSpansMap {

		// trace from otel store
		traceFromOTel, ok := traceTree[traceId]

		// if we have not got all the spans for a trace, don't save the complete trace
		if !ok || len(spans) != len(traceFromOTel.spans) {
			tracesToRemove = append(tracesToRemove, traceId)
			continue
		}
	}

	// remove traces which are incomplete
	for _, traceId := range tracesToRemove {
		delete(traceIdToSpansMap, traceId)
	}

	scenarioForPersistence := tracePersistenceModel.Scenario{
		ScenarioId:        scenario.Id,
		ScenarioType:      scenario.Type,
		ScenarioTitle:     scenario.Title,
		TraceIdToSpansMap: traceIdToSpansMap,
	}
	return &scenarioForPersistence
}

func (scenarioManager ScenarioManager) collectFullTraces(name string) *[]models.HttpRawDataModel {
	// get all the traceIds from the traceStore
	traceIds, err := scenarioManager.traceStore.GetAllValuesFromSet(name)
	if err != nil {
		zkLogger.Error(LoggerTagScenarioManager, "Error getting all values from set ", name, err)
		return nil
	}

	// get the raw data for traces from vazir
	startTime := "-20m" // -5m, -10m, -1h etc

	results := make([]models.HttpRawDataModel, 0)
	for index := 0; index < len(traceIds); index = index + batchSizeForRawDataCollector {

		length := batchSizeForRawDataCollector
		if index+batchSizeForRawDataCollector > len(traceIds) {
			length = len(traceIds) - index
		}
		rawData, err := scenarioManager.traceRawDataCollector.GetHTTPRawData(traceIds[index:length], startTime)
		if err != nil {
			zkLogger.Error(LoggerTagScenarioManager, "Error getting raw data for traces ", traceIds, err)
			return nil
		}
		results = append(results, rawData.Results...)
	}

	zkLogger.Debug(LoggerTagScenarioManager, "Number of raw values from traceRawDataCollector = %v", len(results))
	return &results
}

type HttpRequest struct {
	ReqPath    string `json:"req_path"`
	ReqMethod  string `json:"req_method"`
	ReqHeaders string `json:"req_headers"`
	ReqBody    string `json:"req_body"`
}

func getHttpRequestData(value models.HttpRawDataModel) string {
	req := HttpRequest{
		ReqPath:    value.ReqPath,
		ReqMethod:  value.ReqPath,
		ReqHeaders: value.ReqMethod,
		ReqBody:    value.ReqHeaders,
	}
	return jsonToString(req)
}

type HttpResponse struct {
	RespStatus  string `json:"resp_status"`
	RespMessage string `json:"resp_message"`
	RespHeaders string `json:"resp_headers"`
	RespBody    string `json:"resp_body"`
}

func getHttpResponseData(value models.HttpRawDataModel) string {
	res := HttpResponse{
		RespStatus:  value.RespStatus,
		RespMessage: value.RespMessage,
		RespHeaders: value.RespHeaders,
		RespBody:    value.RespBody,
	}
	return jsonToString(res)
}

func getHttpSpan(value models.HttpRawDataModel, traceTree map[string]*TraceFromOTel) (*tracePersistenceModel.Span, error) {

	trace, ok := traceTree[value.TraceId]
	if !ok {
		return nil, fmt.Errorf("trace not found in trace tree. traceId = %s", value.TraceId)
	}

	span, ok := trace.spans[value.SpanId]
	if !ok {
		return nil, fmt.Errorf("span not found in trace tree. traceId = %s, spanId = %s", value.TraceId, value.SpanId)
	}

	// value.WorkloadIds is a comma separated string. Convert it to a list
	workloadIds := strings.Split(value.WorkloadIds, ",")

	return &tracePersistenceModel.Span{
		SpanId:          value.SpanId,
		ParentSpanId:    span.ParentSpanID,
		Source:          value.Source,
		Destination:     value.Destination,
		WorkloadIdList:  workloadIds,
		Metadata:        map[string]interface{}{},
		LatencyMs:       getLatencyPtr(value.Latency),
		Protocol:        "http",
		RequestPayload:  getHttpRequestData(value),
		ResponsePayload: getHttpResponseData(value),
	}, nil
}

func getLatencyPtr(latencyStr string) *float32 {
	latency32, err := common.ToFloat32(latencyStr)
	if err == nil {
		return &latency32
	}
	return nil
}

func jsonToString(jsonObj interface{}) string {
	jsonBytes, err := json.Marshal(jsonObj)
	if err != nil {
		return ""
	}
	return string(jsonBytes)
}
