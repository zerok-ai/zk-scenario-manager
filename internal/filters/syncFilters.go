package filters

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	store "github.com/zerok-ai/zk-utils-go/storage/redis"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"log"
	"scenario-manager/internal/config"
	"time"

	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	_ "github.com/zerok-ai/zk-rawdata-reader/vzReader/pxl"
)

const (
	FilterProcessingTickInterval = 10 * time.Second
	TTLForTransientSets          = 30 * time.Second
	TTLForScenarioSets           = 5 * time.Minute

	SCENARIO_SET_PREFIX = "scenario:"
)

type ScenarioManager struct {
	scenarioStore *store.VersionedStore[model.Scenario]

	traceStore  *TraceStore
	redisClient *redis.Client

	traceRawDataCollector *vzReader.VzReader
}

func getNewVZReader() (*vzReader.VzReader, error) {
	reader := vzReader.VzReader{
		CloudAddr:   "px.avinpx07.getanton.com:443",
		DirectVzId:  "94711f31-f693-46be-91c3-832c0f64b12f",
		DirectVzKey: "px-api-ce1bbae5-49c7-4d81-99e2-0d11865bb5df",
	}

	err := reader.Init()
	if err != nil {
		fmt.Printf("Failed to init reader, err: %v\n", err)
		return nil, err
	}

	return &reader, nil
}

func NewScenarioManager(cfg config.AppConfigs) *ScenarioManager {
	reader, err := getNewVZReader()
	if err != nil {
		return nil
	}
	fp := ScenarioManager{
		scenarioStore:         store.GetVersionedStore(*cfg.Redis, "scenarios", true, model.Scenario{}),
		traceStore:            GetTraceStore(*cfg.Redis, TTLForTransientSets),
		traceRawDataCollector: reader,
	}
	return &fp
}

func (f ScenarioManager) Init() ScenarioManager {
	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", FilterProcessingTickInterval, f.processFilters)
	tickerTask.Start()
	f.processFilters()
	return f
}

func (f ScenarioManager) processFilters() {
	scenarios := f.scenarioStore.GetAllValues()
	namesOfAllSets, err := f.traceStore.GetAllKeys()
	log.Println("All scenarios: ", scenarios)
	log.Println("All keys in traceStore: ", namesOfAllSets)
	if err != nil {
		log.Println("Error getting all keys from traceStore ", err)
		return
	}

	//setNames := make([]string, 0)
	for _, scenario := range scenarios {

		if scenario == nil {
			log.Println("Found nil scenario")
			continue
		}

		te := TraceEvaluator{scenario,
			f.traceStore,
			namesOfAllSets,
			TTLForScenarioSets}

		resultSetName, err := te.EvalScenario(SCENARIO_SET_PREFIX)

		if err != nil {
			log.Println("Error evaluating scenario", scenario, resultSetName, err)
			continue
		}

		if resultSetName != nil {
			//collect traces for scenario
			traceRawData := f.collectFullTraces(*resultSetName)
			if traceRawData == nil {
				continue
			}

			//TODO: save the data to the scenario store
		}
	}
}

func (f ScenarioManager) collectFullTraces(name string) *[]models.HTTP_raw_data {
	// get all the traces from the traceStore
	traceIds, err := f.traceStore.GetAllValuesFromSet(name)
	if err != nil {
		log.Println("Error getting all values from set ", name, err)
		return nil
	}

	// get the raw data for traces
	startTime := "-205m" // -5m, -10m, -1h etc
	log.Printf("sending %d traces to traceRawDataCollector.  start time = %s\n", len(traceIds), startTime)
	for i := 0; i < 5; i++ {
		fmt.Printf("|%s|\n", traceIds[i])
	}
	rawData, err := f.traceRawDataCollector.GetHTTPRawData(traceIds[:20], startTime)
	if err != nil {
		log.Println("Error getting raw data for traces ", traceIds, err)
		return nil
	}

	log.Printf("Number of raw values from traceRawDataCollector %d.  rawdata.ResultStats = %v", len(rawData.Results), rawData.ResultStats)
	return &rawData.Results
}
