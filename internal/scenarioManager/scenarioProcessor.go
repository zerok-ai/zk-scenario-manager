package scenarioManager

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	zkRedis "github.com/zerok-ai/zk-utils-go/storage/redis"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"scenario-manager/config"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/stores"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"sort"
	"strconv"
	"sync"
	"time"
)

var ctx = context.Background()

type ScenarioProcessor struct {
	id                      string
	cfg                     config.AppConfigs
	scenarioStore           *zkRedis.VersionedStore[model.Scenario]
	tracePersistenceService *tracePersistence.TracePersistenceService
	traceStore              *stores.TraceStore
	oTelStore               *stores.OTelDataHandler
	oTelProducer            *stores.OTelQueue
	traceRawDataCollector   *vzReader.VzReader
	issueRateMap            typedef.IssueRateMap
	mutex                   sync.Mutex
}

func NewScenarioProcessor(cfg config.AppConfigs, tps *tracePersistence.TracePersistenceService) (*ScenarioProcessor, error) {

	vs, err := zkRedis.GetVersionedStore[model.Scenario](&cfg.Redis, clientDBNames.ScenariosDBName, ScenarioRefreshInterval)
	if err != nil {
		return nil, err
	}

	otelProducer, err := stores.GetOTelProducer(cfg.Redis)
	if err != nil {
		return nil, err
	}

	fp := ScenarioProcessor{
		id:                      uuid.New().String(),
		scenarioStore:           vs,
		traceStore:              stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
		oTelProducer:            otelProducer,
		tracePersistenceService: tps,
		cfg:                     cfg,
	}

	reader, err := GetNewVZReader(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get new VZ reader")
	}
	fp.traceRawDataCollector = reader
	fp.oTelStore = stores.GetOTelStore(cfg.Redis)

	return &fp, nil
}

func (scenarioProcessor *ScenarioProcessor) Init() *ScenarioProcessor {

	// trigger recurring processing of one of the scenarios for available traces of interest
	duration := time.Duration(scenarioProcessor.cfg.ScenarioConfig.ProcessingIntervalInSeconds) * time.Second
	tickerTask := ticker.GetNewTickerTask("scenario-processor", duration, scenarioProcessor.scenarioTickHandler)

	zkLogger.DebugF(LoggerTag, "Starting to process all scenarios every %v", duration)
	tickerTask.Start()

	return scenarioProcessor
}

func (scenarioProcessor *ScenarioProcessor) scenarioTickHandler() {

	// 1. find scenario to process
	scenario := scenarioProcessor.findScenarioToProcess()
	if scenario == nil {
		zkLogger.InfoF(LoggerTag, "No scenario to process")
		return
	}

	//	 2. process current scenario
	scenarioProcessor.processScenario(scenario)
}

func (scenarioProcessor *ScenarioProcessor) findScenarioToProcess() *model.Scenario {

	// declare variables
	var currentScenarioIndex int64
	var err error
	var scenarioIds []int
	var sid int

	// 1. get all scenarios
	scenarios := scenarioProcessor.scenarioStore.GetAllValues()
	if len(scenarios) == 0 {
		zkLogger.Error(LoggerTag, "Error getting all scenarios")
		return nil
	}

	// 2. get the current scenario index to process from redis
	if currentScenarioIndex, err = scenarioProcessor.traceStore.GetIndexOfScenarioToProcess(); err != nil {
		zkLogger.Error(LoggerTag, "Error getting index of the scenario to process from redis")
		return nil
	}
	index := int(currentScenarioIndex % int64(len(scenarios)))

	// 3. sort all the scenarios based on the scenario ids.
	for key := range scenarios {
		// convert key to integer
		if sid, err = strconv.Atoi(key); err != nil {
			continue
		}
		scenarioIds = append(scenarioIds, sid)
	}
	sort.Ints(scenarioIds)

	// 4. get the scenario to process
	scenarioToProcess := scenarios[fmt.Sprintf("%d", scenarioIds[index])]

	return scenarioToProcess
}

func (scenarioProcessor *ScenarioProcessor) markProcessingEnd(scenario *model.Scenario, processedWorkloadSets string) {
	scenarioProcessor.traceStore.FinishedProcessingScenario(scenario.Id, scenarioProcessor.id, processedWorkloadSets)
}
