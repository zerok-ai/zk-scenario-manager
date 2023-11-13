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
	"scenario-manager/internal/filters"
	"scenario-manager/internal/stores"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
	"sort"
	"strconv"
	"strings"
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
	oTelProducer            *stores.TraceQueue
	traceRawDataCollector   *vzReader.VzReader
	issueRateMap            typedef.IssueRateMap
	mutex                   sync.Mutex
}

func NewScenarioProcessor(cfg config.AppConfigs, tps *tracePersistence.TracePersistenceService) (*ScenarioProcessor, error) {

	vs, err := zkRedis.GetVersionedStore[model.Scenario](&cfg.Redis, clientDBNames.ScenariosDBName, ScenarioRefreshInterval)
	if err != nil {
		return nil, err
	}

	oTelProducer, err := stores.GetTraceProducer(cfg.Redis, oTelProducerName)
	if err != nil {
		return nil, err
	}

	fp := ScenarioProcessor{
		id:                      "S" + uuid.New().String(),
		scenarioStore:           vs,
		traceStore:              stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
		oTelProducer:            oTelProducer,
		tracePersistenceService: tps,
		cfg:                     cfg,
	}

	reader, err := GetNewVZReader(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get new VZ reader")
	}
	fp.traceRawDataCollector = reader
	fp.oTelStore = stores.GetOTelStore(cfg.Redis)

	returnValue := fp.init()
	return returnValue, nil
}

func (scenarioProcessor *ScenarioProcessor) Close() {
	scenarioProcessor.scenarioStore.Close()
	scenarioProcessor.traceStore.Close()
	scenarioProcessor.oTelStore.Close()
	scenarioProcessor.oTelProducer.Close()
	scenarioProcessor.traceRawDataCollector.Close()
}

func (scenarioProcessor *ScenarioProcessor) init() *ScenarioProcessor {

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

func (scenarioProcessor *ScenarioProcessor) processScenario(scenario *model.Scenario) {

	// check if allowed to process current scenario
	if !scenarioProcessor.traceStore.AllowedToProcessScenarioId(scenario.Id, scenarioProcessor.id, scenarioProcessingTime) {
		zkLogger.Info(LoggerTag, "Another processor is already processing the scenario")
		return
	}

	// get all the workload sets to process for the current scenario
	namesOfAllSets, lastWorkloadSetsToProcess := scenarioProcessor.getWorkLoadSetsToProcess(scenario)

	// evaluate scenario and get all traceIds
	allTraceIds := filters.NewTraceEvaluator(scenarioProcessor.cfg, scenario, scenarioProcessor.traceStore, namesOfAllSets, filters.TTLForScenarioSets).EvalScenario()
	if allTraceIds == nil || len(allTraceIds) == 0 {
		zkLogger.Info(LoggerTag, "No traces satisfying the scenario")
	}

	// publish all traceIds to OTel queue for processing
	message := OTELTraceMessage{Traces: allTraceIds, ProducerId: scenarioProcessor.id}
	err := scenarioProcessor.oTelProducer.PublishTracesToQueue(message)
	if err != nil {
		return
	}

	// mark the processing end for the current scenario
	scenarioProcessor.markProcessingEnd(scenario, lastWorkloadSetsToProcess)
}

// getWorkLoadSetsToProcess gets all the workload sets to process for the current scenario
// the function also returns a comma separated string of the last workload set to process for each workload
func (scenarioProcessor *ScenarioProcessor) getWorkLoadSetsToProcess(scenario *model.Scenario) ([]string, string) {

	// 1 get the last processed workload ids
	processedWorkLoads := scenarioProcessor.getLastProcessedWorkLoadIdIndex(scenario)

	// 2 iterate over workload sets of the current scenario and get all the workload sets to process
	workloadSetsToProcess := make([]string, 0)
	lastWorkloadSetToProcess := make(map[string]string)
	for workloadId, _ := range *scenario.Workloads {

		//	 get all the sets from redis with the workloadId prefix
		setNames, err := scenarioProcessor.traceStore.GetAllKeys(workloadId + "_[0-9]+")
		if err != nil {
			zkLogger.DebugF(LoggerTag, "Error getting all keys from redis for workloadId: %v - %v", workloadId, err)
			continue
		}

		allPossibleSets := make([]string, 60)

		// iterate over all the set names and get the set index
		for _, setName := range setNames {
			// get the set index from the set name
			split := strings.Split(setName, "_")
			if len(split) != 2 {
				continue
			}
			setIndex, _ := strconv.Atoi(split[1])
			allPossibleSets[setIndex] = setName
		}

		// start after the last processed set index and cycle through the array for MAX_SUFFIX_COUNT
		// ignore the empty elements in the array till the first non-empty element is found
		// continue to collect values and break the loop if an empty element is found
		lastProcessedSetIndex, ok := processedWorkLoads[workloadId]
		if !ok {
			lastProcessedSetIndex = -1
		}

		foundFirstElement := false
		var lastSetName string
		for i := 0; i < MAX_SUFFIX_COUNT; i++ {
			currentIndex := ((lastProcessedSetIndex + 1) + i) % MAX_SUFFIX_COUNT
			value := allPossibleSets[currentIndex]
			if value == "" {
				if foundFirstElement {
					break
				}
				continue
			}
			foundFirstElement = true
			workloadSetsToProcess = append(workloadSetsToProcess, value)
			lastSetName = value
		}
		if foundFirstElement {
			lastWorkloadSetToProcess[workloadId] = lastSetName
		}
	}

	return workloadSetsToProcess, joinValuesInMapToCSV(lastWorkloadSetToProcess)
}

func (scenarioProcessor *ScenarioProcessor) getLastProcessedWorkLoadIdIndex(scenario *model.Scenario) map[string]int {
	processedWorkLoads := make(map[string]int)
	lastProcessedWorkloadIdSetsCSV := scenarioProcessor.traceStore.GetNameOfLastProcessedWorkloadSets(scenario.Id)
	if len(lastProcessedWorkloadIdSetsCSV) == 0 {
		zkLogger.DebugF(LoggerTag, "No workload sets have been previously processed for scenario: %v", scenario.Id)
		return processedWorkLoads
	}

	// split the last processed workload ids on comma
	lastProcessedWorkloadIdSets := strings.Split(lastProcessedWorkloadIdSetsCSV, ",")
	// iterate over lastProcessedWorkloadIdSets
	for _, lastProcessedWorkloadIdSet := range lastProcessedWorkloadIdSets {
		// lastProcessedWorkloadIdSet is of the format <workloadId>_<set index>
		split := strings.Split(lastProcessedWorkloadIdSet, "_")
		if len(split) != 2 {
			continue
		}
		processedWorkLoads[split[0]], _ = strconv.Atoi(split[1])
	}

	return processedWorkLoads
}
