package scenarioManager

import (
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	LoggerTagScenarioProcessor       = "scenario-processor"
	currentProcessingWorkerKeyPrefix = "CPV"
)

type ScenarioProcessor struct {
	id                    string
	cfg                   config.AppConfigs
	scenarioStore         *zkRedis.VersionedStore[model.Scenario]
	traceStore            *stores.TraceStore
	oTelStore             *stores.OTelDataHandler
	oTelProducer          *stores.TraceQueue
	traceRawDataCollector *vzReader.VzReader
	issueRateMap          typedef.IssueRateMap
	mutex                 sync.Mutex
}

func NewScenarioProcessor(cfg config.AppConfigs) (*ScenarioProcessor, error) {

	vs, err := zkRedis.GetVersionedStore[model.Scenario](&cfg.Redis, clientDBNames.ScenariosDBName, ScenarioRefreshInterval)
	if err != nil {
		return nil, err
	}

	oTelProducer, err := stores.GetTraceProducer(cfg.Redis, OTelQueue)
	if err != nil {
		return nil, err
	}

	fp := ScenarioProcessor{
		id:            "S" + uuid.New().String(),
		scenarioStore: vs,
		traceStore:    stores.GetTraceStore(cfg.Redis, TTLForTransientSets),
		oTelProducer:  oTelProducer,
		cfg:           cfg,
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

func (scenarioProcessor *ScenarioProcessor) GetScenarioStore() *zkRedis.VersionedStore[model.Scenario] {
	return scenarioProcessor.scenarioStore
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

	zkLogger.DebugF(LoggerTagScenarioProcessor, "Starting to process all scenarios every %v", duration)
	tickerTask.Start()

	return scenarioProcessor
}

func (scenarioProcessor *ScenarioProcessor) scenarioTickHandler() {

	// 1. find scenario to process
	scenario := scenarioProcessor.findScenarioToProcess()
	if scenario == nil {
		zkLogger.InfoF(LoggerTagScenarioProcessor, "No scenario to process")
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
		zkLogger.Error(LoggerTagScenarioProcessor, "Error getting all scenarios")
		return nil
	}

	// 2. get the current scenario index to process from redis
	if currentScenarioIndex, err = scenarioProcessor.traceStore.GetIndexOfScenarioToProcess(); err != nil {
		zkLogger.Error(LoggerTagScenarioProcessor, "Error getting index of the scenario to process from redis")
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
	scenarioProcessor.FinishedProcessingScenario(scenario.Id, scenarioProcessor.id, processedWorkloadSets)
}

func (scenarioProcessor *ScenarioProcessor) FinishedProcessingScenario(scenarioId, scenarioProcessorId, lastProcessedSets string) {
	// remove the Key for currently processing worker
	key := fmt.Sprintf("%s_%s", currentProcessingWorkerKeyPrefix, scenarioId)
	result := scenarioProcessor.traceStore.GetValueForKey(key)
	if result != scenarioProcessorId {
		return
	}
	err := scenarioProcessor.traceStore.DeleteSets([]string{key})
	if err != nil {
		zkLogger.Error(LoggerTagScenarioProcessor, "Error deleting currently processing worker:", err)
		return
	}
}

func (scenarioProcessor *ScenarioProcessor) AllowedToProcessScenarioId(scenarioId, scenarioProcessorId string, scenarioProcessingTime time.Duration) bool {

	key := fmt.Sprintf("%s_%s", currentProcessingWorkerKeyPrefix, scenarioId)
	result := scenarioProcessor.traceStore.GetValueForKey(key)

	if result == "" || result != scenarioProcessorId {
		key := fmt.Sprintf("%s_%s", currentProcessingWorkerKeyPrefix, scenarioId)
		err := scenarioProcessor.traceStore.SetValueForKeyWithExpiry(key, scenarioProcessorId, scenarioProcessingTime)
		if err != nil {
			zkLogger.Error(LoggerTagScenarioProcessor, "Error setting last processed workload set:", err)
			return false
		}
		return true
	}
	return false
}

func (scenarioProcessor *ScenarioProcessor) processScenario(scenario *model.Scenario) {

	zkLogger.Debug(LoggerTagScenarioProcessor, "")
	zkLogger.DebugF(LoggerTagScenarioProcessor, "Processing scenario: %v", scenario.Id)

	// check if allowed to process current scenario
	if !scenarioProcessor.AllowedToProcessScenarioId(scenario.Id, scenarioProcessor.id, scenarioProcessingTime) {
		zkLogger.Info(LoggerTagScenarioProcessor, "Another processor is already processing the scenario")
		return
	}

	// get all the workload sets to process for the current scenario
	namesOfAllSets, lastWorkloadSetsToProcess := scenarioProcessor.getWorkLoadSetsToProcess(scenario)

	// mark the processing end for the current scenario
	defer scenarioProcessor.markProcessingEnd(scenario, lastWorkloadSetsToProcess)

	// evaluate scenario and get all traceIds
	allTraceIds := NewTraceEvaluator(scenarioProcessor.cfg, scenario, scenarioProcessor.traceStore, namesOfAllSets, TTLForScenarioSets).EvalScenario()
	if allTraceIds == nil || len(allTraceIds) == 0 {
		zkLogger.DebugF(LoggerTagScenarioProcessor, "No traces satisfying the scenario")
		return
	}

	// mark all traceIds as processed in redis
	setName := fmt.Sprintf("%s_%s_%d", SetPrefixOTelProcessed, scenario.Id, time.Now().UnixMilli())
	membersToAdd := make([]interface{}, 0)
	for _, traceId := range allTraceIds {
		membersToAdd = append(membersToAdd, string(traceId))
	}
	err := scenarioProcessor.traceStore.Add(setName, membersToAdd)
	if err != nil {
		zkLogger.DebugF(LoggerTagScenarioProcessor, "Error marking traceIds as processed in set %s : %v", setName, err)
	}
	scenarioProcessor.traceStore.SetExpiryForSet(setName, TTLForScenarioSets)

	// publish all traceIds to OTel queue for processing
	message := OTELTraceMessage{Scenario: *scenario, Traces: allTraceIds, ProducerId: scenarioProcessor.id}
	err = scenarioProcessor.oTelProducer.PublishTracesToQueue(message)
	if err != nil {
		return
	}

}

// getWorkLoadSetsToProcess gets all the workload sets to process for the current scenario
// the function also returns a comma separated string of the last workload set to process for each workload
func (scenarioProcessor *ScenarioProcessor) getWorkLoadSetsToProcess(scenario *model.Scenario) ([]string, string) {

	//  iterate over workload sets of the current scenario and get all the workload sets to process
	workloadSetsToProcess := make([]string, 0)
	lastWorkloadSetToProcess := make(map[string]string)
	for workloadId, _ := range *scenario.Workloads {

		//	 get all the sets from redis with the workloadId prefix
		setNames, err := scenarioProcessor.traceStore.GetAllKeysWithPrefixAndRegex(workloadId+"_", `[0-9]+$`)
		if err != nil {
			zkLogger.DebugF(LoggerTagScenarioProcessor, "Error getting all keys from redis for workloadId: %v - %v", workloadId, err)
			continue
		}

		workloadSetsToProcess = append(workloadSetsToProcess, setNames...)
	}

	return workloadSetsToProcess, joinValuesInMapToCSV(lastWorkloadSetToProcess)
}

func joinValuesInMapToCSV(data map[string]string) string {

	// Initialize an empty string to store the joined values
	var joinedValues []string

	// Iterate over the map and append the values to the slice
	for _, value := range data {
		joinedValues = append(joinedValues, value)
	}

	// Join the values into a comma-separated string
	result := strings.Join(joinedValues, ",")

	return result
}
