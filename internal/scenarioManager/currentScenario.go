package scenarioManager

import (
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"scenario-manager/internal/filters"
	"strconv"
	"strings"
)

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
	message := OTelMessage{Traces: allTraceIds, ProducerId: scenarioProcessor.id}
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
