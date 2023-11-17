package scenarioManager

import (
	"fmt"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"scenario-manager/config"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/stores"
	"strconv"
	"strings"
	"time"
)

const (
	LoggerTagEvaluation = "evaluation"
)

type TraceEvaluator struct {
	scenario                   *model.Scenario
	traceStore                 *stores.TraceStore
	namesOfAllSets             []string
	ttlForTransientScenarioSet time.Duration
	cfg                        config.AppConfigs
}

func NewTraceEvaluator(cfg config.AppConfigs, scenario *model.Scenario, traceStore *stores.TraceStore, namesOfAllSets []string, ttlForTransientScenarioSet time.Duration) *TraceEvaluator {
	if scenario == nil {
		zkLogger.Error(LoggerTagEvaluation, "scenario is nil")
		return nil
	}
	if traceStore == nil {
		zkLogger.Error(LoggerTagEvaluation, "traceStore is nil")
		return nil
	}

	return &TraceEvaluator{
		scenario:                   scenario,
		traceStore:                 traceStore,
		namesOfAllSets:             namesOfAllSets,
		ttlForTransientScenarioSet: ttlForTransientScenarioSet,
		cfg:                        cfg,
	}
}

func (te TraceEvaluator) EvalScenario() []typedef.TTraceid {
	resultKey := te.evalFilter(te.scenario.Filter)
	if resultKey == nil {
		return nil
	}

	traceIds := te.getValidTracesForProcessing(*resultKey)
	return traceIds
}

func (te TraceEvaluator) getValidTracesForProcessing(traceSetForScenario string) []typedef.TTraceid {

	var finalTraces []string
	finalValueFromInputTraceSet := true

	// remove all the already processed traces from the set of traces to process
	keys, err := te.traceStore.GetAllKeysWithPrefixAndRegex(SetPrefixOTelProcessed+"_"+te.scenario.Id, `_[0-9]+$`)
	if err == nil || len(keys) > 0 {
		processedTracesKey := fmt.Sprintf("%s_%s_%d", SetPrefixOTelProcessedAggregate, te.scenario.Id, time.Now().UnixMilli())
		ok := te.unionSets(keys, processedTracesKey)
		if ok {
			finalValueFromInputTraceSet = false
			finalTraces = te.traceStore.GetValuesAfterSetDiff(traceSetForScenario, processedTracesKey)
		}
	}

	if finalValueFromInputTraceSet {
		finalTraces, _ = te.traceStore.GetAllValuesFromSet(traceSetForScenario)
	}

	traces := make([]typedef.TTraceid, 0)
	for _, trace := range finalTraces {
		traces = append(traces, typedef.TTraceid(trace))
	}

	return traces
}

func (te TraceEvaluator) evalFilter(f model.Filter) *string {

	var workloadTraceSetNames []string

	if f.Type == model.WORKLOAD {

		// shortlist the sets matching the workloadID prefix
		matchingSets := matchPrefixesButNotEquals(*f.WorkloadIds, te.namesOfAllSets)

		// loop on matchingSets and union them
		workloadTraceSetNames = make([]string, 0)
		for workloadId, sets := range matchingSets {
			resultSetName := fmt.Sprintf("%s_%s_%s_%d", SetPrefixWorkload, te.scenario.Id, workloadId, time.Now().UnixNano())
			if te.unionSets(sets, resultSetName) {
				workloadTraceSetNames = append(workloadTraceSetNames, resultSetName)
			}
		}
	} else if f.Type == model.FILTER {
		workloadTraceSetNames = te.evalFilters(*f.Filters)
	}

	resultSetName := fmt.Sprintf("%s_%s_%d", SetPrefixFilterResult, te.scenario.Id, time.Now().UnixNano())
	if !te.evalCondition(f.Condition, workloadTraceSetNames, resultSetName) {
		return nil
	}

	return &resultSetName
}

func (te TraceEvaluator) evalFilters(f model.Filters) []string {
	results := make([]string, 0)
	for i := 0; i < len(f); i++ {
		result := te.evalFilter(f[i])
		if result != nil {
			results = append(results, *result)
		}
	}
	return results
}

func (te TraceEvaluator) unionSets(dataSetNames []string, resultSetName string) bool {
	return te.evalCondition(model.CONDITION_OR, dataSetNames, resultSetName)
}

func (te TraceEvaluator) evalCondition(c model.Condition, dataSetNames []string, resultSetName string) bool {
	result := false
	if c == model.CONDITION_AND {
		result = te.traceStore.NewIntersectionSet(resultSetName, dataSetNames...)
	} else if c == model.CONDITION_OR {
		result = te.traceStore.NewUnionSet(resultSetName, dataSetNames...)
	}

	return result
}

func (te TraceEvaluator) DeleteOldSets(sets []string, maxSetCount int) {
	// sort sets by name
	expandedSets := make([]string, maxSetCount)
	for _, setName := range sets {
		// break setName on '_'
		setNameParts := strings.Split(setName, "_")
		if len(setNameParts) > 1 {
			index, err := strconv.Atoi(setNameParts[len(setNameParts)-1])
			if err == nil {
				expandedSets[index] = setName
			}
		}
	}

	// other than the latest set, mark the rest of the sets for deletion
	keysToDelete := make([]string, 0)
	for index, setName := range expandedSets {
		if setName == "" {
			continue
		}
		prev := index - 1
		if prev < 0 {
			prev = maxSetCount - 1
		}

		prevSetName := expandedSets[prev]
		if prevSetName != "" {
			keysToDelete = append(keysToDelete, prevSetName)
		}
	}
	te.traceStore.DeleteSets(keysToDelete)
}

func matchPrefixesButNotEquals(prefixes, keys []string) map[string][]string {
	matchingSets := map[string][]string{}
	for _, key := range keys {
		for _, prefix := range prefixes {
			if strings.HasPrefix(key, prefix) && key != prefix {
				arr, ok := matchingSets[prefix]
				if !ok {
					arr = []string{}
				}
				matchingSets[prefix] = append(arr, key)
				break
			}
		}
	}
	return matchingSets
}
