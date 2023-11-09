package filters

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"scenario-manager/config"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/stores"
	"sort"
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

func (te TraceEvaluator) EvalScenario() ([]typedef.TTraceid, error) {
	resultKey, err := te.evalFilter(te.scenario.Filter)
	if err != nil {
		return nil, err
	}
	traceSetForScenario := *resultKey

	if !te.traceStore.SetExists(traceSetForScenario) {
		strError := fmt.Sprintf("No trace of interest for scenario: %v for scenario. Result key:%v doesn't exist", te.scenario.Id, *resultKey)
		zkLogger.Info(LoggerTagEvaluation, strError)
		return nil, fmt.Errorf(strError)
	}

	// get all the traceIds from the traceStore
	traceIds, err := te.traceStore.GetAllValuesFromSet(traceSetForScenario)
	if err != nil {
		return nil, err
	}
	te.traceStore.DeleteSet([]string{traceSetForScenario})

	result := make([]typedef.TTraceid, len(traceIds))
	for i, traceId := range traceIds {
		result[i] = typedef.TTraceid(traceId)
	}

	// cleanup the old sets
	//te.DeleteOldSets(te.namesOfAllSets, te.cfg.ScenarioConfig.RedisRuleSetCount)

	return result, err
}

func (te TraceEvaluator) evalFilter(f model.Filter) (*string, error) {

	var workloadTraceSetNames []string

	if f.Type == model.WORKLOAD {

		// shortlist the sets matching the prefixes
		matchingSets := getHSetsOfInterest(*f.WorkloadIds, te.namesOfAllSets)

		// loop on matchingSets and union them
		workloadTraceSetNames = make([]string, 0)
		for workloadId, sets := range matchingSets {
			resultSetName := workloadId
			if err := te.unionSets(sets, resultSetName); err != nil {
				return nil, err
			}
			workloadTraceSetNames = append(workloadTraceSetNames, resultSetName)
		}
	} else if f.Type == model.FILTER {
		ret, err := te.evalFilters(*f.Filters)
		if err != nil {
			return nil, err
		}
		workloadTraceSetNames = *ret
	}
	resultSetName := uniqueStringFromStringSet(f.Condition, workloadTraceSetNames)
	if err := te.evalCondition(f.Condition, workloadTraceSetNames, resultSetName); err != nil {
		return nil, err
	}

	return &resultSetName, nil
}

func (te TraceEvaluator) evalFilters(f model.Filters) (*[]string, error) {
	var results []string
	for i := 0; i < len(f); i++ {
		result, err := te.evalFilter(f[i])
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("something went wrong while evaluating filters: %v", f)
	}
	return &results, nil
}

func (te TraceEvaluator) unionSets(dataSetNames []string, resultSetName string) error {
	return te.evalCondition(model.CONDITION_OR, dataSetNames, resultSetName)
}

func (te TraceEvaluator) evalCondition(c model.Condition, dataSetNames []string, resultSetName string) error {
	var err error = nil
	if c == model.CONDITION_AND {
		err = te.traceStore.NewIntersectionSet(resultSetName, dataSetNames...)
	} else if c == model.CONDITION_OR {
		err = te.traceStore.NewUnionSet(resultSetName, dataSetNames...)
	}

	return err
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
	te.traceStore.DeleteSet(keysToDelete)
}

func getHSetsOfInterest(prefixes, keys []string) map[string][]string {

	// get all the workload sets except the latest
	setsOfWorkloadId := matchPrefixesButNotEquals(prefixes, keys)

	setsToReturn := setsOfWorkloadId
	return setsToReturn
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

func uniqueStringFromStringSet(condition model.Condition, set []string) string {

	// Copy the slice
	copied := make([]string, len(set))
	copy(copied, set)
	sort.Strings(copied)

	// Concatenate the strings
	combined := strings.Join(copied, string(condition))
	if len(copied) > 1 {
		combined = "(" + combined + ")"
	}
	/**/
	// Hash the combined string using SHA1 calculating hash. avoiding sha256 for performance reasons
	hash := sha1.Sum([]byte(combined))

	// Convert the hash to a hex string
	hashString := hex.EncodeToString(hash[:])
	/*/
	hashString := combined
	/**/

	return hashString
}
