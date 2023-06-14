package filters

import (
	"fmt"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"sort"
	"strings"
)

type TraceEvaluator struct {
	scenario       *model.Scenario
	traceStore     *TraceStore
	namesOfAllSets []string
}

func (te TraceEvaluator) EvalScenario() (*string, error) {
	resultKey, err := te.evalFilter(te.scenario.Filter)
	if err != nil {
		err = te.traceStore.SetExpiryForSet(*resultKey, TTLForTransientSets)
	}
	return resultKey, err
}

func (te TraceEvaluator) evalFilter(f model.Filter) (*string, error) {

	var workloadTraceSetNames []string

	if f.Type == model.WORKLOAD {

		// shortlist the sets matching the prefixes
		matchingSets := matchPrefixesButNotEquals(*f.WorkloadIds, te.namesOfAllSets)

		// loop on matchingSets and union them
		for workloadId, sets := range matchingSets {
			if err := te.evalCondition(model.CONDITION_OR, sets, workloadId); err != nil {
				return nil, err
			}
		}
		workloadTraceSetNames = *f.WorkloadIds
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

func (te TraceEvaluator) evalCondition(c model.Condition, dataSetNames []string, resultSetName string) error {

	if c == model.CONDITION_AND {
		return te.traceStore.NewIntersectionSet(resultSetName, dataSetNames...)
	} else if c == model.CONDITION_OR {
		return te.traceStore.NewUnionSet(resultSetName, dataSetNames...)
	}
	return fmt.Errorf("unknown condition: %s", c)
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
	/*/
	// Hash the combined string using SHA1 calculating hash. avoiding sha256 for performance reasons
	hash := sha1.Sum([]byte(combined))

	// Convert the hash to a hex string
	hashString := hex.EncodeToString(hash[:])
	/*/
	hashString := combined
	/**/

	fmt.Println("Unique string:", hashString)

	return hashString
}
