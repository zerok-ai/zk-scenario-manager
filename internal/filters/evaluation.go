package filters

import (
	"fmt"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
)

func EvalFilter(f model.Filter, traceStore *TraceStore) (*string, error) {

	//calculate the name of the resultSet
	resultSetName := "hello"
	var results []string

	if f.Type == model.WORKLOAD {
		results = *f.WorkloadIds
	} else if f.Type == model.FILTER {
		ret, err := EvalFilters(*f.Filters, traceStore)
		if err != nil {
			return nil, err
		}
		results = *ret
	}
	if err := EvalCondition(f.Condition, results, resultSetName, traceStore); err != nil {
		return nil, err
	}

	return &resultSetName, nil
}

func EvalFilters(f model.Filters, traceStore *TraceStore) (*[]string, error) {
	var results []string
	for i := 0; i < len(f); i++ {
		result, err := EvalFilter(f[i], traceStore)
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

func EvalCondition(c model.Condition, dataSetNames []string, resultSetName string, traceStore *TraceStore) error {
	if c == "AND" {
		return traceStore.Union(resultSetName, dataSetNames...)
	} else if c == "OR" {
		return traceStore.Intersection(resultSetName, dataSetNames...)
	}
	return fmt.Errorf("unknown condition: %s", c)
}
