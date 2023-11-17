package scenarioManager

import (
	"strings"
	"time"
)

const (
	LoggerTag = "scenario-processor"

	MAX_SUFFIX_COUNT = 60

	RateLimitLoggerTag = "scenario-manager-rate-limit"
	INTERNAL           = "INTERNAL"
	CLIENT             = "CLIENT"
	SERVER             = "SERVER"

	ScenarioRefreshInterval      = 20 * time.Minute
	TTLForTransientSets          = 30 * time.Second
	TTLForScenarioSets           = 15 * time.Minute
	batchSizeForRawDataCollector = 20
	timeRangeForRawDataQuery     = "-15m" // -5m, -10m, -1h etc

	scenarioProcessingTime = 5 * time.Minute

	/*
		Workload id sets (these are created by oTel receiver)
		-----------
		<workloadId>_<number>

		OTel
		-----------
		OTel_P_<scenarioID>_<time>
		OTel_P_All_<scenarioID>_<time>
	*/
	SetPrefixOTelProcessed          = "OTel_P"
	SetPrefixOTelProcessedAggregate = "OTel_P_All"

	/*
		filter evaluation
		-----------
		a) union set for each workload

		workload_<scenarioID>_<workloadId>_<time_nano>

		b) union set for each filter

		filter_<scenarioID>_<time_nano>

	*/
	SetPrefixWorkload     = "workload"
	SetPrefixFilterResult = "filter"

	/*
		ebpf
		-----------
		ebpf_temp_<workerID>_<time>
		ebpf_All_P_<workerID>
		ebpf_P_<workerID>_<time>
	*/
	SetPrefixEBPFTemp               = "ebpf_temp"
	SetPrefixEBPFProcessed          = "ebpf_P"
	SetPrefixEBPFProcessedAggregate = "ebpf_All_P"

	currentProcessingWorkerKeyPrefix = "CPV"
)

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
