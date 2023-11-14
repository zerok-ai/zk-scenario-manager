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
	TTLForTransientSets          = 120 * time.Second
	TTLForScenarioSets           = 15 * time.Minute
	batchSizeForRawDataCollector = 20
	timeRangeForRawDataQuery     = "-5m" // -5m, -10m, -1h etc

	scenarioProcessingTime = 5 * time.Minute
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
