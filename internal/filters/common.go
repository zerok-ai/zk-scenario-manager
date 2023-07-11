package filters

import "time"

const (
	LoggerTag = "scenario-manager"

	FilterProcessingTickInterval = 5 * time.Minute
	ScenarioRefreshInterval      = 20 * time.Minute
	TTLForTransientSets          = 120 * time.Second
	TTLForScenarioSets           = 5 * time.Minute
	batchSizeForRawDataCollector = 20
	timeRangeForRawDataQuery     = "-15m" // -5m, -10m, -1h etc
)
