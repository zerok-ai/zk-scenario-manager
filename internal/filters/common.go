package filters

import "time"

const (
	FilterProcessingTickInterval = 10 * time.Second
	ScenarioRefreshInterval      = 20 * time.Minute
	TTLForTransientSets          = 120 * time.Second
	TTLForScenarioSets           = 5 * time.Minute
	batchSizeForRawDataCollector = 20
	timeRangeForRawDataQuery     = "-30m" // -5m, -10m, -1h etc
)
