package filters

import "time"

const (
	LoggerTag          = "scenario-manager"
	RateLimitLoggerTag = "scenario-manager-rate-limit"
	INTERNAL           = "INTERNAL"
	CLIENT             = "CLIENT"
	SERVER             = "SERVER"

	// protocols
	PHTTP       = "http"
	PException  = "exception"
	PMySQL      = "mysql"
	PPostgresql = "postgresql"

	ScenarioRefreshInterval      = 20 * time.Minute
	TTLForTransientSets          = 120 * time.Second
	TTLForScenarioSets           = 5 * time.Minute
	RateLimitTickerDuration      = time.Duration(60) * time.Second
	batchSizeForRawDataCollector = 20
	timeRangeForRawDataQuery     = "-15m" // -5m, -10m, -1h etc
)

func epochNSToTime(epochNS uint64) time.Time {
	return time.Unix(0, int64(epochNS))
}
