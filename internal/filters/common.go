package filters

import (
	"time"
)

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

func epochMilliSecondsToTime(epochNS uint64) time.Time {

	numberOfDigits := getNumDigits(epochNS)

	if numberOfDigits > 13 {
		return time.Unix(0, int64(epochNS)).UTC()
	}

	// Given Unix timestamp in milliseconds
	timestampMillis := int64(epochNS)

	// Convert to Unix timestamp in seconds by dividing by 1000
	timestampSeconds := timestampMillis / 1000

	// Convert to time.Time using time.Unix
	return time.Unix(timestampSeconds, 0).UTC()
}

func getNumDigits(timestamp uint64) int {
	numDigits := 0
	for timestamp != 0 {
		timestamp /= 10
		numDigits++
	}
	return numDigits
}
