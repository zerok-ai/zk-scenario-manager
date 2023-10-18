package stores

import (
	"context"
	"time"
)

var (
	ctx = context.Background()
)

const (
	LoggerTag = "stores"
)

func getNumDigits(timestamp uint64) int {
	numDigits := 0
	for timestamp != 0 {
		timestamp /= 10
		numDigits++
	}
	return numDigits
}

func latencyInMilliSeconds(epochStart uint64, epochEnd uint64) float64 {

	numberOfDigits := getNumDigits(epochStart)
	latency := epochEnd - epochStart

	// time is in milliseconds, convert to nanoseconds and return
	if numberOfDigits <= 13 {
		return float64(latency) * 1000000
	}

	return float64(latency)
}

func EpochMilliSecondsToTime(epochNS uint64) time.Time {

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

func EpochNanoSecondsToTime(epochNS uint64) time.Time {
	return time.Unix(0, int64(epochNS)).UTC()
}
