package utils

import "time"

const (
	ScenarioType = "scenario_type"
	ScenarioId   = "scenario_id"
	TraceId      = "trace_id"
	SpanId       = "span_id"
	Source       = "source"
	Destination  = "destination"
	Limit        = "limit"
	Offset       = "offset"
	Duration     = "duration"
)

func ParseTimestamp(timestamp string) (time.Time, error) {
	// Define the layout of the timestamp string
	layout := "2006-01-02T15:04:05.999999Z"

	// Parse the timestamp string using the specified layout
	parsedTime, err := time.Parse(layout, timestamp)
	if err != nil {
		return time.Time{}, err
	}

	return parsedTime, nil
}

func CalendarDaysBetween(start, end time.Time) int {
	start = start.Truncate(24 * time.Hour)
	end = end.Truncate(24 * time.Hour)
	duration := end.Sub(start)
	days := int(duration.Hours() / 24)
	return days
}
