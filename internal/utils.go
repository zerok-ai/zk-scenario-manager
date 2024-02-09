package internal

import (
	"fmt"
	zklogger "github.com/zerok-ai/zk-utils-go/logs"
	"strings"
)

var LogTag = "utils"

func SplitTraceIdSpanId(traceIdSpanId string) (string, string, error) {

	parts := strings.Split(traceIdSpanId, "-")

	// Check if there are at least two parts
	if len(parts) >= 2 {
		traceId := parts[0]
		spanId := parts[1]
		return traceId, spanId, nil
	} else {
		zklogger.Error(LogTag, "Invalid input string, does not contain '-'")
		return "", "", fmt.Errorf("invalid traceIdSpanId string, does not contain '-'")
	}
}
