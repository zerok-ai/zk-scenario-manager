package internal

import (
	"fmt"
	"strings"
)

func SplitTraceIdSpanId(traceIdSpanId string) (string, string, error) {

	parts := strings.Split(traceIdSpanId, "-")

	// Check if there are at least two parts
	if len(parts) >= 2 {
		traceId := parts[0]
		spanId := parts[1]
		return traceId, spanId, nil
	} else {
		fmt.Println("Invalid input string, does not contain '-'")
		return "", "", fmt.Errorf("invalid traceIdSpanId string, does not contain '-'")
	}
}
