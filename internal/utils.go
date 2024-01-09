package internal

import (
	"fmt"
	"github.com/zerok-ai/zk-utils-go/ds"
	"github.com/zerok-ai/zk-utils-go/logs"
	"go.opentelemetry.io/proto/otlp/common/v1"
	"strings"
)

var LogTag = "internal"

func ConvertMapToKVList(attrMap map[string]interface{}) []*v1.KeyValue {
	var attr []*v1.KeyValue
	for k, v := range attrMap {
		attr = append(attr, &v1.KeyValue{
			Key:   k,
			Value: GetAnyValueFromInterface(v),
		})
	}
	return attr
}

func GetAnyValueFromInterface(value interface{}) *v1.AnyValue {
	switch v := value.(type) {
	case string:
		return &v1.AnyValue{
			Value: &v1.AnyValue_StringValue{
				StringValue: v,
			},
		}
	case ds.Set[string]:
		return GetAnyValueFromInterface(v.GetAll())
	case []string:
		var values []*v1.AnyValue
		for _, item := range v {
			values = append(values, GetAnyValueFromInterface(item))
		}
		return &v1.AnyValue{
			Value: &v1.AnyValue_ArrayValue{
				ArrayValue: &v1.ArrayValue{
					Values: values,
				},
			},
		}
	case []interface{}:
		var values []*v1.AnyValue
		for _, item := range v {
			values = append(values, GetAnyValueFromInterface(item))
		}
		return &v1.AnyValue{
			Value: &v1.AnyValue_ArrayValue{
				ArrayValue: &v1.ArrayValue{
					Values: values,
				},
			},
		}
	case bool:
		return &v1.AnyValue{
			Value: &v1.AnyValue_BoolValue{
				BoolValue: v,
			},
		}
	case float64:
		return &v1.AnyValue{
			Value: &v1.AnyValue_DoubleValue{
				DoubleValue: v,
			},
		}
	case []byte:
		return &v1.AnyValue{
			Value: &v1.AnyValue_BytesValue{
				BytesValue: v,
			},
		}
	case int64:
		return &v1.AnyValue{
			Value: &v1.AnyValue_IntValue{
				IntValue: v,
			},
		}
	default:
		logger.Debug(LogTag, "Unknown type ", v)
	}
	return nil
}

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
