package internal

import (
	"github.com/zerok-ai/zk-utils-go/logs"
	"go.opentelemetry.io/proto/otlp/common/v1"
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
