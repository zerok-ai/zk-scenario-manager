package stores

import (
	"encoding/json"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/proto/enrichedSpan"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	v12 "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/redis"
)

type ErrorStore struct {
	redisHandler redis.RedisHandlerInterface
}

func (e ErrorStore) initialize() *ErrorStore {
	return &e
}

func (e ErrorStore) Close() {
	e.redisHandler.Shutdown()
}

var LogTag = "ErrorStore"

func GetErrorStore(redisConfig config.RedisConfig) *ErrorStore {
	dbName := clientDBNames.ErrorDetailDBName
	redisHandler, err := redis.NewRedisHandler(&redisConfig, dbName)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error while creating resource redis handler:", err)
		return nil
	}

	errorStore := ErrorStore{redisHandler: redisHandler}.initialize()
	return errorStore
}

func (e ErrorStore) GetValueForKey(key string) string {
	result, err := e.redisHandler.Get(key)
	if err != nil {
		return ""
	}
	return result
}

func (e ErrorStore) GetExceptionDataForHashes(tracesFromOTelStore map[typedef.TTraceid]*TraceFromOTel) map[typedef.TTraceid]*TraceFromOTel {

	for _, spanFromOTel := range tracesFromOTelStore {
		for _, span := range spanFromOTel.Spans {
			spanEventMap := span.SpanEvents
			spanEventList := make([]*v1.Span_Event, 0)
			for _, event := range spanEventMap {
				var spanEvent *v1.Span_Event
				if event["name"] == "exception" {
					exceptionHash := event["exception_hash"].(string)
					exceptionData := e.GetExceptionDetailsFromRedisUsingHashes(exceptionHash)
					spanEvent = GetSpanEventFromException(exceptionData)
					spanEvent.Name = "exception"
				} else {
					attr := event["attributes"]
					var exceptionDataMap map[string]interface{}
					if attr == nil {
						exceptionDataMap = make(map[string]interface{})
					} else {
						exceptionDataMap = attr.(map[string]interface{})
					}
					zkLogger.InfoF(LogTag, "Exception data map %v\n", exceptionDataMap)
					spanEvent.Attributes = enrichedSpan.ConvertMapToKVList(exceptionDataMap).KeyValueList
					spanEvent.Name = event["name"].(string)
				}
				spanEventList = append(spanEventList, spanEvent)
			}
			if spanEventList != nil {
				span.Span.Events = spanEventList
			}
		}
	}
	return tracesFromOTelStore
}

func GetSpanEventFromException(e ExceptionDetails) *v1.Span_Event {
	spanEvent := v1.Span_Event{
		Attributes: []*v12.KeyValue{
			{
				Key:   "exception.message",
				Value: &v12.AnyValue{Value: &v12.AnyValue_StringValue{StringValue: e.Message}},
			},
			{
				Key:   "exception.stacktrace",
				Value: &v12.AnyValue{Value: &v12.AnyValue_StringValue{StringValue: e.Stacktrace}},
			},
			{
				Key:   "exception.type",
				Value: &v12.AnyValue{Value: &v12.AnyValue_StringValue{StringValue: e.Type}},
			},
		},
	}

	return &spanEvent
}

func (e ErrorStore) GetExceptionDetailsFromRedisUsingHashes(hash string) ExceptionDetails {
	var ExceptionDetails ExceptionDetails
	exceptionDetailsJSON, err := e.redisHandler.Get(hash)
	if err != nil {
		zkLogger.ErrorF(LogTag, "Error while getting exception details for hash %s: %v\n", hash, err)
		return ExceptionDetails
	}

	if utils.IsEmpty(exceptionDetailsJSON) {
		zkLogger.InfoF(LogTag, "Exception details for hash %s is Empty or Null \n", hash)
		return ExceptionDetails
	}

	err = json.Unmarshal([]byte(exceptionDetailsJSON), &ExceptionDetails)
	if err != nil {
		zkLogger.ErrorF(LogTag, "Error while unmarshalling exception details for hash %s: %v\n", hash, err)
		return ExceptionDetails
	}
	return ExceptionDetails
}

type ExceptionDetails struct {
	Message    string `json:"message"`
	Stacktrace string `json:"stacktrace"`
	Type       string `json:"type"`
}
