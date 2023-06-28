package filters

import (
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"reflect"
	"time"
)

const LoggerTagOtelStore = "otelStore"

type OtelStore struct {
	redisClient *redis.Client
}

func (t OtelStore) initialize() *OtelStore {
	return &t
}

func (t OtelStore) Close() {
	t.redisClient.Close()
}

func GetOtelStore(redisConfig *config.RedisConfig) *OtelStore {
	dbName := "otel"
	zkLogger.Debug(LoggerTagOtelStore, "GetOtelStore: config=", redisConfig, "dbName=", dbName, "dbID=", redisConfig.DBs[dbName])
	readTimeout := time.Duration(redisConfig.ReadTimeout) * time.Second
	_redisClient := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprint(redisConfig.Host, ":", redisConfig.Port),
		Password:    "",
		DB:          redisConfig.DBs[dbName],
		ReadTimeout: readTimeout,
	})

	return OtelStore{redisClient: _redisClient}.initialize()
}

type SpanFromOTel struct {
	Kind         string `json:"spanKind"`
	ParentSpanID string `json:"parentSpanID"`
}

type TraceFromOTel struct {
	// spans is a map of spanID to span
	spans map[string]*SpanFromOTel
}

func (t OtelStore) GetTracesFromDBWithNonInternalSpans(keys []string) (map[string]*TraceFromOTel, error) {
	client := t.redisClient

	// Use the MGet command to retrieve the values
	result, err := client.MGet(ctx, keys...).Result()
	if err != nil {
		zkLogger.Error(LoggerTagOtelStore, "Error retrieving values from Redis:", err)
		return nil, err
	}

	traceMap := make(map[string]*TraceFromOTel)
	// Process the result
	for i, trace := range result {

		traceId := keys[i]
		traceFromOTel := traceMap[traceId]
		if traceFromOTel == nil {
			traceFromOTel = &TraceFromOTel{spans: map[string]*SpanFromOTel{}}
			traceMap[traceId] = traceFromOTel
		}

		if trace == nil {
			zkLogger.Debug(LoggerTagOtelStore, "Unable to get trace for traceId ", traceId)
			continue
		}
		zkLogger.Debug(LoggerTagOtelStore, "data type of trace ", reflect.TypeOf(trace))
		mapOfSpansFromDB := trace.(map[string]string)
		for spanId, spanData := range mapOfSpansFromDB {
			var sp SpanFromOTel
			err = json.Unmarshal([]byte(spanData), &sp)
			traceFromOTel.spans[spanId] = &sp
		}

		// sanitize the parent span id
		for _, spanFromOTel := range traceFromOTel.spans {
			spanFromOTel.ParentSpanID = findParentSpan(traceFromOTel.spans, spanFromOTel)
		}

		//	remove the spans where kind is INTERNAL
		newSpans := map[string]*SpanFromOTel{}
		for spanId, spanFromOTel := range traceFromOTel.spans {
			if spanFromOTel.Kind != "INTERNAL" {
				newSpans[spanId] = spanFromOTel
			}
		}
		traceFromOTel.spans = newSpans

	}
	return traceMap, err
}

func findParentSpan(spans map[string]*SpanFromOTel, currentSpan *SpanFromOTel) string {
	parentSpanID := currentSpan.ParentSpanID
	if currentSpan.Kind == "INTERNAL" {
		newSpan, ok := spans[currentSpan.ParentSpanID]
		if ok {
			parentSpanID = findParentSpan(spans, newSpan)
		}
	}
	return parentSpanID
}
