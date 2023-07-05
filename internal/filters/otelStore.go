package filters

import (
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"time"
)

const LoggerTagOtelStore = "otelStore"

type OtelStore struct {
	redisClient *redis.Client
}

const (
	INTERNAL = "INTERNAL"
	DELETE   = "DELETE"
	CLIENT   = "CLIENT"
	SERVER   = "SERVER"
)

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
	SpanID       string
	Kind         string `json:"spanKind"`
	ParentSpanID string `json:"parentSpanID"`
	Children     []SpanFromOTel
}

type TraceFromOTel struct {
	// spans is a map of spanID to span
	spans map[string]*SpanFromOTel
}

func (t OtelStore) GetSpansForTracesFromDB(keys []string) (map[string]*TraceFromOTel, error) {

	redisClient := t.redisClient

	// 1. Begin a transaction
	pipe := redisClient.TxPipeline()
	// 2. Retrieve the hashes within the transaction
	var hashResults []*redis.MapStringStringCmd
	for _, hashKey := range keys {
		hashResult := pipe.HGetAll(ctx, hashKey)
		hashResults = append(hashResults, hashResult)
	}
	// 3. Execute the transaction
	_, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Println("Error executing transaction:", err)
		return nil, err
	}

	// 4. Process the results
	result := map[string]*TraceFromOTel{}
	for i, hashResult := range hashResults {
		traceId := keys[i]
		trace, err := hashResult.Result()

		if err != nil {
			fmt.Println("Error retrieving trace:", err)
			continue
		}

		traceFromOTel := &TraceFromOTel{spans: map[string]*SpanFromOTel{}}

		// 4.1 Unmarshal the spans
		for spanId, spanData := range trace {
			var sp SpanFromOTel
			err = json.Unmarshal([]byte(spanData), &sp)
			if err != nil {
				zkLogger.ErrorF(LoggerTagOtelStore, "Error retrieving span:", err)
				continue
			}
			sp.SpanID = spanId
			traceFromOTel.spans[spanId] = &sp
		}

		// 4.2 set the parent-child relationships and find root span
		var rootSpan *string
		for _, spanFromOTel := range traceFromOTel.spans {
			parentSpan, ok := traceFromOTel.spans[spanFromOTel.ParentSpanID]
			if ok {
				parentSpan.Children = append(parentSpan.Children, *spanFromOTel)
			} else {
				rootSpan = &spanFromOTel.SpanID
			}
		}

		if rootSpan == nil {
			zkLogger.Debug(LoggerTagOtelStore, "rootSpan not found")
			continue
		}

		// 4.3 prune the unwanted spans
		prune(traceFromOTel.spans, *rootSpan)

		zkLogger.DebugF(LoggerTagOtelStore, "rootSpan: %s", rootSpan)
		result[traceId] = traceFromOTel
	}
	return result, nil
}

// prune removes the spans that are not required - internal spans and server spans that are not the root span
func prune(spans map[string]*SpanFromOTel, currentSpanID string) ([]string, bool) {
	currentSpan := spans[currentSpanID]

	// call prune on the children
	newChildSpansArray := make([]SpanFromOTel, 0)
	newChildIdsArray := make([]string, 0)
	for _, child := range currentSpan.Children {
		newChildIds, pruned := prune(spans, child.SpanID)
		if pruned {
			delete(spans, child.SpanID)
		}
		for _, spId := range newChildIds {

			span := spans[spId]
			span.ParentSpanID = currentSpan.SpanID

			// update the span in the map
			spans[span.SpanID] = span

			newChildSpansArray = append(newChildSpansArray, *span)
		}

		newChildIdsArray = append(newChildIdsArray, newChildIds...)
	}
	currentSpan.Children = newChildSpansArray
	spans[currentSpanID] = currentSpan

	parentSpan, isParentSpanPresent := spans[currentSpan.ParentSpanID]
	if currentSpan.Kind == INTERNAL || (currentSpan.Kind == SERVER && isParentSpanPresent && parentSpan.Kind == CLIENT) {
		return newChildIdsArray, true
	}

	return []string{currentSpanID}, false
}
