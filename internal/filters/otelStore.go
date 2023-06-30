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

func (t OtelStore) getDataFromDB(keys []string) ([]*redis.MapStringStringCmd, error) {

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
	return hashResults, nil
}

func (t OtelStore) GetTracesFromDBWithNonInternalSpans(keys []string) (map[string]*TraceFromOTel, error) {

	keys = []string{"aaaaaaaa6f1df46b570bf18198000220", "aaaaaaaa92dc2395219c584980000233", "aaaaaaaa6d666cb02f4f51b681000226"}
	zkLogger.Debug(LoggerTagOtelStore, "GetTracesFromDBWithNonInternalSpans: key count =", len(keys))
	//hashResults, err := t.getDataFromDB(keys)

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
	hashTransactionResults, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Println("Error executing transaction:", err)
		return nil, err
	}

	zkLogger.DebugF(LoggerTagOtelStore, "hashTransactionResults=%d", len(hashTransactionResults))

	if err != nil {
		return nil, err
	}

	// Process the results
	traceMap := make(map[string]*TraceFromOTel)
	for i, hashResult := range hashResults {

		traceId := keys[i]
		trace, err := hashResult.Result()
		if err != nil {
			zkLogger.Error(LoggerTagOtelStore, "Error retrieving values for traceId %s: %v\n", traceId, err)
			continue
		}

		// create a container for spans
		traceFromOTel := traceMap[traceId]
		if traceFromOTel == nil {
			traceFromOTel = &TraceFromOTel{spans: map[string]*SpanFromOTel{}}
			traceMap[traceId] = traceFromOTel
		}
		for spanId, spanData := range trace {
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
	zkLogger.DebugF(LoggerTagOtelStore, "Expected traces: %d  Spans from otel=%d", len(keys), len(traceMap))
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
