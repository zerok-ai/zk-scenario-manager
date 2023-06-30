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
		var rootSpan *SpanFromOTel
		for _, spanFromOTel := range traceFromOTel.spans {
			parentSpan, ok := traceFromOTel.spans[spanFromOTel.ParentSpanID]
			if ok {
				parentSpan.Children = append(parentSpan.Children, *spanFromOTel)
			} else {
				rootSpan = spanFromOTel
			}
			//zkLogger.DebugF(LoggerTagOtelStore, "SpanId: %s , ParentSpanId: %s, child count %d", spanFromOTel.SpanID, spanFromOTel.ParentSpanID, len(spanFromOTel.Children))
			zkLogger.DebugF(LoggerTagOtelStore, "")
		}

		if rootSpan == nil {
			zkLogger.Debug(LoggerTagOtelStore, "rootSpan not found")
			continue
		}

		// 4.3 prune the unwanted spans
		pr, _ := prune(traceFromOTel.spans, *rootSpan)
		rootSpan = &pr[0]

		zkLogger.DebugF(LoggerTagOtelStore, "rootSpan: %s", rootSpan)

		//spansForResult := &TraceFromOTel{spans: map[string]*SpanFromOTel{}}

		result[traceId] = traceFromOTel
	}
	return result, nil
}

// prune removes the spans that are not required - internal spans and server spans that are not the root span
func prune(spans map[string]*SpanFromOTel, currentSpan SpanFromOTel) ([]SpanFromOTel, bool) {
	currentSpan = *spans[currentSpan.SpanID]

	newChildArray := make([]SpanFromOTel, 0)
	// call prune on the children
	for _, child := range currentSpan.Children {
		newChildren, pruned := prune(spans, child)
		if pruned {
			delete(spans, child.SpanID)
			for i, _ := range newChildren {
				// Create a pointer to the current person
				ptr := &newChildren[i]

				// Set the parentSpanID of the child to the current span
				ptr.ParentSpanID = currentSpan.SpanID
				spans[ptr.SpanID].ParentSpanID = currentSpan.SpanID
			}
		}
		newChildArray = append(newChildArray, newChildren...)
	}
	currentSpan.Children = newChildArray

	parentSpan, isParentSpanPresent := spans[currentSpan.ParentSpanID]
	if currentSpan.Kind == INTERNAL || (currentSpan.Kind == SERVER && isParentSpanPresent && parentSpan.Kind == CLIENT) {
		return currentSpan.Children, true
	}

	return []SpanFromOTel{currentSpan}, false
}
