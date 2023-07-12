package stores

import (
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"time"
)

type OTelStore struct {
	redisClient *redis.Client
}

func (t OTelStore) initialize() *OTelStore {
	return &t
}

func (t OTelStore) Close() {
	t.redisClient.Close()
}

func GetOTelStore(redisConfig *config.RedisConfig) *OTelStore {
	dbName := "otel"
	zkLogger.Debug(LoggerTag, "GetOTelStore: config=", redisConfig, "dbName=", dbName, "dbID=", redisConfig.DBs[dbName])
	readTimeout := time.Duration(redisConfig.ReadTimeout) * time.Second
	_redisClient := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprint(redisConfig.Host, ":", redisConfig.Port),
		Password:    "",
		DB:          redisConfig.DBs[dbName],
		ReadTimeout: readTimeout,
	})

	return OTelStore{redisClient: _redisClient}.initialize()
}

type SpanFromOTel struct {
	SpanID       string
	Kind         string `json:"spanKind"`
	Protocol     string `json:"protocol"`
	ParentSpanID string `json:"parentSpanID"`
	Children     []SpanFromOTel
}

type TraceFromOTel struct {
	// Spans is a map of spanID to span
	Spans map[string]*SpanFromOTel
}

// GetSpansForTracesFromDB retrieves the spans for the given traceIds from the database
// Returns a map of traceId to TraceFromOTel
// Returns a map of protocol to array of traces
func (t OTelStore) GetSpansForTracesFromDB(keys []string) (map[string]*TraceFromOTel, *map[string][]string, error) {

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
		return nil, nil, err
	}

	// 4. Process the results
	result := map[string]*TraceFromOTel{}
	tracesForProtocol := make(map[string]ds.Set, 0)
	for i, hashResult := range hashResults {
		traceId := keys[i]
		trace, err := hashResult.Result()

		if err != nil {
			zkLogger.Error(LoggerTag, "Error retrieving trace:", err)
			continue
		}

		if len(trace) == 0 {
			zkLogger.Debug(LoggerTag, "No trace found for traceId:", traceId)
			continue
		}

		traceFromOTel := &TraceFromOTel{Spans: map[string]*SpanFromOTel{}}

		// 4.1 Unmarshal the Spans
		for spanId, spanData := range trace {
			var sp SpanFromOTel
			err = json.Unmarshal([]byte(spanData), &sp)
			if err != nil {
				zkLogger.ErrorF(LoggerTag, "Error retrieving span:", err)
				continue
			}
			sp.SpanID = spanId
			traceFromOTel.Spans[spanId] = &sp
		}

		// 4.2 set the parent-child relationships and find root span
		var rootSpan *string
		for _, spanFromOTel := range traceFromOTel.Spans {
			parentSpan, ok := traceFromOTel.Spans[spanFromOTel.ParentSpanID]
			if ok {
				parentSpan.Children = append(parentSpan.Children, *spanFromOTel)
			} else {
				rootSpan = &spanFromOTel.SpanID
			}
		}

		if rootSpan == nil {
			zkLogger.Debug(LoggerTag, "rootSpan not found")
			continue
		}

		// 4.3 prune the unwanted Spans
		prune(traceFromOTel.Spans, *rootSpan)

		// 4.4 find the protocol for each span
		for _, value := range traceFromOTel.Spans {
			protocol := value.Protocol
			traceSetForProtocol, ok := tracesForProtocol[protocol]
			if !ok {
				traceSetForProtocol = make(ds.Set, 0)
			}
			traceSetForProtocol[traceId] = true
			tracesForProtocol[protocol] = traceSetForProtocol
		}

		zkLogger.DebugF(LoggerTag, "rootSpan: %s", *rootSpan)
		result[traceId] = traceFromOTel
	}

	// 5. Convert the tracesForProtocol map to a map of protocol to array of traces
	traceArrayForProtocol := make(map[string][]string, 0)
	for protocol, traceSet := range tracesForProtocol {
		traceArrayForProtocol[protocol] = traceSet.GetAll()
	}

	return result, &traceArrayForProtocol, nil
}

// prune removes the Spans that are not required - internal Spans and server Spans that are not the root span
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
