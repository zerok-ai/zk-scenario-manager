package stores

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	otlpCommonV1 "go.opentelemetry.io/proto/otlp/common/v1"
	otlpTraceV1 "go.opentelemetry.io/proto/otlp/trace/v1"
	typedef "scenario-manager/internal"
)

type TWorkLoadIdToSpan map[typedef.TWorkloadId]*SpanFromOTel
type TWorkLoadIdToSpanArray map[typedef.TWorkloadId][]*SpanFromOTel

type OTelDataHandler struct {
	redisClient *redis.Client
}

func (t OTelDataHandler) initialize() *OTelDataHandler {
	return &t
}

func (t OTelDataHandler) Close() {
	err := t.redisClient.Close()
	if err != nil {
		return
	}
}

func GetOTelStore(redisConfig config.RedisConfig) *OTelDataHandler {
	dbName := clientDBNames.TraceDBName
	_redisClient := config.GetRedisConnection(dbName, redisConfig)
	return OTelDataHandler{redisClient: _redisClient}.initialize()
}

type OTelError struct {
	ErrorType     string `json:"error_type"`
	ExceptionType string `json:"exception_type"`
	Hash          string `json:"hash"`
	Message       string `json:"message"`
}

type SpanFromOTel struct {
	Span                   *otlpTraceV1.Span        `json:"span"`
	SpanAttributes         map[string]interface{}   `json:"span_attributes,omitempty"`
	SpanEvents             []map[string]interface{} `json:"span_events,omitempty"`
	ResourceAttributesHash string                   `json:"resource_attributes_hash,omitempty"`
	ScopeAttributesHash    string                   `json:"scope_attributes_hash,omitempty"`
	WorkloadIDList         []string                 `json:"workload_id_list"`
	GroupByMap             GroupByMap               `json:"group_by"`
	ScopeAttributes        []*otlpCommonV1.KeyValue `json:"scope_attributes,omitempty"`
	ResourceAttributes     []*otlpCommonV1.KeyValue `json:"resource_attributes,omitempty"`
	TraceID                typedef.TTraceid         `json:"trace_id"`
	IsRoot                 bool                     `json:"is_root"`
	GroupByTitleSet        ds.Set[string]           `json:"group_by_title_set"`
	SpanID                 typedef.TSpanId          `json:"span_id"`
	ParentSpanID           typedef.TSpanId          `json:"parent_span_id"`
}

type SpanAttributes interface {
	populateThroughAttributeMap()
}

type TraceFromOTel struct {
	// Spans is a map of spanID to span
	Spans      map[typedef.TSpanId]*SpanFromOTel
	RootSpanID typedef.TSpanId
}

// GetSpansForTracesFromDB retrieves the spans for the given traceIds from the database
// Returns a map of traceId to TraceFromOTel
// Returns a map of protocol to array of traces
func (t OTelDataHandler) GetSpansForTracesFromDB(keys []typedef.TTraceid) (result map[typedef.TTraceid]*TraceFromOTel, err error) {

	redisClient := t.redisClient

	// 1. Begin a transaction
	pipe := redisClient.TxPipeline()
	// 2. Retrieve the hashes within the transaction
	var hashResults []*redis.MapStringStringCmd
	for _, hashKey := range keys {
		hashResult := pipe.HGetAll(ctx, string(hashKey))
		hashResults = append(hashResults, hashResult)
	}
	// 3. Execute the transaction
	_, err = pipe.Exec(ctx)
	if err != nil {
		fmt.Println("Error executing transaction:", err)
		return nil, err
	}

	// 4. Process the results
	result = t.processResult(keys, hashResults)

	return result, nil
}

func (t OTelDataHandler) processResult(keys []typedef.TTraceid, hashResults []*redis.MapStringStringCmd) (result map[typedef.TTraceid]*TraceFromOTel) {

	result = make(map[typedef.TTraceid]*TraceFromOTel)
	for i, hashResult := range hashResults {
		traceId := keys[i]
		trace, err1 := hashResult.Result()

		if err1 != nil {
			zkLogger.Error(LoggerTag, "Error retrieving trace:", err1)
			continue
		}

		if len(trace) == 0 {
			zkLogger.DebugF(LoggerTag, "No trace data found for traceId: %s in OTel store", traceId)
			continue
		}

		traceFromOTel := &TraceFromOTel{Spans: map[typedef.TSpanId]*SpanFromOTel{}}

		// 4.1 Unmarshal the Spans
		for spanId, spanData := range trace {
			var sp SpanFromOTel
			err2 := json.Unmarshal([]byte(spanData), &sp)
			if err2 != nil {
				zkLogger.Error(LoggerTag, "Error retrieving span:", err2)
				continue
			}
			sp.TraceID = traceId
			sp.SpanID = typedef.TSpanId(spanId)
			sp.ParentSpanID = typedef.TSpanId(hex.EncodeToString(sp.Span.ParentSpanId))

			traceFromOTel.Spans[typedef.TSpanId(spanId)] = &sp
		}

		// 4.2 set the parent-child relationships and find root span
		var rootSpan *SpanFromOTel
		for _, spanFromOTel := range traceFromOTel.Spans {
			_, ok := traceFromOTel.Spans[spanFromOTel.ParentSpanID]
			if !ok {
				rootSpan = spanFromOTel
			}
		}

		if rootSpan == nil {
			zkLogger.Debug(LoggerTag, "rootSpanID not found for trace id ", traceId)
			continue
		}

		rootSpan.IsRoot = true
		traceFromOTel.RootSpanID = rootSpan.SpanID

		result[traceId] = traceFromOTel
	}

	return result
}

type GroupByValueItem struct {
	WorkloadId string `json:"workload_id"`
	Title      string `json:"title"`
	Hash       string `json:"hash"`
}

type GroupByValues []*GroupByValueItem
type ScenarioId string
type GroupByMap map[ScenarioId]GroupByValues
