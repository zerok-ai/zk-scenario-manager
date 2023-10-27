package stores

import (
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	typedef "scenario-manager/internal"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
)

type TWorkLoadIdToSpan map[typedef.TWorkloadId]*tracePersistenceModel.Span
type TWorkLoadIdToSpanArray map[typedef.TWorkloadId][]*tracePersistenceModel.Span

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
	zkLogger.Debug(LoggerTag, "GetOTelStore: config=", redisConfig, "dbName=", dbName, "dbID=", redisConfig.DBs[dbName])
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
	TraceID      typedef.TTraceid
	SpanID       typedef.TSpanId
	ParentSpanID typedef.TSpanId `json:"parent_span_id"`

	SpanName   string `json:"span_name"`
	Kind       string `json:"span_kind"`
	Method     string `json:"method"`
	OTelSchema string `json:"schema_version"`

	StartTimeNS uint64 `json:"start_ns"`
	LatencyNS   uint64 `json:"latency_ns"`

	ServiceName string `json:"service_name"`
	Source      string `json:"source"`
	SourceIP    string `json:"source_ip"`
	Destination string `json:"destination"`
	DestIP      string `json:"destination_ip"`

	Errors   []OTelError          `json:"errors"`
	Protocol typedef.ProtocolType `json:"protocol"`

	GroupByMap tracePersistenceModel.GroupByMap `json:"group_by"`

	WorkloadIDList []string `json:"workload_id_list"`

	// attributes
	//SpanAttributes     typedef.GenericMap `json:"span_attributes"`
	//ResourceAttributes typedef.GenericMap `json:"resource_attributes"`
	//ScopeAttributes    typedef.GenericMap `json:"scope_attributes"`

	SpanForPersistence *tracePersistenceModel.Span
	Children           []SpanFromOTel

	// Protocol properties.
	Route    string   `json:"route"`
	Scheme   string   `json:"scheme"`
	Path     string   `json:"path"`
	Query    string   `json:"query"`
	Status   *float64 `json:"status"`
	Username string   `json:"username"`
}

func (spanFromOTel *SpanFromOTel) createAndPopulateSpanForPersistence() {

	spanFromOTel.SpanForPersistence = &tracePersistenceModel.Span{
		TraceID:           string(spanFromOTel.TraceID),
		SpanID:            string(spanFromOTel.SpanID),
		SpanName:          spanFromOTel.SpanName,
		OTelSchemaVersion: spanFromOTel.OTelSchema,
		Kind:              spanFromOTel.Kind,
		ParentSpanID:      string(spanFromOTel.ParentSpanID),
		StartTime:         EpochNanoSecondsToTime(spanFromOTel.StartTimeNS),
		Latency:           spanFromOTel.LatencyNS,
		WorkloadIDList:    spanFromOTel.WorkloadIDList,
		GroupByMap:        spanFromOTel.GroupByMap,
		Method:            spanFromOTel.Method,

		ServiceName:   spanFromOTel.ServiceName,
		Source:        spanFromOTel.Source,
		SourceIP:      spanFromOTel.SourceIP,
		Destination:   spanFromOTel.Destination,
		DestinationIP: spanFromOTel.DestIP,
		Protocol:      spanFromOTel.Protocol,

		Status:   spanFromOTel.Status,
		Route:    spanFromOTel.Route,
		Scheme:   spanFromOTel.Scheme,
		Query:    spanFromOTel.Query,
		Path:     spanFromOTel.Path,
		Username: spanFromOTel.Username,
	}
}

func (spanFromOTel *SpanFromOTel) populateErrorAttributeMap() []string {

	errorIds := make([]string, 0)

	spanForPersistence := spanFromOTel.SpanForPersistence
	// set protocol for exception
	if spanFromOTel.Errors != nil && len(spanFromOTel.Errors) > 0 {

		for _, oTelError := range spanFromOTel.Errors {
			errorIds = append(errorIds, oTelError.Hash)
		}

		bytesOTelErrors, err := json.Marshal(spanFromOTel.Errors)
		if err != nil {
			zkLogger.Error(LoggerTag, "populateErrorAttributeMap: err=", err)
			return errorIds
		}
		spanForPersistence.Errors = string(bytesOTelErrors)
	}
	return errorIds
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
func (t OTelDataHandler) GetSpansForTracesFromDB(keys []typedef.TTraceid) (result map[typedef.TTraceid]*TraceFromOTel, oTelErrors []string, err error) {

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
		return nil, nil, err
	}

	// 4. Process the results
	result, oTelErrors = t.processResult(keys, hashResults)

	return result, oTelErrors, nil
}

func (t OTelDataHandler) processResult(keys []typedef.TTraceid, hashResults []*redis.MapStringStringCmd) (result map[typedef.TTraceid]*TraceFromOTel, errors []string) {

	result = make(map[typedef.TTraceid]*TraceFromOTel)
	errors = make([]string, 0)

	for i, hashResult := range hashResults {
		traceId := keys[i]
		trace, err1 := hashResult.Result()

		if err1 != nil {
			zkLogger.Error(LoggerTag, "Error retrieving trace:", err1)
			continue
		}

		if len(trace) == 0 {
			zkLogger.DebugF(LoggerTag, "No trace found for traceId: %s in OTel store", traceId)
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
			sp.createAndPopulateSpanForPersistence()

			// handle exceptions
			errors = append(errors, sp.populateErrorAttributeMap()...)

			traceFromOTel.Spans[typedef.TSpanId(spanId)] = &sp
		}

		// 4.2 set the parent-child relationships and find root span
		var rootSpan *SpanFromOTel
		for _, spanFromOTel := range traceFromOTel.Spans {
			parentSpan, ok := traceFromOTel.Spans[spanFromOTel.ParentSpanID]
			if ok {
				parentSpan.Children = append(parentSpan.Children, *spanFromOTel)
			} else {
				rootSpan = spanFromOTel
			}
		}

		if rootSpan == nil {
			zkLogger.Debug(LoggerTag, "rootSpanID not found for trace id ", traceId)
			continue
		}

		rootSpan.SpanForPersistence.IsRoot = true
		traceFromOTel.RootSpanID = rootSpan.SpanID

		result[traceId] = traceFromOTel
	}

	return result, errors
}
