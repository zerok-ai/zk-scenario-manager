package stores

import (
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	typedef "scenario-manager/internal"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
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
	TraceID      typedef.TTraceid
	SpanID       typedef.TSpanId
	ParentSpanID typedef.TSpanId `json:"parentSpanID"`
	Kind         string          `json:"spanKind"`

	StartTime uint64 `json:"start_ns"`
	EndTime   uint64 `json:"end_ns"`

	attributes         map[string]string
	SpanForPersistence *tracePersistenceModel.Span
	Children           []SpanFromOTel
}

func (spanFromOTel *SpanFromOTel) createAndPopulateSpanForPersistence() {

	spanFromOTel.SpanForPersistence = &tracePersistenceModel.Span{
		TraceID:      string(spanFromOTel.TraceID),
		SpanID:       string(spanFromOTel.SpanID),
		Kind:         spanFromOTel.Kind,
		ParentSpanID: string(spanFromOTel.ParentSpanID),
		StartTime:    epochMilliSecondsToTime(spanFromOTel.StartTime),
		Latency:      latencyInMilliSeconds(spanFromOTel.StartTime, spanFromOTel.EndTime),
	}

	// set protocol
	if protocol, ok := spanFromOTel.attributes[OTelAttrProtocol]; ok {
		spanFromOTel.SpanForPersistence.Protocol = protocol
	}

	if !spanFromOTel.populateThroughDBAttributeMap() {
		spanFromOTel.populateThroughHttpAttributeMap()
	}
}

func (spanFromOTel *SpanFromOTel) populateThroughHttpAttributeMap() {

	spanForPersistence := spanFromOTel.SpanForPersistence
	// set protocol for exception
	if httpMethod, methodExists := spanFromOTel.attributes[OTelAttrHttpMethod]; methodExists {
		if url, urlExists := spanFromOTel.attributes[OTelAttrHttpUrl]; urlExists {
			if url == OTelExceptionUrl && httpMethod == HTTPPost {
				spanForPersistence.Protocol = PException
			}
		}
	}

	//	set route
	if spanFromOTel.Kind == SERVER {
		if route, ok := spanFromOTel.attributes[OTelHttpAttrRoute]; ok {
			spanForPersistence.Route = route
		}

		if scheme, ok := spanFromOTel.attributes[OTelHttpAttrScheme]; ok {
			spanForPersistence.Scheme = scheme
		}

		if route, ok := spanFromOTel.attributes[OTelHttpAttrQuery]; ok {
			spanForPersistence.Query = route
		}

	} else if spanFromOTel.Kind == CLIENT {
		if route, ok := spanFromOTel.attributes[OTelHttpAttrServerAddress]; ok {
			spanForPersistence.Route += route
		}
		if route, ok := spanFromOTel.attributes[OTelHttpAttrServerPort]; ok {
			spanForPersistence.Route += ":" + route
		}
	}
}

func (spanFromOTel *SpanFromOTel) populateThroughDBAttributeMap() bool {

	spanForPersistence := spanFromOTel.SpanForPersistence
	if db, exists := spanFromOTel.attributes[OTelAttrDBSystem]; exists {
		spanForPersistence.Protocol = db
		spanForPersistence.Scheme = db
	} else {
		return false
	}

	// set route
	spanForPersistence.Route = ""
	if dbName, ok := spanFromOTel.attributes[OTelDBAttrDBName]; ok {
		spanForPersistence.Route += dbName
	}
	if dbTableName, ok := spanFromOTel.attributes[OTelDBAttrDBSqlTable]; ok {
		spanForPersistence.Route += dbTableName
	}

	// path
	if path, ok := spanFromOTel.attributes[OTelDBAttrConnectionString]; ok {
		spanForPersistence.Path = path
	}

	// query
	if query, ok := spanFromOTel.attributes[OTelDBStatement]; ok {
		spanForPersistence.Query = query
	}

	//username
	if userName, ok := spanFromOTel.attributes[OTelDBAttrUserName]; ok {
		spanForPersistence.Username = userName
	}

	return true
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
func (t OTelStore) GetSpansForTracesFromDB(keys []typedef.TTraceid) (map[typedef.TTraceid]*TraceFromOTel, error) {

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
	_, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Println("Error executing transaction:", err)
		return nil, err
	}

	// 4. Process the results
	result := map[typedef.TTraceid]*TraceFromOTel{}
	//tracesForProtocol := make(map[string]ds.Set[string], 0)
	for i, hashResult := range hashResults {
		traceId := keys[i]
		trace, err1 := hashResult.Result()

		if err1 != nil {
			zkLogger.Error(LoggerTag, "Error retrieving trace:", err)
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
				zkLogger.ErrorF(LoggerTag, "Error retrieving span:", err)
				continue
			}
			sp.TraceID = traceId
			sp.SpanID = typedef.TSpanId(spanId)
			sp.createAndPopulateSpanForPersistence()

			traceFromOTel.Spans[typedef.TSpanId(spanId)] = &sp
		}

		// 4.2 set the parent-child relationships and find root span
		var rootSpan *SpanFromOTel
		rootFound := false
		for _, spanFromOTel := range traceFromOTel.Spans {
			parentSpan, ok := traceFromOTel.Spans[spanFromOTel.ParentSpanID]
			if ok {
				parentSpan.Children = append(parentSpan.Children, *spanFromOTel)
			} else {
				if rootFound {
					rootSpan = nil
					break
				}
				rootSpan = spanFromOTel
				rootFound = true
			}
		}

		if rootSpan == nil {
			zkLogger.Debug(LoggerTag, "rootSpanID not found for trace id ", traceId)
			continue
		}
		//else if rootSpan.Kind == SERVER {
		//	rootClient := SpanFromOTel{
		//		TraceID:  rootSpan.TraceID,
		//		SpanID:   rootSpan.ParentSpanID,
		//		Kind:     CLIENT,
		//		Protocol: rootSpan.Protocol,
		//		Children: []SpanFromOTel{*rootSpan},
		//	}
		//	traceFromOTel.Spans[rootSpan.ParentSpanID] = &rootClient
		//	rootSpan = &rootClient
		//}
		rootSpan.SpanForPersistence.IsRoot = true
		traceFromOTel.RootSpanID = rootSpan.SpanID

		result[traceId] = traceFromOTel
	}

	return result, nil
}
