package stores

import (
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"net/url"
	typedef "scenario-manager/internal"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"strconv"
	"strings"
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
	_redisClient := config.GetRedisConnection(dbName, *redisConfig)
	return OTelStore{redisClient: _redisClient}.initialize()
}

type SpanFromOTel struct {
	TraceID      typedef.TTraceid
	SpanID       typedef.TSpanId
	ParentSpanID typedef.TSpanId `json:"parent_span_id"`
	Kind         string          `json:"span_kind"`

	StartTime uint64 `json:"start_ns"`
	EndTime   uint64 `json:"end_ns"`

	SourceIP string `json:"source_ip"`
	DestIP   string `json:"dest_ip"`

	ServiceName string `json:"service_name"`

	Attributes         map[string]interface{} `json:"attributes"`
	SpanForPersistence *tracePersistenceModel.Span
	Children           []SpanFromOTel
}

func (spanFromOTel *SpanFromOTel) GetStringAttribute(attr string) (string, bool) {
	var protocol interface{}
	success := false
	var stringValue string
	if protocol, success = spanFromOTel.Attributes[attr]; success {
		switch v := protocol.(type) {
		case string:
			stringValue = v
		case float64:
			stringValue = strconv.FormatFloat(v, 'f', -1, 64)
		case int:
			stringValue = strconv.Itoa(v)
		default:
			stringValue = "Unknown Type"
			success = false
			zkLogger.Error(LoggerTag, "getStringAttribute: Unknown Type for ", attr, " value=", protocol)
		}
	}
	return stringValue, success
}

func (spanFromOTel *SpanFromOTel) getNumberAttribute(attr string) (string, bool) {
	str := ""
	success := false
	if protocol, ok := spanFromOTel.Attributes[attr]; ok {
		str, success = protocol.(string)
	}
	return str, success
}

func (spanFromOTel *SpanFromOTel) createAndPopulateSpanForPersistence() {

	spanFromOTel.SpanForPersistence = &tracePersistenceModel.Span{
		TraceID:       string(spanFromOTel.TraceID),
		SpanID:        string(spanFromOTel.SpanID),
		Kind:          spanFromOTel.Kind,
		ParentSpanID:  string(spanFromOTel.ParentSpanID),
		StartTime:     EpochMilliSecondsToTime(spanFromOTel.StartTime),
		Latency:       latencyInMilliSeconds(spanFromOTel.StartTime, spanFromOTel.EndTime),
		SourceIP:      spanFromOTel.SourceIP,
		DestinationIP: spanFromOTel.DestIP,
		ServiceName:   spanFromOTel.ServiceName,
	}

	// set protocol
	if protocol, ok := spanFromOTel.GetStringAttribute(OTelAttrProtocol); ok {
		spanFromOTel.SpanForPersistence.Protocol = protocol
	}

	if !spanFromOTel.populateThroughDBAttributeMap() {
		spanFromOTel.populateThroughHttpAttributeMap()
	}
}

func (spanFromOTel *SpanFromOTel) populateThroughHttpAttributeMap() {
	spanForPersistence := spanFromOTel.SpanForPersistence
	// set protocol for exception
	if httpMethod, methodExists := spanFromOTel.Attributes[OTelAttrHttpMethod]; methodExists {
		spanForPersistence.Method = httpMethod.(string)
		if urlAttribute, urlExists := spanFromOTel.Attributes[OTelAttrHttpUrl]; urlExists {
			urlString := urlAttribute.(string)
			parsedUrl, parseErr := url.Parse(urlString)
			if parseErr != nil {
				zkLogger.Error(LoggerTag, "populateThroughHttpAttributeMap: parseErr=", parseErr, " url=", urlString)
			} else {
				host := parsedUrl.Host
				path := parsedUrl.Path
				if strings.Contains(host, ZkOperatorServiceName) && strings.Contains(path, OTelExceptionUrl) && httpMethod == HTTPPost {
					spanForPersistence.Protocol = PException
				}
			}
		}
	}

	if status, ok := spanFromOTel.GetStringAttribute(OTelAttrHttpStatus); ok {
		s, err := strconv.Atoi(status)
		if err != nil {
			zkLogger.Error(LoggerTag, "populateThroughHttpAttributeMap: status=", status, " err=", err)
		}
		spanForPersistence.Status = s
	}

	//	set route
	if spanFromOTel.Kind == SERVER {
		if route, ok := spanFromOTel.GetStringAttribute(OTelHttpAttrRoute); ok {
			spanForPersistence.Route = route
		}

		if scheme, ok := spanFromOTel.GetStringAttribute(OTelHttpAttrScheme); ok {
			spanForPersistence.Scheme = scheme
		}

		if query, ok := spanFromOTel.GetStringAttribute(OTelHttpAttrQuery); ok {
			spanForPersistence.Query = query
		}

		// path
		if path, ok := spanFromOTel.GetStringAttribute(OTelAttrHttpTarget); ok {
			spanForPersistence.Path = path
		}

	} else if spanFromOTel.Kind == CLIENT {
		if netPeerName, ok := spanFromOTel.GetStringAttribute(OTelHttpAttrNetPeerName); ok {
			spanForPersistence.Route = netPeerName
		}

		// path
		if path, ok := spanFromOTel.GetStringAttribute(OTelAttrHttpUrl); ok {
			spanForPersistence.Path = path
		}
	}
}

func (spanFromOTel *SpanFromOTel) populateThroughDBAttributeMap() bool {

	spanForPersistence := spanFromOTel.SpanForPersistence
	if db, exists := spanFromOTel.GetStringAttribute(OTelAttrDBSystem); exists {
		spanForPersistence.Protocol = db
		spanForPersistence.Scheme = db
	} else {
		return false
	}

	// set route
	spanForPersistence.Route = ""
	if dbName, ok := spanFromOTel.GetStringAttribute(OTelDBAttrDBName); ok {
		spanForPersistence.Route += dbName
	}
	if dbTableName, ok := spanFromOTel.GetStringAttribute(OTelDBAttrDBSqlTable); ok {
		spanForPersistence.Route += dbTableName
	}

	// path
	if path, ok := spanFromOTel.GetStringAttribute(OTelDBAttrConnectionString); ok {
		spanForPersistence.Path = path
	}

	// query
	if query, ok := spanFromOTel.GetStringAttribute(OTelDBStatement); ok {
		spanForPersistence.Query = query
	}

	//username
	if userName, ok := spanFromOTel.GetStringAttribute(OTelDBAttrUserName); ok {
		spanForPersistence.Username = userName
	}

	//method
	if userName, ok := spanFromOTel.GetStringAttribute(OTelDBAttrOperation); ok {
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
				zkLogger.Error(LoggerTag, "Error retrieving span:", err2)
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

		rootSpan.SpanForPersistence.IsRoot = true
		traceFromOTel.RootSpanID = rootSpan.SpanID

		result[traceId] = traceFromOTel
	}

	return result, nil
}
