package stores

import (
	"encoding/hex"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/utils"
	zkUtilsCommonModel "github.com/zerok-ai/zk-utils-go/common"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkUtilsOtel "github.com/zerok-ai/zk-utils-go/proto"
	zkUtilsEnrichedSpan "github.com/zerok-ai/zk-utils-go/proto/enrichedSpan"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	otlpCommonV1 "go.opentelemetry.io/proto/otlp/common/v1"
	otlpTraceV1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"net"
	"regexp"
	smUtils "scenario-manager/internal"
	typedef "scenario-manager/internal"
	otlpReceiverClient "scenario-manager/internal/client"
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
	Span                   *otlpTraceV1.Span               `json:"span"`
	SpanAttributes         zkUtilsCommonModel.GenericMap   `json:"span_attributes,omitempty"`
	SpanEvents             []zkUtilsCommonModel.GenericMap `json:"span_events,omitempty"`
	ResourceAttributesHash string                          `json:"resource_attributes_hash,omitempty"`
	ScopeAttributesHash    string                          `json:"scope_attributes_hash,omitempty"`
	WorkloadIDList         []string                        `json:"workload_id_list"`
	GroupByMap             zkUtilsCommonModel.GroupByMap   `json:"group_by"`
	ScopeAttributes        []*otlpCommonV1.KeyValue        `json:"scope_attributes,omitempty"`
	ResourceAttributes     []*otlpCommonV1.KeyValue        `json:"resource_attributes,omitempty"`
	TraceID                typedef.TTraceid                `json:"trace_id"`
	IsRoot                 bool                            `json:"is_root"`
	GroupByTitleSet        ds.Set[string]                  `json:"group_by_title_set"`
	SpanID                 typedef.TSpanId                 `json:"span_id"`
	ParentSpanID           typedef.TSpanId                 `json:"parent_span_id"`
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
		zkLogger.Error(LogTag, "Error executing transaction:", err)
		return nil, err
	}

	// 4. Process the results
	data, err := t.fetchSpanData(keys, hashResults)
	if err != nil {
		return nil, err
	}

	result = t.processResult(keys, data)

	return result, nil
}

func (t OTelDataHandler) fetchSpanData(keys []typedef.TTraceid, hashResults []*redis.MapStringStringCmd) (map[string]map[string]*zkUtilsOtel.OtelEnrichedRawSpanForProto, error) {
	// keys will have trace id's

	//hashResults will have the data for each trace id ie map[string]string traceId : map[spanId]NodeIpOfOtlpReceiver

	// map[string]string
	//map[ip][traces]
	//getting list of trace id's

	//create map[nodeId][]traceId+'-'+spanId

	//and make an api call to fetch all the data for each trace id in go routine

	// result will map of traceId+'-'+spanId to TraceFromOTel

	//and the convert above data to in below format

	// combine all the data again
	nodeIpMap := make(map[string][]string)
	otlpNodeIpEncounterGivenTraceMap := make(map[string]map[string]bool)

	for i, nodeIp := range hashResults {
		traceId := keys[i]
		traceSpanNodeIpMap, err1 := nodeIp.Result()

		if err1 != nil {
			zkLogger.Error(LoggerTag, "Error retrieving trace's SpanId NodeIp Map from redis DB3 result set", err1)
			continue
		}
		if len(traceSpanNodeIpMap) == 0 {
			zkLogger.DebugF(LoggerTag, "No trace data found for traceId: %s in OTel store", traceId)
			continue
		}
		for spanId, spanNodeIp := range traceSpanNodeIpMap {
			//validate if the string is an ip address
			if isValidNodeIP(spanNodeIp) == false {
				zkLogger.Error(LoggerTag, fmt.Sprintf("Error while creating nodeIp-traceList map traceId: %s, spanId: %s because Invalid Node IP: %s", traceId, spanId, spanNodeIp), err1)
				continue
			}
			//initialize the map for the given nodeIp if not present
			if otlpNodeIpEncounterGivenTraceMap[spanNodeIp] == nil || len(otlpNodeIpEncounterGivenTraceMap[spanNodeIp]) == 0 {
				otlpNodeIpEncounterGivenTraceMap[spanNodeIp] = make(map[string]bool)
			}
			//check if the nodeIp encountered for the given traceId
			if otlpNodeIpEncounterGivenTraceMap[spanNodeIp][string(traceId)] == true {
				continue
			}
			otlpNodeIpEncounterGivenTraceMap[spanNodeIp][string(traceId)] = true
			//traceSpanId := string(traceId) + "-" + spanId
			nodeIpMap[spanNodeIp] = append(nodeIpMap[spanNodeIp], string(traceId))
		}
	}

	//make an api call to fetch all the data for each trace id in go routine
	var otlpReceiverResultMap map[string]map[string]*zkUtilsOtel.OtelEnrichedRawSpanForProto
	otlpReceiverResultMap, err := t.getSpanData(nodeIpMap)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error retrieving data from OTLP receiver", err)
		return otlpReceiverResultMap, err
	}

	return otlpReceiverResultMap, nil
}

func (t OTelDataHandler) getSpanData(nodeIpTraceIdMap map[string][]string) (map[string]map[string]*zkUtilsOtel.OtelEnrichedRawSpanForProto, error) {

	otlpReceiverResultMap := make(map[string]map[string]*zkUtilsOtel.OtelEnrichedRawSpanForProto)

	for nodeIp, traceIdSpanIdList := range nodeIpTraceIdMap {
		traceDataFromOtlpReceiver, err := otlpReceiverClient.GetSpanData(nodeIp, traceIdSpanIdList, "8147") //TODO: get from config
		if err != nil {
			zkLogger.Error(LoggerTag, fmt.Sprintf("Error retrieving data from OTLP receiver with nodeIP: %s for traces : %s", nodeIp, traceIdSpanIdList), err)
			continue
		}

		//Iterate over the response list and create a map of traceId to spanId to spanData.
		for _, response := range traceDataFromOtlpReceiver.ResponseList {

			traceIdSpanId := response.Key
			spanData := response.Value
			//TODO: This separator might change if we decide to -o- as separator.
			traceId, spanId, err := smUtils.SplitTraceIdSpanId(traceIdSpanId, "-")

			if err != nil {
				zkLogger.Error(LoggerTag, fmt.Sprintf("Error splitting traceIdSpanId: %s", traceIdSpanId), err)
				continue
			}

			if len(otlpReceiverResultMap[traceId]) == 0 || otlpReceiverResultMap[traceId] == nil {
				otlpReceiverResultMap[traceId] = make(map[string]*zkUtilsOtel.OtelEnrichedRawSpanForProto)
				spanDataMap := make(map[string]*zkUtilsOtel.OtelEnrichedRawSpanForProto)
				spanDataMap[spanId] = spanData
				otlpReceiverResultMap[traceId] = spanDataMap
			} else {
				otlpReceiverResultMap[traceId][spanId] = spanData
			}
		}
		//Iterate over the ebpf response list and update the map of traceId to spanId to spanData with ebpf data.
		for _, response := range traceDataFromOtlpReceiver.EbpfResponseList {

			traceIdSpanId := response.Key
			ebpfData := response.Value
			traceId, spanId, err := smUtils.SplitTraceIdSpanId(traceIdSpanId, "-e-")

			if err != nil {
				zkLogger.Error(LoggerTag, fmt.Sprintf("Error splitting traceIdSpanId: %s", traceIdSpanId), err)
				continue
			}

			if otlpReceiverResultMap[traceId] != nil {
				spanData, ok := otlpReceiverResultMap[traceId][spanId]
				if ok {
					spanData.SpanAttributes.KeyValueList = append(spanData.SpanAttributes.KeyValueList, t.getEbpfAttributes(ebpfData)...)
				}
			}
		}
	}

	return otlpReceiverResultMap, nil
}

func (t OTelDataHandler) getEbpfAttributes(ebpfData *zkUtilsOtel.EbpfEntryDataForSpan) []*otlpCommonV1.KeyValue {
	keyValueList := []*otlpCommonV1.KeyValue{}
	if ebpfData != nil {

		requestBody := ebpfData.ReqBody
		keyValueList = append(keyValueList, t.getAttribute("zk_request_body", requestBody))

		responseBody := ebpfData.RespBody
		keyValueList = append(keyValueList, t.getAttribute("zk_response_body", responseBody))

		contentType := ebpfData.ContentType
		keyValueList = append(keyValueList, t.getAttribute("zk_content_type", contentType))

		reqPath := ebpfData.ReqPath
		keyValueList = append(keyValueList, t.getAttribute("zk_request_path", reqPath))

		reqMethod := ebpfData.ReqMethod
		keyValueList = append(keyValueList, t.getAttribute("zk_request_method", reqMethod))

		respStatus := ebpfData.RespStatus
		keyValueList = append(keyValueList, t.getAttribute("zk_response_status", respStatus))

		//TODO: Change the headers to keyvalue list after checking the string format.
		requestHeaders := ebpfData.ReqHeaders
		keyValueList = append(keyValueList, t.getAttribute("zk_request_headers", requestHeaders))

		responseHeaders := ebpfData.RespHeaders
		keyValueList = append(keyValueList, t.getAttribute("zk_response_headers", responseHeaders))

	}

	return keyValueList
}

func (t OTelDataHandler) getAttribute(key, value string) *otlpCommonV1.KeyValue {
	return &otlpCommonV1.KeyValue{
		Key:   key,
		Value: &otlpCommonV1.AnyValue{Value: &otlpCommonV1.AnyValue_StringValue{StringValue: value}},
	}
}

func (t OTelDataHandler) processResult(keys []typedef.TTraceid, traceSpanData map[string]map[string]*zkUtilsOtel.OtelEnrichedRawSpanForProto) (result map[typedef.TTraceid]*TraceFromOTel) {
	result = make(map[typedef.TTraceid]*TraceFromOTel)
	for i := range keys {
		traceId := keys[i]
		spanMap := traceSpanData[string(traceId)]

		if spanMap == nil {
			zkLogger.Error(LoggerTag, fmt.Sprintf("Error retrieving data from otlp receiver got null data for traceId : %s", traceId), nil)
			continue
		}

		if len(spanMap) == 0 {
			zkLogger.DebugF(LoggerTag, "No trace data found for traceId: %s in OTel store", traceId)
			continue
		}

		traceFromOTel := &TraceFromOTel{Spans: map[typedef.TSpanId]*SpanFromOTel{}}

		// 4.1 Unmarshal the Spans
		for spanId, protoSpan := range spanMap {
			var sp SpanFromOTel
			x := zkUtilsEnrichedSpan.GetEnrichedSpan(protoSpan)

			sp.Span = x.Span
			sp.SpanAttributes = x.SpanAttributes
			sp.SpanEvents = x.SpanEvents
			sp.ResourceAttributesHash = x.ResourceAttributesHash
			sp.ScopeAttributesHash = x.ScopeAttributesHash
			sp.WorkloadIDList = x.WorkloadIdList
			sp.GroupByMap = x.GroupBy
			sp.TraceID = traceId
			sp.SpanID = typedef.TSpanId(spanId)
			sp.ParentSpanID = typedef.TSpanId(hex.EncodeToString(sp.Span.ParentSpanId))

			traceFromOTel.Spans[typedef.TSpanId(spanId)] = &sp

			traceFromOTel.Spans[typedef.TSpanId(spanId)] = &sp
		}

		// 4.2 set the parent-child relationships and find root span
		var rootSpan *SpanFromOTel
		for _, spanFromOTel := range traceFromOTel.Spans {
			_, ok := traceFromOTel.Spans[spanFromOTel.ParentSpanID]
			if !ok && utils.IsEmpty(string(spanFromOTel.ParentSpanID)) {
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

func isValidIP(ip string) bool {
	parsedIP := net.ParseIP(ip)
	return parsedIP != nil
}

// isNodeIP checks if the input string is a valid Node IP (IPv4 or IPv6)
func isValidNodeIP(ip string) bool {
	// Use a regular expression to check if the string is in the form of a valid IP
	// This is just a basic example, you may need to adjust the regex based on your specific requirements
	ipRegex := regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$|^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$`)

	return ipRegex.MatchString(ip) && isValidIP(ip)
}
