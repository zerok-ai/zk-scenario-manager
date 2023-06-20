package dto

import (
	"encoding/json"
	zkCrypto "github.com/zerok-ai/zk-utils-go/crypto"
	"scenario-manager/internal/tracePersistence/model"
)

type IncidentDto struct {
	ScenarioId      string  `json:"scenario_id"`
	ScenarioVersion string  `json:"scenario_version"`
	TraceId         string  `json:"trace_id"`
	Title           string  `json:"title"`
	ScenarioType    string  `json:"scenario_type"`
	Velocity        float32 `json:"velocity"`
	TotalCount      int     `json:"total_count"`
	Source          string  `json:"source"`
	Destination     string  `json:"destination"`
	FirstSeen       string  `json:"first_seen"`
	LastSeen        string  `json:"last_seen"`
}

type TraceTableDto struct {
	ScenarioId      string `json:"scenario_id"`
	ScenarioVersion string `json:"scenario_version"`
	TraceId         string `json:"trace_id"`
	Title           string `json:"title"`
	ScenarioType    string `json:"scenario_type"`
	CreatedAt       string `json:"created_at"`
}

type TraceMetadataTableDto struct {
	TraceId      string  `json:"trace_id"`
	SpanId       string  `json:"span_id"`
	ParentSpanId string  `json:"parent_span_id"`
	Source       string  `json:"source"`
	Destination  string  `json:"destination"`
	Error        bool    `json:"error"`
	Metadata     string  `json:"metadata"`
	LatencyMs    float32 `json:"latency_ms"`
	Protocol     string  `json:"protocol"`
}

type TraceRawDataTableDto struct {
	TraceId         string `json:"trace_id"`
	SpanId          string `json:"span_id"`
	RequestPayload  []byte `json:"request_payload"`
	ResponsePayload []byte `json:"response_payload"`
}

func (t TraceTableDto) GetAllColumns() []any {
	return []any{t.ScenarioId, t.ScenarioVersion, t.TraceId}
}

func (t TraceMetadataTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.SpanId, t.ParentSpanId, t.Source, t.Destination, t.Error, t.Metadata, t.LatencyMs, t.Protocol}
}

func (t TraceRawDataTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.SpanId, t.RequestPayload, t.ResponsePayload}
}

func ConvertTraceToTraceDto(t model.Trace) (*TraceTableDto, *TraceMetadataTableDto, *TraceRawDataTableDto, *error) {
	var traceDto TraceTableDto
	var traceMetadataDto TraceMetadataTableDto
	var traceRawDataDto TraceRawDataTableDto

	requestCompressedStr, err := zkCrypto.CompressStringGzip(t.RequestPayload)
	if err != nil {
		return nil, nil, nil, &err
	}

	responseCompressedStr, err := zkCrypto.CompressStringGzip(t.ResponsePayload)
	if err != nil {
		return nil, nil, nil, &err
	}

	m, err := json.Marshal(t.Metadata)
	if err != nil {
		return nil, nil, nil, &err
	}

	traceDto.ScenarioId = t.ScenarioId
	traceDto.ScenarioVersion = t.ScenarioVersion
	traceDto.TraceId = t.TraceId

	traceMetadataDto.TraceId = t.TraceId
	traceMetadataDto.SpanId = t.SpanId
	traceMetadataDto.Source = t.Source
	traceMetadataDto.Destination = t.Destination
	traceMetadataDto.Error = *t.Error
	traceMetadataDto.Metadata = string(m)
	traceMetadataDto.LatencyMs = *t.LatencyMs
	traceMetadataDto.Protocol = t.Protocol

	traceRawDataDto.TraceId = t.TraceId
	traceRawDataDto.SpanId = t.SpanId
	traceRawDataDto.RequestPayload = requestCompressedStr
	traceRawDataDto.ResponsePayload = responseCompressedStr

	return &traceDto, &traceMetadataDto, &traceRawDataDto, nil
}
