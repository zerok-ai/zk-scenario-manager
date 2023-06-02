package model

import (
	"encoding/json"
	zkCrypto "github.com/zerok-ai/zk-utils-go/crypto"
)

type Trace struct {
	ScenarioId      string                 `json:"scenario_id"`
	ScenarioVersion string                 `json:"scenario_version"`
	TraceId         string                 `json:"trace_id"`
	SpanId          string                 `json:"span_id"`
	MetaData        map[string]interface{} `json:"meta_data"`
	Protocol        string                 `json:"protocol"`
	RequestPayload  string                 `json:"request_payload"`
	ResponsePayload string                 `json:"response_payload"`
}

type TraceDto struct {
	ScenarioId      string                 `json:"scenario_id"`
	ScenarioVersion string                 `json:"scenario_version"`
	TraceId         string                 `json:"trace_id"`
	SpanId          string                 `json:"span_id"`
	MetaData        map[string]interface{} `json:"meta_data"`
	Protocol        string                 `json:"protocol"`
	RequestPayload  []byte                 `json:"request_payload"`
	ResponsePayload []byte                 `json:"response_payload"`
}

type TraceTable struct {
	ScenarioId      string `json:"scenario_id"`
	ScenarioVersion string `json:"scenario_version"`
	TraceId         string `json:"trace_id"`
}

type TraceMetaDataTable struct {
	TraceId  string `json:"trace_id"`
	SpanId   string `json:"span_id"`
	MetaData string `json:"meta_data"`
	Protocol string `json:"protocol"`
}

type TraceRawDataTable struct {
	TraceId         string `json:"trace_id"`
	SpanId          string `json:"span_id"`
	RequestPayload  []byte `json:"request_payload"`
	ResponsePayload []byte `json:"response_payload"`
}

type TraceRawDataResponseObject struct {
	TraceId         string `json:"trace_id"`
	SpanId          string `json:"span_id"`
	RequestPayload  string `json:"request_payload"`
	ResponsePayload string `json:"response_payload"`
}

func (t TraceTable) GetArgs() []any {
	return []any{t.ScenarioId, t.ScenarioVersion, t.TraceId}
}

func (t TraceMetaDataTable) GetArgs() []any {
	return []any{t.TraceId, t.SpanId, t.MetaData, t.Protocol}
}

func (t TraceRawDataTable) GetArgs() []any {
	return []any{t.TraceId, t.SpanId, t.RequestPayload, t.ResponsePayload}
}

func GetTraceTableData(t TraceDto) TraceTable {
	return TraceTable{
		ScenarioId:      t.ScenarioId,
		ScenarioVersion: t.ScenarioVersion,
		TraceId:         t.TraceId,
	}
}

func GetTraceTableMetaData(t TraceDto) TraceMetaDataTable {
	s, _ := json.Marshal(t.MetaData)
	return TraceMetaDataTable{
		TraceId:  t.TraceId,
		SpanId:   t.SpanId,
		MetaData: string(s),
		Protocol: t.Protocol,
	}
}

func GetTraceTableRawData(t TraceDto) TraceRawDataTable {
	return TraceRawDataTable{
		TraceId:         t.TraceId,
		SpanId:          t.SpanId,
		RequestPayload:  t.RequestPayload,
		ResponsePayload: t.ResponsePayload,
	}
}

func ConvertTraceToTraceDto(t Trace) (*TraceDto, *error) {
	var traceDto TraceDto

	reqCompressedStr, err := zkCrypto.CompressString(t.RequestPayload)
	if err != nil {
		return nil, &err
	}

	resCompressedStr, err := zkCrypto.CompressString(t.ResponsePayload)
	if err != nil {
		return nil, &err
	}

	traceDto.ScenarioId = t.ScenarioId
	traceDto.ScenarioVersion = t.ScenarioVersion
	traceDto.TraceId = t.TraceId
	traceDto.SpanId = t.SpanId
	traceDto.Protocol = t.Protocol
	traceDto.MetaData = t.MetaData
	traceDto.RequestPayload = reqCompressedStr
	traceDto.ResponsePayload = resCompressedStr

	return &traceDto, nil
}

func ConvertTraceRawDataToTraceRawDataResponse(t TraceRawDataTable) (*TraceRawDataResponseObject, *error) {
	var resp TraceRawDataResponseObject
	reqDeompressedStr, err := zkCrypto.DecompressString(t.RequestPayload)
	if err != nil {
		return nil, &err
	}

	resDecompressedStr, err := zkCrypto.DecompressString(t.ResponsePayload)
	if err != nil {
		return nil, &err
	}

	resp.TraceId = t.TraceId
	resp.SpanId = t.SpanId
	resp.RequestPayload = reqDeompressedStr
	resp.ResponsePayload = resDecompressedStr

	return &resp, nil
}
