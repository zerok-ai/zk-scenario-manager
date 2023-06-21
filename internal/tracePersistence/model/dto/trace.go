package dto

import (
	"encoding/json"
	"github.com/zerok-ai/zk-utils-go/common"
	zkCrypto "github.com/zerok-ai/zk-utils-go/crypto"
	"github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/zkerrors"
	"scenario-manager/internal/tracePersistence/model"
)

var LogTag = "trace_dto"

type IncidentDto struct {
	ScenarioId      string  `json:"scenario_id"`
	ScenarioVersion string  `json:"scenario_version"`
	Title           string  `json:"title"`
	ScenarioType    string  `json:"scenario_type"`
	Velocity        float32 `json:"velocity"`
	TotalCount      int     `json:"total_count"`
	Source          string  `json:"source"`
	Destination     string  `json:"destination"`
	FirstSeen       string  `json:"first_seen"`
	LastSeen        string  `json:"last_seen"`
}

type ScenarioTableDto struct {
	ScenarioId      string `json:"scenario_id"`
	ScenarioVersion string `json:"scenario_version"`
	TraceId         string `json:"trace_id"`
	ScenarioTitle   string `json:"scenario_title"`
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

func (t ScenarioTableDto) GetAllColumns() []any {
	return []any{t.ScenarioId, t.ScenarioVersion, t.TraceId, t.ScenarioTitle, t.ScenarioType}
}

func (t TraceMetadataTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.SpanId, t.ParentSpanId, t.Source, t.Destination, t.Error, t.Metadata, t.LatencyMs, t.Protocol}
}

func (t TraceRawDataTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.SpanId, t.RequestPayload, t.ResponsePayload}
}

func ConvertScenarioToTraceDto(s model.Scenario) ([]ScenarioTableDto, []TraceMetadataTableDto, []TraceRawDataTableDto, *error) {
	var scenarioDtoList []ScenarioTableDto
	var traceMetadataDtoList []TraceMetadataTableDto
	var traceRawDataDtoList []TraceRawDataTableDto

	for traceId, spans := range s.TraceToSpansMap {
		var scenarioDto ScenarioTableDto
		var traceMetadataDto TraceMetadataTableDto
		var traceRawDataDto TraceRawDataTableDto

		scenarioDto.ScenarioId = s.ScenarioId
		scenarioDto.ScenarioVersion = s.ScenarioVersion
		scenarioDto.ScenarioType = s.ScenarioType
		scenarioDto.TraceId = traceId
		scenarioDto.ScenarioTitle = s.ScenarioTitle

		scenarioDtoList = append(scenarioDtoList, scenarioDto)

		for _, span := range spans {
			requestCompressedStr, err := zkCrypto.CompressStringGzip(span.RequestPayload)
			if err != nil {
				return nil, nil, nil, &err
			}

			responseCompressedStr, err := zkCrypto.CompressStringGzip(span.ResponsePayload)
			if err != nil {
				return nil, nil, nil, &err
			}

			m, err := json.Marshal(span.Metadata)
			if err != nil {
				return nil, nil, nil, &err
			}

			traceMetadataDto.TraceId = traceId
			traceMetadataDto.SpanId = span.SpanId
			traceMetadataDto.Source = span.Source
			traceMetadataDto.Destination = span.Destination
			traceMetadataDto.Error = span.Error
			traceMetadataDto.Metadata = string(m)
			traceMetadataDto.LatencyMs = *span.LatencyMs
			traceMetadataDto.Protocol = span.Protocol
			traceMetadataDto.ParentSpanId = span.ParentSpanId

			traceRawDataDto.TraceId = traceId
			traceRawDataDto.SpanId = span.SpanId
			traceRawDataDto.RequestPayload = requestCompressedStr
			traceRawDataDto.ResponsePayload = responseCompressedStr

			traceMetadataDtoList = append(traceMetadataDtoList, traceMetadataDto)
			traceRawDataDtoList = append(traceRawDataDtoList, traceRawDataDto)
		}
	}

	return scenarioDtoList, traceMetadataDtoList, traceRawDataDtoList, nil
}

func ValidateScenario(s model.Scenario) (bool, *zkerrors.ZkError) {
	if s.ScenarioId == "" {
		logger.Error(LogTag, "scenario_id empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	}

	if s.ScenarioVersion == "" {
		logger.Error(LogTag, "scenario_id empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	}

	if s.ScenarioType == "" {
		logger.Error(LogTag, "scenario_type empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	}

	if s.ScenarioTitle == "" {
		logger.Error(LogTag, "scenario_title empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	}

	for traceId, spans := range s.TraceToSpansMap {
		if traceId == "" {
			logger.Error(LogTag, "trace Id empty")
			return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
		}

		for _, span := range spans {
			if span.SpanId == "" {
				logger.Error(LogTag, "span_id empty")
				//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
				break

			}

			if span.Protocol == "" {
				logger.Error(LogTag, "protocol empty")
				//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
				break

			}

			if span.Source == "" {
				logger.Error(LogTag, "source empty")
				//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
				break

			}

			if span.Destination == "" {
				logger.Error(LogTag, "destination empty")
				//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
				break

			}

			if span.LatencyMs == nil {
				logger.Error(LogTag, "latency_ms empty")
				//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
				break

			}

			if span.RequestPayload == "" {
				logger.Error(LogTag, "request payload empty")
				//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
				break

			}

			if span.ResponsePayload == "" {
				logger.Error(LogTag, "response payload empty")
				//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
				break
			}

		}
	}

	return true, nil
}
