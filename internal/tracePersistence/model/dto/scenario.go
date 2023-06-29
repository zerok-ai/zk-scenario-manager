package dto

import (
	"encoding/json"
	"github.com/zerok-ai/zk-utils-go/common"
	zkCrypto "github.com/zerok-ai/zk-utils-go/crypto"
	logger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/zkerrors"
	"scenario-manager/internal/tracePersistence/model"
	"time"
)

var LogTag = "trace_dto"

type ScenarioTableDto struct {
	ScenarioId      string    `json:"scenario_id"`
	ScenarioVersion string    `json:"scenario_version"`
	TraceId         string    `json:"trace_id"`
	ScenarioTitle   string    `json:"scenario_title"`
	ScenarioType    string    `json:"scenario_type"`
	CreatedAt       time.Time `json:"created_at"`
}

func (t ScenarioTableDto) GetAllColumns() []any {
	return []any{t.ScenarioId, t.ScenarioVersion, t.TraceId, t.ScenarioTitle, t.ScenarioType}
}

func ConvertScenarioToTraceDto(s model.Scenario) ([]ScenarioTableDto, []SpanTableDto, []SpanRawDataTableDto, *error) {
	var scenarioDtoList []ScenarioTableDto
	var spanDtoList []SpanTableDto
	var spanRawDataDtoList []SpanRawDataTableDto

	for traceId, spans := range s.TraceToSpansMap {
		var scenarioDto ScenarioTableDto
		var spanMetadataDto SpanTableDto
		var spanRawDataDto SpanRawDataTableDto

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

			spanMetadataDto.TraceId = traceId
			spanMetadataDto.SpanId = span.SpanId
			spanMetadataDto.Source = span.Source
			spanMetadataDto.Destination = span.Destination
			spanMetadataDto.WorkloadIdList = span.WorkloadIdList
			spanMetadataDto.Metadata = string(m)
			spanMetadataDto.LatencyMs = *span.LatencyMs
			spanMetadataDto.Protocol = span.Protocol
			spanMetadataDto.ParentSpanId = span.ParentSpanId

			spanRawDataDto.TraceId = traceId
			spanRawDataDto.SpanId = span.SpanId
			spanRawDataDto.RequestPayload = requestCompressedStr
			spanRawDataDto.ResponsePayload = responseCompressedStr

			spanDtoList = append(spanDtoList, spanMetadataDto)
			spanRawDataDtoList = append(spanRawDataDtoList, spanRawDataDto)
		}
	}

	return scenarioDtoList, spanDtoList, spanRawDataDtoList, nil
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
