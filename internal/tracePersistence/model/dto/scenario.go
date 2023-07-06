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

type IssuesDetailDto struct {
	IssueTableDtoList    []IssueTableDto       `json:"issue_table_dto_list"`
	ScenarioTableDtoList []IncidentTableDto    `json:"scenario_table_dto_list"`
	SpanTableDtoList     []SpanTableDto        `json:"span_table_dto_list"`
	SpanRawDataTableList []SpanRawDataTableDto `json:"span_raw_data_table_list"`
}

type IssueTableDto struct {
	IssueId         string `json:"issue_id"`
	IssueTitle      string `json:"issue_title"`
	ScenarioId      string `json:"scenario_id"`
	ScenarioVersion string `json:"scenario_version"`
}

func (t IssueTableDto) GetAllColumns() []any {
	return []any{t.IssueId, t.IssueTitle, t.ScenarioId, t.ScenarioVersion}
}

type IncidentTableDto struct {
	TraceId   string    `json:"trace_id"`
	IssueId   string    `json:"issue_id"`
	CreatedAt time.Time `json:"created_at"`
}

func (t IncidentTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.IssueId}
}

func ConvertScenarioToTraceDto(s model.IncidentIssuesMapping) ([]IssueTableDto, []IncidentTableDto, []SpanTableDto, []SpanRawDataTableDto, *error) {
	var issueDtoList []IssueTableDto
	var scenarioDtoList []IncidentTableDto
	var spanDtoList []SpanTableDto
	var spanRawDataDtoList []SpanRawDataTableDto
	traceId := s.Incident.TraceId
	for _, issue := range s.IssueList {
		issueDto := IssueTableDto{
			IssueId:         issue.IssueId,
			IssueTitle:      issue.IssueTitle,
			ScenarioId:      s.ScenarioId,
			ScenarioVersion: s.ScenarioVersion,
		}
		issueDtoList = append(issueDtoList, issueDto)

		scenarioDto := IncidentTableDto{
			TraceId:   traceId,
			IssueId:   issue.IssueId,
			CreatedAt: s.Incident.IncidentCollectionTime,
		}
		scenarioDtoList = append(scenarioDtoList, scenarioDto)
	}

	for _, span := range s.Incident.Spans {
		var spanMetadataDto SpanTableDto
		var spanRawDataDto SpanRawDataTableDto

		requestCompressedStr, err := zkCrypto.CompressStringGzip(span.RequestPayload.GetString())
		if err != nil {
			return nil, nil, nil, nil, &err
		}

		responseCompressedStr, err := zkCrypto.CompressStringGzip(span.ResponsePayload.GetString())
		if err != nil {
			return nil, nil, nil, nil, &err
		}

		m, err := json.Marshal(span.Metadata)
		if err != nil {
			return nil, nil, nil, nil, &err
		}

		spanMetadataDto.TraceId = traceId
		spanMetadataDto.SpanId = span.SpanId
		spanMetadataDto.Source = span.Source
		spanMetadataDto.Destination = span.Destination
		spanMetadataDto.WorkloadIdList = span.WorkloadIdList
		spanMetadataDto.Status = span.ResponsePayload.GetStatus()
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

	return issueDtoList, scenarioDtoList, spanDtoList, spanRawDataDtoList, nil
}

func ValidateIssue(s model.IncidentIssuesMapping) (bool, *zkerrors.ZkError) {
	if s.ScenarioId == "" {
		logger.Error(LogTag, "scenario_id empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	}

	if s.ScenarioVersion == "" {
		logger.Error(LogTag, "scenario_version empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	}

	if s.Incident.TraceId == "" {
		logger.Error(LogTag, "trace Id empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	}

	for _, span := range s.Incident.Spans {
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

		if span.RequestPayload.GetString() == "" {
			logger.Error(LogTag, "request payload empty")
			//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
			break

		}

		if span.ResponsePayload.GetString() == "" {
			logger.Error(LogTag, "response payload empty")
			//return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
			break
		}
	}

	return true, nil
}
