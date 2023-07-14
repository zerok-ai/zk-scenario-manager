package dto

import (
	"encoding/json"
	"github.com/zerok-ai/zk-utils-go/common"
	zkCrypto "github.com/zerok-ai/zk-utils-go/crypto"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
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
	IssueHash       string `json:"issue_hash"`
	IssueTitle      string `json:"issue_title"`
	ScenarioId      string `json:"scenario_id"`
	ScenarioVersion string `json:"scenario_version"`
}

func (t IssueTableDto) GetAllColumns() []any {
	return []any{t.IssueHash, t.IssueTitle, t.ScenarioId, t.ScenarioVersion}
}

type IncidentTableDto struct {
	TraceId                string    `json:"trace_id"`
	IssueHash              string    `json:"issue_hash"`
	IncidentCollectionTime time.Time `json:"incident_collection_time"`
}

func (t IncidentTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.IssueHash, t.IncidentCollectionTime}
}

func ConvertScenarioToTraceDto(s model.IncidentWithIssues) ([]IssueTableDto, []IncidentTableDto, []SpanTableDto, []SpanRawDataTableDto, *error) {
	var issueDtoList []IssueTableDto
	var incidentDtoList []IncidentTableDto
	var spanDtoList []SpanTableDto
	var spanRawDataDtoList []SpanRawDataTableDto
	traceId := s.Incident.TraceId
	incidentCollectionTime := s.Incident.IncidentCollectionTime
	for _, issueGroup := range s.IssueGroupList {
		for _, issue := range issueGroup.Issues {
			issueDto := IssueTableDto{
				IssueHash:       issue.IssueHash,
				IssueTitle:      issue.IssueTitle,
				ScenarioId:      issueGroup.ScenarioId,
				ScenarioVersion: issueGroup.ScenarioVersion,
			}
			issueDtoList = append(issueDtoList, issueDto)

			incidentDto := IncidentTableDto{
				TraceId:                traceId,
				IssueHash:              issue.IssueHash,
				IncidentCollectionTime: incidentCollectionTime,
			}
			incidentDtoList = append(incidentDtoList, incidentDto)
		}
	}

	for _, span := range s.Incident.Spans {
		var spanMetadataDto SpanTableDto
		var spanRawDataDto SpanRawDataTableDto

		var requestCompressedStr, responseCompressedStr []byte
		var err error
		if span.RequestPayload != nil {
			requestCompressedStr, err = zkCrypto.CompressStringGzip(span.RequestPayload.GetString())
			if err != nil {
				return nil, nil, nil, nil, &err
			}
		}

		if span.ResponsePayload != nil {
			responseCompressedStr, err = zkCrypto.CompressStringGzip(span.ResponsePayload.GetString())
			if err != nil {
				return nil, nil, nil, nil, &err
			}
			spanMetadataDto.Status = span.ResponsePayload.GetStatus()
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

		spanMetadataDto.Metadata = string(m)
		spanMetadataDto.LatencyMs = span.LatencyMs
		spanMetadataDto.Protocol = span.Protocol
		spanMetadataDto.ParentSpanId = span.ParentSpanId
		spanMetadataDto.IssueHashList = span.IssueHashList
		spanMetadataDto.Time = span.Time

		spanRawDataDto.TraceId = traceId
		spanRawDataDto.SpanId = span.SpanId
		spanRawDataDto.RequestPayload = requestCompressedStr
		spanRawDataDto.ResponsePayload = responseCompressedStr

		spanDtoList = append(spanDtoList, spanMetadataDto)
		spanRawDataDtoList = append(spanRawDataDtoList, spanRawDataDto)

	}

	return issueDtoList, incidentDtoList, spanDtoList, spanRawDataDtoList, nil
}

func ValidateIssue(s model.IncidentWithIssues) (bool, *zkerrors.ZkError) {
	if s.IssueGroupList == nil || len(s.IssueGroupList) == 0 {
		zkLogger.Error(LogTag, "issue_group_list empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "issue_group_list empty"))
	}

	if s.Incident.TraceId == "" {
		zkLogger.Error(LogTag, "traceid empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "traceId empty"))
	}

	validIssueGroupList := make([]model.IssueGroup, 0)

	for _, issueGroup := range s.IssueGroupList {
		if issueGroup.ScenarioId == "" {
			zkLogger.Error(LogTag, "scenario_id empty")
			continue
		}

		if issueGroup.ScenarioVersion == "" {
			zkLogger.Error(LogTag, "scenario_version empty")
			continue
		}

		if issueGroup.Issues == nil || len(issueGroup.Issues) == 0 {
			zkLogger.Error(LogTag, "issues list empty")
			continue
		}

		// todo: check if this is even possible to happen
		validIssuesList := make([]model.Issue, 0)

		for _, issue := range issueGroup.Issues {
			if issue.IssueHash == "" || issue.IssueTitle == "" {
				zkLogger.Error(LogTag, "issueHash or issueTitle empty")
				continue
			}
			validIssuesList = append(validIssuesList, issue)
		}

		if len(validIssuesList) == 0 {
			zkLogger.Error(LogTag, "issues list empty")
			continue
		}

		issueGroup.Issues = validIssuesList
		validIssueGroupList = append(validIssueGroupList, issueGroup)
	}

	if len(validIssueGroupList) == 0 {
		zkLogger.Error(LogTag, "issueGroup list empty")
		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "issueGroup list empty"))
	}

	s.IssueGroupList = validIssueGroupList
	return true, nil

	//for _, span := range s.Incident.Spans {
	//	if span.SpanId == "" {
	//		zkLogger.Error(LogTag, "span_id empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//
	//	if span.Protocol == "" {
	//		zkLogger.Error(LogTag, "protocol empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//
	//	if span.Source == "" {
	//		zkLogger.Error(LogTag, "source empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//
	//	if span.Destination == "" {
	//		zkLogger.Error(LogTag, "destination empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//
	//	if span.LatencyMs == nil {
	//		zkLogger.Error(LogTag, "latency_ms empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//
	//	if span.RequestPayload.GetString() == "" {
	//		zkLogger.Error(LogTag, "request payload empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//
	//	if span.ResponsePayload.GetString() == "" {
	//		zkLogger.Error(LogTag, "response payload empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//
	//	if span.IssueHashList == nil || len(span.IssueHashList) == 0 {
	//		zkLogger.Error(LogTag, "issue hash list empty")
	//		return false, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "invalid data"))
	//	}
	//}

	return true, nil
}
