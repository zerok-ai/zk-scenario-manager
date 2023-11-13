package dto

import (
	"bytes"
	"encoding/json"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/utils"
	"github.com/zerok-ai/zk-utils-go/common"
	zkCrypto "github.com/zerok-ai/zk-utils-go/crypto"
	zkHttp "github.com/zerok-ai/zk-utils-go/http"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/zkerrors"
	"io"
	"scenario-manager/internal/tracePersistence/model"
	"time"
)

var LogTag = "trace_dto"

type IssuesDetailDto struct {
	IssueTableDtoList    []IssueTableDto
	IncidentTableDtoList []IncidentTableDto
	SpanTableDtoList     []SpanTableDto
	SpanRawDataTableList []SpanRawDataTableDto
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

type ErrorsDataTableDto struct {
	Id   string `json:"id"`
	Data []byte `json:"data"`
}

func (e ErrorsDataTableDto) GetAllColumns() []any {
	return []any{e.Id, e.Data}
}

func ConvertErrorToErrorDto(errorData model.ErrorData) (ErrorsDataTableDto, *error) {
	var errorDto ErrorsDataTableDto
	if !utils.IsEmpty(errorData.Data) {
		compressedStr, err := zkCrypto.CompressStringGzip(errorData.Data)
		if err != nil {
			zkLogger.Error(LogTag, err)
			return errorDto, &err
		}
		errorDto.Data = compressedStr
	}
	errorDto.Id = errorData.Id

	return errorDto, nil
}

func GetRawDataDto(rawData model.SpanRawData, obfuscate bool) (SpanRawDataTableDto, *error) {
	var response SpanRawDataTableDto
	if obfuscate {
		rawData = obfuscateRawData(rawData)
	}

	var requestCompressedStr, responseCompressedStr []byte
	var err error
	if !utils.IsEmpty(rawData.ReqBody) {
		requestCompressedStr, err = zkCrypto.CompressStringGzip(rawData.ReqBody)
		if err != nil {
			return response, &err
		}
	}

	if !utils.IsEmpty(rawData.RespBody) {
		responseCompressedStr, err = zkCrypto.CompressStringGzip(rawData.RespBody)
		if err != nil {
			return response, &err
		}
	}

	response = SpanRawDataTableDto{
		TraceID:     rawData.TraceID,
		SpanID:      rawData.SpanID,
		ReqHeaders:  rawData.ReqHeaders,
		RespHeaders: rawData.RespHeaders,
		IsTruncated: rawData.IsTruncated,
		ReqBody:     requestCompressedStr,
		RespBody:    responseCompressedStr,
	}

	return response, nil

}

func ConvertIncidentIssuesToIssueDto(s model.IncidentWithIssues, obfuscate bool) (IssuesDetailDto, *error) {
	var response IssuesDetailDto
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
		spanDataDto := SpanToSpanDto(*span, traceId)
		spanDtoList = append(spanDtoList, spanDataDto)

		if spanDataDto.HasRawData {
			rawDataDto, err := GetRawDataDto(span.SpanRawData, obfuscate)
			if err != nil {
				zkLogger.Error(LogTag, err)
				return response, err
			}

			spanRawDataDtoList = append(spanRawDataDtoList, rawDataDto)
		}
	}

	zkLogger.Debug(LogTag, "issueDtoList len: ", len(issueDtoList))
	zkLogger.Debug(LogTag, "incidentDtoList len: ", len(incidentDtoList))
	zkLogger.Debug(LogTag, "spanDtoList len: ", len(spanDtoList))
	zkLogger.Debug(LogTag, "spanRawDataDtoList len: ", len(spanRawDataDtoList))

	response = IssuesDetailDto{
		IssueTableDtoList:    issueDtoList,
		IncidentTableDtoList: incidentDtoList,
		SpanTableDtoList:     spanDtoList,
		SpanRawDataTableList: spanRawDataDtoList,
	}

	return response, nil
}

func SpanToSpanDto(span model.Span, traceId string) SpanTableDto {
	s := SpanTableDto{
		TraceID:             traceId,
		SpanID:              span.SpanID,
		SpanName:            span.SpanName,
		ParentSpanID:        span.ParentSpanID,
		IsRoot:              span.IsRoot,
		Kind:                span.Kind,
		StartTime:           span.StartTime,
		Latency:             span.Latency,
		Source:              span.Source,
		Destination:         span.Destination,
		WorkloadIDList:      span.WorkloadIDList,
		Protocol:            string(span.Protocol),
		IssueHashList:       span.IssueHashList,
		RequestPayloadSize:  span.RequestPayloadSize,
		ResponsePayloadSize: span.ResponsePayloadSize,
		Method:              span.Method,
		Route:               span.Route,
		Scheme:              span.Scheme,
		Path:                span.Path,
		Query:               span.Query,
		Status:              span.Status,
		Username:            span.Username,
		SourceIP:            span.SourceIP,
		DestinationIP:       span.DestinationIP,
		ServiceName:         span.ServiceName,
		Errors:              span.Errors,

		SpanAttributes:     span.SpanAttributes,
		ResourceAttributes: span.ResourceAttributes,
		ScopeAttributes:    span.ScopeAttributes,
		HasRawData:         !utils.IsEmpty(span.ReqBody) || !utils.IsEmpty(span.RespBody) || !utils.IsEmpty(span.ReqHeaders) || !utils.IsEmpty(span.RespHeaders),
	}

	return s
}

func ValidateAndSanitiseIssue(s model.IncidentWithIssues) (bool, model.IncidentWithIssues, *zkerrors.ZkError) {
	var resp model.IncidentWithIssues
	if s.IssueGroupList == nil || len(s.IssueGroupList) == 0 {
		zkLogger.Error(LogTag, "issue_group_list empty")
		return false, resp, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "issue_group_list empty"))
	}

	if s.Incident.TraceId == "" {
		zkLogger.Error(LogTag, "traceid empty")
		return false, resp, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "traceId empty"))
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
			continue
		}

		i := model.IssueGroup{
			ScenarioId:      issueGroup.ScenarioId,
			ScenarioVersion: issueGroup.ScenarioVersion,
			Issues:          validIssuesList,
		}
		validIssueGroupList = append(validIssueGroupList, i)
	}

	if len(validIssueGroupList) == 0 {
		zkLogger.Error(LogTag, "issueGroup list empty")
		return false, resp, common.ToPtr(zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequest, "issueGroup list empty"))
	}

	resp.IssueGroupList = validIssueGroupList
	resp.Incident = s.Incident
	return true, resp, nil

}

func obfuscateRawData(rawData model.SpanRawData) model.SpanRawData {
	var responseRawData model.SpanRawData
	responseRawData.SpanID = rawData.SpanID
	responseRawData.TraceID = rawData.TraceID

	rawData.TraceID = ""
	rawData.SpanID = ""

	requestBody := struct {
		Data     model.SpanRawData `json:"data"`
		Language string            `json:"language"`
	}{
		Data:     rawData,
		Language: "en",
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		zkLogger.Error(LogTag, "Cannot Marshal data, encountered Err", err)
		return responseRawData
	}

	bodyReader := bytes.NewReader(jsonBody)
	response, zkErr := zkHttp.Create().Go("POST", "http://localhost:9103/i/obfuscate", bodyReader)
	if zkErr != nil {
		zkLogger.Error(LogTag, "Cannot obfuscate data, encountered Err in prisidio call", zkErr)
		return responseRawData
	}

	type rawDataStruct struct {
		Data model.SpanRawData `json:"data"`
	}

	responseBody, err := io.ReadAll(response.Body)
	var responseMap map[string]rawDataStruct
	err = json.Unmarshal(responseBody, &responseMap)
	if err != nil {
		zkLogger.Error(LogTag, "Cannot Unmarshal data, encountered Err", err)
		return responseRawData
	}

	o := responseMap["payload"].Data
	responseRawData.ReqBody = o.ReqBody
	responseRawData.RespBody = o.RespBody
	responseRawData.ReqHeaders = o.ReqHeaders
	responseRawData.RespHeaders = o.RespHeaders
	return responseRawData
}
