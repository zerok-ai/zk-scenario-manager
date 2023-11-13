package service

import (
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/utils"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
	"scenario-manager/internal/tracePersistence/model"
	"scenario-manager/internal/tracePersistence/model/dto"
	"scenario-manager/internal/tracePersistence/repository"
)

var LogTag = "zk_trace_persistence_service"

type TracePersistenceService interface {
	SaveIncidents([]model.IncidentWithIssues) *zkErrors.ZkError
	SaveErrors([]model.ErrorData) *zkErrors.ZkError
	SaveEBPFData([]model.Span) *zkErrors.ZkError
	Close() error
}

func NewScenarioPersistenceService(repo repository.TracePersistenceRepo, obfuscate bool) TracePersistenceService {
	return tracePersistenceService{repo: repo, obfuscate: obfuscate}
}

func (s tracePersistenceService) Close() error {
	return s.repo.Close()
}

type tracePersistenceService struct {
	repo      repository.TracePersistenceRepo
	obfuscate bool
}

func (s tracePersistenceService) SaveIncidents(issuesDetails []model.IncidentWithIssues) *zkErrors.ZkError {

	issuesDetailsDtoList := make([]dto.IssuesDetailDto, 0)

	for _, issuesDetail := range issuesDetails {
		isValid, validIssueDetail, zkErr := dto.ValidateAndSanitiseIssue(issuesDetail)
		if !isValid || zkErr != nil {
			zkLogger.Error("Invalid issuesDetail", zkErr)
			continue
		}

		v, err := dto.ConvertIncidentIssuesToIssueDto(validIssueDetail, s.obfuscate)
		if err != nil {
			zkLogger.Error(LogTag, err)
			continue
		}

		issuesDetailsDtoList = append(issuesDetailsDtoList, v)
	}

	if len(issuesDetailsDtoList) == 0 {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return &zkErr
	}

	saveErr := s.repo.SaveTraceList(issuesDetailsDtoList)
	if saveErr != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}

func (s tracePersistenceService) SaveErrors(errors []model.ErrorData) *zkErrors.ZkError {
	sanitizedErrors := make([]model.ErrorData, 0)
	for _, e := range errors {
		if e.Id == "" || e.Data == "" {
			continue
		}
		sanitizedErrors = append(sanitizedErrors, e)
	}

	if len(sanitizedErrors) == 0 {
		zkLogger.Error(LogTag, "Empty error list, or contains invalid data")
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return &zkErr
	}

	errorDtoList := make([]dto.ErrorsDataTableDto, 0)
	for _, e := range sanitizedErrors {
		errorDto, err := dto.ConvertErrorToErrorDto(e)
		if err != nil {
			zkLogger.Error(LogTag, err)
			zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
			return &zkErr
		}
		errorDtoList = append(errorDtoList, errorDto)
	}

	err := s.repo.SaveErrors(errorDtoList)
	if err != nil {
		zkLogger.Error(LogTag, "Failed to save errors", err)
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}

func (s tracePersistenceService) SaveEBPFData(data []model.Span) *zkErrors.ZkError {
	sanitizedSpanData := make([]model.Span, 0)
	for _, spanRawData := range data {
		if !utils.IsEmpty(spanRawData.ReqBody) || !utils.IsEmpty(spanRawData.RespBody) || !utils.IsEmpty(spanRawData.ReqHeaders) || !utils.IsEmpty(spanRawData.RespHeaders) {
			continue
		}
		sanitizedSpanData = append(sanitizedSpanData, spanRawData)
	}

	if len(sanitizedSpanData) == 0 {
		zkLogger.Error(LogTag, "Empty raw data list, or contains invalid data")
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return &zkErr
	}

	rawDataTableDtoList := make([]dto.SpanRawDataTableDto, 0)
	spanTableDtoList := make([]dto.SpanTableDto, 0)
	for _, spanData := range sanitizedSpanData {
		rawDataDto, err := dto.GetRawDataDto(spanData.SpanRawData, s.obfuscate)
		if err != nil {
			zkLogger.Error(LogTag, err)
			zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
			return &zkErr
		}

		rawDataTableDtoList = append(rawDataTableDtoList, rawDataDto)
		spanTableDto := dto.SpanTableDto{TraceID: spanData.TraceID, SpanID: spanData.SpanID, WorkloadIDList: spanData.WorkloadIDList}
		spanTableDtoList = append(spanTableDtoList, spanTableDto)
	}

	err := s.repo.SaveEBPFData(rawDataTableDtoList, spanTableDtoList)
	if err != nil {
		zkLogger.Error(LogTag, "Failed to save raw data", err)
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}
