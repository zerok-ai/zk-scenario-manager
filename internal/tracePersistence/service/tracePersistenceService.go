package service

import (
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
	"scenario-manager/internal/tracePersistence/model"
	"scenario-manager/internal/tracePersistence/model/dto"
	"scenario-manager/internal/tracePersistence/repository"
)

var LogTag = "zk_trace_persistence_service"

type TracePersistenceService interface {
	SaveIncidents([]model.IncidentWithIssues) *zkErrors.ZkError
	SaveExceptions([]model.ExceptionData) *zkErrors.ZkError
	Close() error
}

func NewScenarioPersistenceService(repo repository.TracePersistenceRepo) TracePersistenceService {
	return tracePersistenceService{repo: repo}
}

func (s tracePersistenceService) Close() error {
	return s.repo.Close()
}

type tracePersistenceService struct {
	repo repository.TracePersistenceRepo
}

func (s tracePersistenceService) SaveIncidents(issuesDetails []model.IncidentWithIssues) *zkErrors.ZkError {

	issuesDetailsDtoList := make([]dto.IssuesDetailDto, 0)

	for _, issuesDetail := range issuesDetails {
		b, validIssueDetail, zkErr := dto.ValidateAndSanitiseIssue(issuesDetail)
		if !b || zkErr != nil {
			zkLogger.Error("Invalid issuesDetail", zkErr)
			continue
		}

		v, err := dto.ConvertIncidentIssuesToIssueDto(validIssueDetail)
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

func (s tracePersistenceService) SaveExceptions(exceptions []model.ExceptionData) *zkErrors.ZkError {
	sanitizedExceptions := make([]model.ExceptionData, 0)
	for _, exception := range exceptions {
		if exception.Id == "" || exception.ExceptionBody == "" {
			continue
		}
		sanitizedExceptions = append(sanitizedExceptions, exception)
	}

	if len(sanitizedExceptions) == 0 {
		zkLogger.Error(LogTag, "Empty exception list, or contains invalid data")
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return &zkErr
	}

	exceptionDtoList := make([]dto.ExceptionTableDto, 0)
	for _, exception := range sanitizedExceptions {
		exceptionDto, err := dto.ConvertExceptionToExceptionDto(exception)
		if err != nil {
			zkLogger.Error(LogTag, err)
			zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
			return &zkErr
		}
		exceptionDtoList = append(exceptionDtoList, exceptionDto)
	}

	err := s.repo.SaveExceptions(exceptionDtoList)
	if err != nil {
		zkLogger.Error(LogTag, "Failed to save exceptions", err)
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}
