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

	if issuesDetails == nil || len(issuesDetails) == 0 {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return &zkErr
	}

	issuesDetailsDtoList := make([]dto.IssuesDetailDto, 0)

	for _, issuesDetail := range issuesDetails {
		b, zkErr, validIssueDetail := dto.ValidateAndSanitiseIssue(issuesDetail)
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
