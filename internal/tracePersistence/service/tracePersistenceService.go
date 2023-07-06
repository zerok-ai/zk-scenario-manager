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
	SaveIssues([]model.IssuesDetail) *zkErrors.ZkError
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

func (s tracePersistenceService) SaveIssues(issuesDetail []model.IssuesDetail) *zkErrors.ZkError {

	issuesDetailsDtoList := make([]dto.IssuesDetailDto, 0)

	for _, issuesDetail := range issuesDetail {
		issueDtoList := make([]dto.IssueTableDto, 0)
		traceDtoList := make([]dto.IncidentTableDto, 0)
		spanDtoList := make([]dto.SpanTableDto, 0)
		spanRawDataDtoList := make([]dto.SpanRawDataTableDto, 0)

		if b, zkErr := dto.ValidateIssue(issuesDetail); !b || zkErr != nil {
			zkLogger.Error("Invalid issuesDetail", zkErr)
			continue
		}

		i, t, tmd, trd, err := dto.ConvertScenarioToTraceDto(issuesDetail)
		if err != nil {
			zkLogger.Error(LogTag, err)
			continue
		}

		issueDtoList = append(issueDtoList, i...)
		traceDtoList = append(traceDtoList, t...)
		spanDtoList = append(spanDtoList, tmd...)
		spanRawDataDtoList = append(spanRawDataDtoList, trd...)

		v := dto.IssuesDetailDto{
			IssueTableDtoList:    issueDtoList,
			ScenarioTableDtoList: traceDtoList,
			SpanTableDtoList:     spanDtoList,
			SpanRawDataTableList: spanRawDataDtoList,
		}

		issuesDetailsDtoList = append(issuesDetailsDtoList, v)

	}

	saveErr := s.repo.SaveTraceList(issuesDetailsDtoList)
	if saveErr != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}
