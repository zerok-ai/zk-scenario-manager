package service

import (
	"fmt"
	zkCommon "github.com/zerok-ai/zk-utils-go/common"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
	"scenario-manager/internal/tracePersistence/model"
	"scenario-manager/internal/tracePersistence/model/dto"
	"scenario-manager/internal/tracePersistence/repository"
)

var LogTag = "zk_trace_persistence_service"

type TracePersistenceService interface {
	SaveTraceList([]model.Scenario) *zkErrors.ZkError
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

func (s tracePersistenceService) SaveTraceList(scenarios []model.Scenario) *zkErrors.ZkError {
	if len(scenarios) == 0 {
		return zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "length of scenarios is 0"))
	}

	// TODO: discuss if the below condition of length > 100 is fine. or it should be read from some config
	threshold := 100
	if len(scenarios) > threshold {
		return zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, fmt.Sprintf("length of scenarios is > %d", threshold)))
	}

	traceDtoList := make([]dto.ScenarioTableDto, 0)
	spanDtoList := make([]dto.SpanTableDto, 0)
	spanRawDataDtoList := make([]dto.SpanRawDataTableDto, 0)
	for _, scenario := range scenarios {
		if b, zkErr := dto.ValidateScenario(scenario); !b || zkErr != nil {
			zkLogger.Error("Invalid scenario", zkErr)
			continue
		}

		t, tmd, trd, err := dto.ConvertScenarioToTraceDto(scenario)
		if err != nil {
			zkLogger.Error(LogTag, err)
			continue
		}

		traceDtoList = append(traceDtoList, t...)
		spanDtoList = append(spanDtoList, tmd...)
		spanRawDataDtoList = append(spanRawDataDtoList, trd...)

	}

	err := s.repo.SaveTraceList(traceDtoList, spanDtoList, spanRawDataDtoList)
	if err != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}
