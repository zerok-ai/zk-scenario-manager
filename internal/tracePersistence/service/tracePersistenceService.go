package service

import (
	"fmt"
	"scenario-manager/internal/tracePersistence/model"
	"scenario-manager/internal/tracePersistence/model/dto"
	"scenario-manager/internal/tracePersistence/model/response"
	"scenario-manager/internal/tracePersistence/repository"

	zkCommon "github.com/zerok-ai/zk-utils-go/common"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
)

var LogTag = "zk_trace_persistence_service"

type TracePersistenceService interface {
	GetIncidentData(source, errorType string, offset, limit int) (traceresponse.IncidentResponse, *zkErrors.ZkError)
	GetTraces(scenarioId string, offset, limit int) (traceresponse.TraceResponse, *zkErrors.ZkError)
	GetTracesMetadata(traceId, spanId string, offset, limit int) (traceresponse.TraceMetadataResponse, *zkErrors.ZkError)
	GetTracesRawData(traceId, spanId string, offset, limit int) (traceresponse.TraceRawDataResponse, *zkErrors.ZkError)
	SaveTraceList([]model.Scenario) *zkErrors.ZkError
}

func NewScenarioPersistenceService(repo repository.TracePersistenceRepo) TracePersistenceService {
	return tracePersistenceService{repo: repo}
}

type tracePersistenceService struct {
	repo repository.TracePersistenceRepo
}

func (s tracePersistenceService) GetIncidentData(source, errorType string, offset, limit int) (traceresponse.IncidentResponse, *zkErrors.ZkError) {
	var response traceresponse.IncidentResponse
	if offset < 0 || limit < 1 {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return response, &zkErr
	}

	data, err := s.repo.GetIncidentData(source, errorType, offset, limit)
	if err == nil {
		response, respErr := traceresponse.ConvertIncidentToIncidentResponse(data)
		if respErr != nil {
			zkLogger.Error(LogTag, err)
		}
		return *response, nil
	}
	zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
	return response, &zkErr
}

func (s tracePersistenceService) GetTraces(scenarioId string, offset, limit int) (traceresponse.TraceResponse, *zkErrors.ZkError) {
	var response traceresponse.TraceResponse
	if offset < 0 || limit < 1 {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return response, &zkErr
	}

	data, err := s.repo.GetTraces(scenarioId, offset, limit)
	if err == nil {
		response, respErr := traceresponse.ConvertTraceToTraceResponse(data)
		if respErr != nil {
			zkLogger.Error(LogTag, err)
		}
		return *response, nil
	}
	zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
	return response, &zkErr
}

func (s tracePersistenceService) GetTracesMetadata(traceId, spanId string, offset, limit int) (traceresponse.TraceMetadataResponse, *zkErrors.ZkError) {
	var response traceresponse.TraceMetadataResponse

	if offset < 0 || limit < 0 {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return response, &zkErr
	}

	data, err := s.repo.GetTracesMetadata(traceId, spanId, offset, limit)
	if err != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return response, &zkErr
	}

	x, respErr := traceresponse.ConvertTraceMetadataToTraceMetadataResponse(data)
	if respErr != nil {
		zkLogger.Error(LogTag, err)
	}
	return *x, nil
}

func (s tracePersistenceService) GetTracesRawData(traceId, spanId string, offset, limit int) (traceresponse.TraceRawDataResponse, *zkErrors.ZkError) {
	var response traceresponse.TraceRawDataResponse
	//TODO: discuss if the below condition of limit > 100 is fine. or it should be read from some config
	threshold := 100
	if offset < 0 || limit < 0 || limit > threshold {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, fmt.Sprintf("either offset or limit < 0 or limit > %d", threshold))
		return response, &zkErr
	}

	data, err := s.repo.GetTracesRawData(traceId, spanId, offset, limit)
	if err != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return response, &zkErr
	}

	x, respErr := traceresponse.ConvertTraceRawDataToTraceRawDataResponse(data)
	if respErr != nil {
		zkLogger.Error(LogTag, err)
	}
	return *x, nil
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
	traceMetadataDtoList := make([]dto.TraceMetadataTableDto, 0)
	traceRawDataDtoList := make([]dto.TraceRawDataTableDto, 0)
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
		traceMetadataDtoList = append(traceMetadataDtoList, tmd...)
		traceRawDataDtoList = append(traceRawDataDtoList, trd...)

	}

	err := s.repo.SaveTraceList(traceDtoList, traceMetadataDtoList, traceRawDataDtoList)
	if err != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}
