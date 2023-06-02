package service

import (
	"fmt"
	"scenario-manager/internal/tracePersistence/model"
	"scenario-manager/internal/tracePersistence/repository"

	zkCommon "github.com/zerok-ai/zk-utils-go/common"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
)

var LogTag = "zk_trace_persistence_service"

type TracePersistenceService interface {
	GetTraces(scenarioId string, offset, limit int) (*[]model.TraceTable, *zkErrors.ZkError)
	GetTracesMetaData(traceId, spanId string, offset, limit int) (*[]model.TraceMetaDataTable, *zkErrors.ZkError)
	GetTracesRawData(traceId, spanId string, offset, limit int) (*[]model.TraceRawDataResponseObject, *zkErrors.ZkError)
	SaveTraceList([]model.Trace) *zkErrors.ZkError
	SaveTrace(model.Trace) *zkErrors.ZkError
}

func NewScenarioPersistenceService(repo repository.TracePersistenceRepo) TracePersistenceService {
	return tracePersistenceService{repo: repo}
}

type tracePersistenceService struct {
	repo repository.TracePersistenceRepo
}

func (s tracePersistenceService) GetTraces(scenarioId string, offset, limit int) (*[]model.TraceTable, *zkErrors.ZkError) {
	if offset < 0 || limit < 0 {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return nil, &zkErr
	}

	data, err := s.repo.GetTraces(scenarioId, offset, limit)
	if err == nil {
		return &data, nil
	}
	zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
	return nil, &zkErr
}

func (s tracePersistenceService) GetTracesMetaData(traceId, spanId string, offset, limit int) (*[]model.TraceMetaDataTable, *zkErrors.ZkError) {
	if offset < 0 || limit < 0 {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, nil)
		return nil, &zkErr
	}

	data, err := s.repo.GetTracesMetaData(traceId, spanId, offset, limit)
	if err == nil {
		return &data, nil
	}
	zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
	return nil, &zkErr
}

func (s tracePersistenceService) GetTracesRawData(traceId, spanId string, offset, limit int) (*[]model.TraceRawDataResponseObject, *zkErrors.ZkError) {
	//TODO: discuss if the below condition of limit > 100 is fine. or it should be read from some config
	threshold := 100
	if offset < 0 || limit < 0 || limit > threshold {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, fmt.Sprintf("either offset or limit < 0 or limit > %d", threshold))
		return nil, &zkErr
	}

	data, err := s.repo.GetTracesRawData(traceId, spanId, offset, limit)
	if err != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return nil, &zkErr
	}

	respArr := make([]model.TraceRawDataResponseObject, 0)

	for _, d := range data {
		c, err := model.ConvertTraceRawDataToTraceRawDataResponse(d)
		if err != nil {
			zkLogger.Error(LogTag, err)
		}
		respArr = append(respArr, *c)
	}
	return &respArr, nil
}

func (s tracePersistenceService) SaveTraceList(traces []model.Trace) *zkErrors.ZkError {
	if len(traces) == 0 {
		return zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "length of traces is 0"))
	}

	//TODO: discuss if the below condition of length > 100 is fine. or it should be read from some config
	threshold := 100
	if len(traces) > threshold {
		return zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, fmt.Sprintf("length of traces is > %d", threshold)))
	}

	traceDtoList := make([]model.TraceDto, 0)
	for _, t := range traces {
		c, err := model.ConvertTraceToTraceDto(t)
		if err != nil {
			zkLogger.Error(LogTag, err)
		}
		traceDtoList = append(traceDtoList, *c)
	}

	err := s.repo.SaveTraceList(traceDtoList)
	if err != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}

func (s tracePersistenceService) SaveTrace(trace model.Trace) *zkErrors.ZkError {

	if b, zkErr := validateTrace(trace); !b || zkErr != nil {
		return zkErr
	}

	traceDto, err := model.ConvertTraceToTraceDto(trace)
	if err != nil {
		zkLogger.Error(LogTag, err)
	}

	repoErr := s.repo.SaveTrace(*traceDto)
	if repoErr != nil {
		zkErr := zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorDbError, nil)
		return &zkErr
	}

	return nil
}

func validateTrace(trace model.Trace) (bool, *zkErrors.ZkError) {
	if trace.ScenarioId == "" {
		zkLogger.Error(LogTag, "scenario_id empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	if trace.ScenarioVersion == "" {
		zkLogger.Error(LogTag, "scenario_id empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	if trace.TraceId == "" {
		zkLogger.Error(LogTag, "trace Id empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	if trace.SpanId == "" {
		zkLogger.Error(LogTag, "span_id empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	if trace.Protocol == "" {
		zkLogger.Error(LogTag, "protocol empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	if trace.MetaData == nil {
		zkLogger.Error(LogTag, "meta data empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	if trace.RequestPayload == "" {
		zkLogger.Error(LogTag, "request payload empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	if trace.ResponsePayload == "" {
		zkLogger.Error(LogTag, "response payload empty")
		return false, zkCommon.ToPtr(zkErrors.ZkErrorBuilder{}.Build(zkErrors.ZkErrorBadRequest, "invalid data"))
	}

	return true, nil
}
