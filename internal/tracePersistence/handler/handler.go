package handler

import (
	"github.com/kataras/iris/v12"
	zkHttp "github.com/zerok-ai/zk-utils-go/http"
	traceResponse "scenario-manager/internal/tracePersistence/model/response"
	"scenario-manager/internal/tracePersistence/service"
	"scenario-manager/internal/tracePersistence/validation"
	"scenario-manager/utils"
	"strconv"
)

type TracePersistenceHandler interface {
	GetIncidents(ctx iris.Context)
	GetTraces(ctx iris.Context)
	GetTracesMetadata(ctx iris.Context)
	GetTracesRawData(ctx iris.Context)
	SaveTraceList(ctx iris.Context)
	SaveTrace(ctx iris.Context)
}

type tracePersistenceHandler struct {
	service service.TracePersistenceService
}

func NewTracePersistenceHandler(persistenceService service.TracePersistenceService) TracePersistenceHandler {
	return &tracePersistenceHandler{
		service: persistenceService,
	}
}

func (t tracePersistenceHandler) GetIncidents(ctx iris.Context) {
	source := ctx.URLParam(utils.Source)
	errorType := ctx.URLParam(utils.ScenarioType)

	limit := ctx.URLParamDefault("limit", "50")
	offset := ctx.URLParamDefault("offset", "0")
	if err := validation.ValidateGetIncidentsDataApi(source, errorType, offset, limit); err != nil {
		z := &zkHttp.ZkHttpResponseBuilder[any]{}
		zkHttpResponse := z.WithZkErrorType(err.Error).Build()
		ctx.StatusCode(zkHttpResponse.Status)
		ctx.JSON(zkHttpResponse)
		return
	}

	l, _ := strconv.Atoi(limit)
	o, _ := strconv.Atoi(offset)

	resp, err := t.service.GetIncidentData(source, errorType, o, l)

	zkHttpResponse := zkHttp.ToZkResponse[traceResponse.IncidentResponse](200, resp, resp, err)
	ctx.StatusCode(zkHttpResponse.Status)
	ctx.JSON(zkHttpResponse)
}

func (t tracePersistenceHandler) GetTraces(ctx iris.Context) {
	scenarioId := ctx.URLParam(utils.ScenarioId)
	limit := ctx.URLParamDefault(utils.Limit, "50")
	offset := ctx.URLParamDefault(utils.Offset, "0")
	if err := validation.ValidateGetTracesApi(scenarioId, offset, limit); err != nil {
		z := &zkHttp.ZkHttpResponseBuilder[any]{}
		zkHttpResponse := z.WithZkErrorType(err.Error).Build()
		ctx.StatusCode(zkHttpResponse.Status)
		ctx.JSON(zkHttpResponse)
		return
	}

	l, _ := strconv.Atoi(limit)
	o, _ := strconv.Atoi(offset)

	resp, err := t.service.GetTraces(scenarioId, o, l)

	zkHttpResponse := zkHttp.ToZkResponse[traceResponse.TraceResponse](200, resp, resp, err)
	ctx.StatusCode(zkHttpResponse.Status)
	ctx.JSON(zkHttpResponse)
}

func (t tracePersistenceHandler) GetTracesMetadata(ctx iris.Context) {
	traceId := ctx.URLParam(utils.TraceId)
	spanId := ctx.URLParam(utils.SpanId)
	limit := ctx.URLParamDefault(utils.Limit, "50")
	offset := ctx.URLParamDefault(utils.Offset, "0")
	if err := validation.ValidateGetTracesMetadataApi(traceId, offset, limit); err != nil {
		z := &zkHttp.ZkHttpResponseBuilder[any]{}
		zkHttpResponse := z.WithZkErrorType(err.Error).Build()
		ctx.StatusCode(zkHttpResponse.Status)
		ctx.JSON(zkHttpResponse)
		return
	}

	l, _ := strconv.Atoi(limit)
	o, _ := strconv.Atoi(offset)

	resp, err := t.service.GetTracesMetadata(traceId, spanId, o, l)

	zkHttpResponse := zkHttp.ToZkResponse[traceResponse.TraceMetadataResponse](200, resp, resp, err)
	ctx.StatusCode(zkHttpResponse.Status)
	ctx.JSON(zkHttpResponse)
}

func (t tracePersistenceHandler) GetTracesRawData(ctx iris.Context) {
	traceId := ctx.URLParam(utils.TraceId)
	spanId := ctx.URLParam(utils.SpanId)
	limit := ctx.URLParamDefault(utils.Limit, "50")
	offset := ctx.URLParamDefault(utils.Offset, "0")
	if err := validation.ValidateGetTracesRawDataApi(traceId, spanId, offset, limit); err != nil {
		z := &zkHttp.ZkHttpResponseBuilder[any]{}
		zkHttpResponse := z.WithZkErrorType(err.Error).Build()
		ctx.StatusCode(zkHttpResponse.Status)
		ctx.JSON(zkHttpResponse)
		return
	}

	l, _ := strconv.Atoi(limit)
	o, _ := strconv.Atoi(offset)

	resp, err := t.service.GetTracesRawData(traceId, spanId, o, l)

	zkHttpResponse := zkHttp.ToZkResponse[traceResponse.TraceRawDataResponse](200, resp, resp, err)
	ctx.StatusCode(zkHttpResponse.Status)
	ctx.JSON(zkHttpResponse)
}

func (t tracePersistenceHandler) SaveTraceList(ctx iris.Context) {
	//TODO implement me
	panic("implement me")
}

func (t tracePersistenceHandler) SaveTrace(ctx iris.Context) {
	//TODO implement me
	panic("implement me")
}
