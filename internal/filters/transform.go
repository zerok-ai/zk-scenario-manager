package filters

import (
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"strconv"
	"strings"
)

func getHttpRequestData(value models.HttpRawDataModel) tracePersistenceModel.HTTPRequestPayload {
	req := tracePersistenceModel.HTTPRequestPayload{
		ReqPath:    value.ReqPath,
		ReqMethod:  value.ReqMethod,
		ReqHeaders: value.ReqHeaders,
		ReqBody:    value.ReqBody,
	}
	return req
}

func getHttpResponseData(value models.HttpRawDataModel) tracePersistenceModel.HTTPResponsePayload {
	res := tracePersistenceModel.HTTPResponsePayload{
		RespStatus:  strconv.Itoa(value.RespStatus),
		RespMessage: value.RespMessage,
		RespHeaders: value.RespHeaders,
		RespBody:    value.RespBody,
	}
	return res
}

func getMySQLRequestData(value models.MySQLRawDataModel) tracePersistenceModel.HTTPRequestPayload {
	req := tracePersistenceModel.HTTPRequestPayload{
		ReqPath: strconv.Itoa(value.RemotePort),
		ReqBody: value.ReqBody,
	}
	return req
}

func getMySQLResponseData(value models.MySQLRawDataModel) tracePersistenceModel.HTTPResponsePayload {
	res := tracePersistenceModel.HTTPResponsePayload{
		RespStatus: strconv.Itoa(value.RespStatus),
		RespBody:   value.RespBody,
	}
	return res
}

func getPgSQLRequestData(value models.PgSQLRawDataModel) tracePersistenceModel.HTTPRequestPayload {
	req := tracePersistenceModel.HTTPRequestPayload{
		ReqPath: strconv.Itoa(value.RemotePort),
		ReqBody: value.Req,
	}
	return req
}

func getPgSQLResponseData(value models.PgSQLRawDataModel) tracePersistenceModel.HTTPResponsePayload {
	res := tracePersistenceModel.HTTPResponsePayload{
		RespBody: value.Resp,
	}
	return res
}

func transformHTTPSpan(fullSpan models.HttpRawDataModel) tracePersistenceModel.Span {
	spanForStorage := tracePersistenceModel.Span{
		SpanId:          fullSpan.SpanId,
		TraceId:         fullSpan.TraceId,
		Source:          fullSpan.Source,
		Destination:     fullSpan.Destination,
		WorkloadIdList:  strings.Split(fullSpan.WorkloadIds, ","),
		Metadata:        map[string]interface{}{},
		LatencyNs:       &fullSpan.Latency,
		RequestPayload:  getHttpRequestData(fullSpan),
		ResponsePayload: getHttpResponseData(fullSpan),
		Time:            epochMilliSecondsToTime(fullSpan.Time),
	}
	return spanForStorage
}

func transformMySQLSpan(fullSpan models.MySQLRawDataModel) tracePersistenceModel.Span {
	spanForStorage := tracePersistenceModel.Span{
		SpanId:          fullSpan.SpanId,
		TraceId:         fullSpan.TraceId,
		Source:          fullSpan.Source,
		Destination:     fullSpan.Destination,
		WorkloadIdList:  strings.Split(fullSpan.WorkloadIds, ","),
		Metadata:        map[string]interface{}{},
		LatencyNs:       &fullSpan.Latency,
		RequestPayload:  getMySQLRequestData(fullSpan),
		ResponsePayload: getMySQLResponseData(fullSpan),
		Time:            epochMilliSecondsToTime(fullSpan.Time),
	}
	return spanForStorage
}

func transformPGSpan(fullSpan models.PgSQLRawDataModel) tracePersistenceModel.Span {
	spanForStorage := tracePersistenceModel.Span{
		SpanId:          fullSpan.SpanId,
		TraceId:         fullSpan.TraceId,
		Source:          fullSpan.Source,
		Destination:     fullSpan.Destination,
		WorkloadIdList:  strings.Split(fullSpan.WorkloadIds, ","),
		Metadata:        map[string]interface{}{},
		LatencyNs:       &fullSpan.Latency,
		RequestPayload:  getPgSQLRequestData(fullSpan),
		ResponsePayload: getPgSQLResponseData(fullSpan),
		Time:            epochMilliSecondsToTime(fullSpan.Time),
	}
	return spanForStorage
}
