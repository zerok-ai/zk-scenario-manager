package filters

import (
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"strings"
)

var sqlMethod = []string{"Sleep", "Quit", "InitDB", "Query", "FieldList", "CreateDB", "DropDB", "Refresh", "Shutdown", "Statistics", "ProcessInfo", "Connect", "ProcessKill", "Debug", "Ping", "Time", "DelayedInsert", "ChangeUser", "BinlogDump", "TableDump", "ConnectOut", "RegisterSlave", "StmtPrepare", "StmtExecute", "StmtSendLongData", "StmtClose", "StmtReset", "SetOption", "StmtFetch", "Daemon", "BinlogDumpGTID", "ResetConnection"}

func getHttpRawData(value models.HttpRawDataModel) tracePersistenceModel.SpanRawData {
	raw := tracePersistenceModel.SpanRawData{
		TraceID:     value.TraceId,
		SpanID:      value.SpanId,
		ReqHeaders:  value.ReqHeaders,
		RespHeaders: value.RespHeaders,
		IsTruncated: false,
		ReqBody:     value.ReqBody,
		RespBody:    value.RespBody,
	}
	return raw
}

func getMySqlRawData(value models.MySQLRawDataModel) tracePersistenceModel.SpanRawData {
	raw := tracePersistenceModel.SpanRawData{
		ReqBody:  value.ReqBody,
		RespBody: value.RespBody,
	}
	return raw
}

func enrichSpanFromHTTPRawData(span *tracePersistenceModel.Span, fullSpan *models.HttpRawDataModel) *tracePersistenceModel.Span {
	span.Source = fullSpan.Source
	span.Destination = fullSpan.Destination
	span.WorkloadIDList = strings.Split(fullSpan.WorkloadIds, ",")
	span.RequestPayloadSize = fullSpan.ReqBodySize
	span.ResponsePayloadSize = fullSpan.RespBodySize
	span.Method = fullSpan.ReqMethod
	span.Path = fullSpan.ReqPath
	span.Status = fullSpan.RespStatus

	span.SpanRawData = getHttpRawData(*fullSpan)
	return span
}

func enrichSpanFromMySQLRawData(span *tracePersistenceModel.Span, fullSpan *models.MySQLRawDataModel) *tracePersistenceModel.Span {
	span.Source = fullSpan.Source
	span.Destination = fullSpan.Destination
	span.WorkloadIDList = strings.Split(fullSpan.WorkloadIds, ",")
	span.ResponsePayloadSize = fullSpan.Rows
	span.Method = sqlMethod[fullSpan.ReqCmd]
	span.Status = fullSpan.RespStatus

	span.SpanRawData = getMySqlRawData(*fullSpan)
	return span
}

//func transformPGSpan(fullSpan models.PgSQLRawDataModel) tracePersistenceModel.Span {
//	spanForStorage := tracePersistenceModel.Span{
//		SpanID:         fullSpan.SpanId,
//		TraceID:        fullSpan.TraceId,
//		Source:         fullSpan.Source,
//		Destination:    fullSpan.Destination,
//		WorkloadIDList: strings.Split(fullSpan.WorkloadIds, ","),
//
//		Status:      fullSpan.RespStatus,
//		SpanRawData: getPgSQLRawData(fullSpan),
//	}
//	return spanForStorage
//}
