package filters

import (
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"strings"
)

var (
	mySqlMethod = []string{"Sleep", "Quit", "InitDB", "Query", "FieldList", "CreateDB", "DropDB", "Refresh", "Shutdown", "Statistics", "ProcessInfo", "Connect", "ProcessKill", "Debug", "Ping", "Time", "DelayedInsert", "ChangeUser", "BinlogDump", "TableDump", "ConnectOut", "RegisterSlave", "StmtPrepare", "StmtExecute", "StmtSendLongData", "StmtClose", "StmtReset", "SetOption", "StmtFetch", "Daemon", "BinlogDumpGTID", "ResetConnection"}
)

func getHttpRawData(value models.HttpRawDataModel) tracePersistenceModel.SpanRawData {
	raw := tracePersistenceModel.SpanRawData{
		TraceID:     value.TraceId,
		SpanID:      value.SpanId,
		ReqHeaders:  value.ReqHeaders,
		RespHeaders: value.RespHeaders,
		IsTruncated: value.IsTruncated,
		ReqBody:     value.ReqBody,
		RespBody:    value.RespBody,
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

func getMySqlRawData(mySqlSpan models.MySQLRawDataModel) tracePersistenceModel.SpanRawData {
	raw := tracePersistenceModel.SpanRawData{
		ReqBody:     mySqlSpan.ReqBody,
		RespBody:    mySqlSpan.RespBody,
		IsTruncated: mySqlSpan.IsTruncated,
	}
	return raw
}

func enrichSpanFromMySQLRawData(span *tracePersistenceModel.Span, mySqlSpan *models.MySQLRawDataModel) *tracePersistenceModel.Span {
	span.Source = mySqlSpan.Source
	span.Destination = mySqlSpan.Destination
	span.WorkloadIDList = strings.Split(mySqlSpan.WorkloadIds, ",")
	span.ResponsePayloadSize = mySqlSpan.Rows
	span.Method = mySqlMethod[mySqlSpan.ReqCmd]
	span.Status = mySqlSpan.RespStatus

	span.SpanRawData = getMySqlRawData(*mySqlSpan)
	return span
}

func getPgSqlRawData(pgSpan models.PgSQLRawDataModel) tracePersistenceModel.SpanRawData {
	raw := tracePersistenceModel.SpanRawData{
		ReqBody:     pgSpan.Req,
		RespBody:    pgSpan.Resp,
		IsTruncated: pgSpan.IsTruncated,
	}

	return raw
}

func enrichSpanFromPostgresRawData(span *tracePersistenceModel.Span, pgSpan *models.PgSQLRawDataModel) *tracePersistenceModel.Span {
	span.Source = pgSpan.Source
	span.Destination = pgSpan.Destination
	span.WorkloadIDList = strings.Split(pgSpan.WorkloadIds, ",")
	span.Method = pgSpan.ReqCmd

	span.SpanRawData = getPgSqlRawData(*pgSpan)
	return span
}
