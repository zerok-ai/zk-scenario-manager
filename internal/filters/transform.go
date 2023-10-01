package filters

import (
	"encoding/json"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/utils"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"strings"
)

var (
	mySqlMethod = []string{"Sleep", "Quit", "InitDB", "Query", "FieldList", "CreateDB", "DropDB", "Refresh", "Shutdown", "Statistics", "ProcessInfo", "Connect", "ProcessKill", "Debug", "Ping", "Time", "DelayedInsert", "ChangeUser", "BinlogDump", "TableDump", "ConnectOut", "RegisterSlave", "StmtPrepare", "StmtExecute", "StmtSendLongData", "StmtClose", "StmtReset", "SetOption", "StmtFetch", "Daemon", "BinlogDumpGTID", "ResetConnection"}
)

func convertInterfaceMapToString(i interface{}) string {

	if mapToConvert, ok := i.(map[string]interface{}); ok {
		s, err := json.Marshal(mapToConvert)
		if err != nil {
			zkLogger.Error("Error while converting interface to string, %v", i, err)
		}
		return string(s)
	} else {
		zkLogger.Error("Error while converting interface to string, %v", i)
		return ""
	}

}

func getHttpRawData(value models.HttpRawDataModel) tracePersistenceModel.SpanRawData {

	reqHeaderStr := convertInterfaceMapToString(value.ReqHeaders)
	resHeaderStr := convertInterfaceMapToString(value.RespHeaders)
	reqBodyStr, _ := value.ReqBody.(string)
	resBodyStr, _ := value.RespBody.(string)

	raw := tracePersistenceModel.SpanRawData{
		TraceID:     value.TraceId,
		SpanID:      value.SpanId,
		ReqHeaders:  reqHeaderStr,
		RespHeaders: resHeaderStr,
		IsTruncated: value.IsTruncated,
		ReqBody:     reqBodyStr,
		RespBody:    resBodyStr,
	}
	return raw
}

func enrichSpanFromHTTPRawData(span *tracePersistenceModel.Span, fullSpan *models.HttpRawDataModel) *tracePersistenceModel.Span {
	if span.IsRoot {
		span.Source = fullSpan.Source
		span.Destination = fullSpan.Destination
		span.Latency = uint64(fullSpan.Latency)
		span.Path = fullSpan.ReqPath
		span.Method = fullSpan.ReqMethod
		span.RequestPayloadSize = fullSpan.ReqBodySize
		span.ResponsePayloadSize = fullSpan.RespBodySize
		span.Status = fullSpan.RespStatus
		span.Protocol = "http"

		if fullSpan.WorkloadIds != "" {
			span.WorkloadIDList = strings.Split(fullSpan.WorkloadIds, ",")
		}
		span.SpanRawData = getHttpRawData(*fullSpan)

		return span
	}

	span.Source = fullSpan.Source
	span.Destination = fullSpan.Destination
	if fullSpan.WorkloadIds != "" {
		span.WorkloadIDList = strings.Split(fullSpan.WorkloadIds, ",")
	}
	span.RequestPayloadSize = fullSpan.ReqBodySize
	span.ResponsePayloadSize = fullSpan.RespBodySize

	if !utils.IsEmpty(fullSpan.ReqMethod) {
		span.Method = fullSpan.ReqMethod
	}

	if !utils.IsEmpty(fullSpan.ReqPath) {
		span.Path = fullSpan.ReqPath
	}

	if fullSpan.RespStatus != 0 {
		span.Status = fullSpan.RespStatus
	}

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
	span.Protocol = "mysql"
	if mySqlSpan.WorkloadIds != "" {
		span.WorkloadIDList = strings.Split(mySqlSpan.WorkloadIds, ",")
	}
	span.ResponsePayloadSize = mySqlSpan.Rows

	if !utils.IsEmpty(mySqlMethod[mySqlSpan.ReqCmd]) {
		span.Method = mySqlMethod[mySqlSpan.ReqCmd]
	}

	if mySqlSpan.RespStatus != 0 {
		span.Status = mySqlSpan.RespStatus
	}

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
	if pgSpan.WorkloadIds != "" {
		span.WorkloadIDList = strings.Split(pgSpan.WorkloadIds, ",")
	}

	if !utils.IsEmpty(pgSpan.ReqCmd) {
		span.Method = pgSpan.ReqCmd
	}

	span.SpanRawData = getPgSqlRawData(*pgSpan)
	return span
}
