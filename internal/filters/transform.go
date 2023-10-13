package filters

import (
	"encoding/json"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	typedef "scenario-manager/internal"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"strings"
)

var (
	mySqlMethod = []string{"Sleep", "Quit", "InitDB", "Query", "FieldList", "CreateDB", "DropDB", "Refresh", "Shutdown", "Statistics", "ProcessInfo", "Connect", "ProcessKill", "Debug", "Ping", "Time", "DelayedInsert", "ChangeUser", "BinlogDump", "TableDump", "ConnectOut", "RegisterSlave", "StmtPrepare", "StmtExecute", "StmtSendLongData", "StmtClose", "StmtReset", "SetOption", "StmtFetch", "Daemon", "BinlogDumpGTID", "ResetConnection"}
)

func convertInterfaceMapToString(i interface{}) string {

	if mapToConvert, ok := i.(typedef.GenericMap); ok {
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

func enrichSpanFromHTTPRawData(span *tracePersistenceModel.Span, fullSpan *models.HttpRawDataModel, fullSpanVersion string) *tracePersistenceModel.Span {
	span.Source = fullSpan.Source
	span.Destination = fullSpan.Destination

	// workload id
	if fullSpan.WorkloadIds != "" {

		// split workload ids
		pxWorkloadList := strings.Split(fullSpan.WorkloadIds, ",")

		// append workload ids to workload ids in the span
		spanWorkloadIDList := span.WorkloadIDList
		if spanWorkloadIDList == nil {
			span.WorkloadIDList = pxWorkloadList
		} else {
			spanWorkloadIDList = append(spanWorkloadIDList, pxWorkloadList...)
			span.WorkloadIDList = spanWorkloadIDList
		}
	}

	// body size
	span.RequestPayloadSize = fullSpan.ReqBodySize
	span.ResponsePayloadSize = fullSpan.RespBodySize

	// response raw data
	span.SpanRawData = getHttpRawData(*fullSpan)

	//if span.Protocol == "" {
	//	span.Protocol = "http"
	//}

	span.EBPFSchemaVersion = fullSpanVersion

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

	// workload id
	if mySqlSpan.WorkloadIds != "" {

		// split workload ids
		pxWorkloadList := strings.Split(mySqlSpan.WorkloadIds, ",")

		// append workload ids to workload ids in the span
		spanWorkloadIDList := span.WorkloadIDList
		if spanWorkloadIDList == nil {
			span.WorkloadIDList = pxWorkloadList
		} else {
			spanWorkloadIDList = append(spanWorkloadIDList, pxWorkloadList...)
			span.WorkloadIDList = spanWorkloadIDList
		}

	}

	// body size
	span.ResponsePayloadSize = mySqlSpan.Rows

	// response raw data
	span.SpanRawData = getMySqlRawData(*mySqlSpan)

	//if !utils.IsEmpty(mySqlMethod[mySqlSpan.ReqCmd]) {
	//	span.Method = mySqlMethod[mySqlSpan.ReqCmd]
	//}

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

	// workload id
	if pgSpan.WorkloadIds != "" {
		// split workload ids
		pxWorkloadList := strings.Split(pgSpan.WorkloadIds, ",")

		// append workload ids to workload ids in the span
		spanWorkloadIDList := span.WorkloadIDList
		if spanWorkloadIDList == nil {
			span.WorkloadIDList = pxWorkloadList
		} else {
			spanWorkloadIDList = append(spanWorkloadIDList, pxWorkloadList...)
			span.WorkloadIDList = spanWorkloadIDList
		}
	}

	// response raw data
	span.SpanRawData = getPgSqlRawData(*pgSpan)

	//if !utils.IsEmpty(pgSpan.ReqCmd) {
	//	span.Method = pgSpan.ReqCmd
	//}

	return span
}
