package repository

import (
	"database/sql"
	"github.com/lib/pq"
	"github.com/zerok-ai/zk-utils-go/interfaces"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/sqlDB"
	"scenario-manager/internal/tracePersistence/model/dto"
	"strings"
	"time"
)

const (
	UpsertErrorQuery        = "INSERT INTO errors_data (id, data) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING"
	UpsertIssueQuery        = "INSERT INTO issue (issue_hash, issue_title, scenario_id, scenario_version) VALUES ($1, $2, $3, $4) ON CONFLICT (issue_hash) DO NOTHING"
	UpsertIncidentQuery     = "INSERT INTO incident (trace_id, issue_hash, incident_collection_time, root_span_time) VALUES ($1, $2, $3, $4) ON CONFLICT (issue_hash, trace_id) DO NOTHING"
	UpsertSpanQuery         = "INSERT INTO span (trace_id, parent_span_id, span_id, span_name, is_root, kind, start_time, latency, source, destination, workload_id_list, protocol, issue_hash_list, request_payload_size, response_payload_size, method, route, scheme, path, query, status, username, source_ip, destination_ip, service_name, errors, span_attributes, resource_attributes, scope_attributes, has_raw_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30) ON CONFLICT (trace_id, span_id) DO NOTHING"
	UpdateIsRootSpanQuery   = "UPDATE span SET is_root = $1 WHERE trace_id=$2 AND span_id=$3"
	DuplicateSpanQuery      = "INSERT INTO span ( trace_id, parent_span_id, span_id, is_root, kind, start_time, latency, SOURCE, destination, workload_id_list, protocol, issue_hash_list, request_payload_size, response_payload_size, METHOD, route, scheme, path, query, status, metadata, username) SELECT trace_id, $1 AS parent_span_id, $2 AS span_id, $3 AS is_root, $4 AS kind, start_time, latency, SOURCE, destination, workload_id_list, protocol, issue_hash_list, request_payload_size, response_payload_size, METHOD, route, scheme, path, query, status, metadata, username FROM span WHERE trace_id = $5 AND span_id = $6"
	UpdateRootSpanTimeQuery = "UPDATE incident SET root_span_time = $1 WHERE trace_id=$2"
	UpsertSpanRawDataQuery  = "INSERT INTO span_raw_data (trace_id, span_id, req_headers, resp_headers, is_truncated, req_body, resp_body) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (trace_id, span_id) DO UPDATE SET req_headers = excluded.req_headers, resp_headers = excluded.resp_headers, is_truncated = excluded.is_truncated, req_body = excluded.req_body, resp_body = excluded.resp_body"
	UpdateWorkloadIdList    = "UPDATE span SET workload_id_list = ARRAY(SELECT DISTINCT UNNEST(workload_id_list || $1)) WHERE trace_id=$2 AND span_id=$3"
	UpdateHasRawData        = "UPDATE span SET has_raw_data = $1 WHERE trace_id=$2 AND span_id=$3"
	GetRootSpanTimeQuery    = "SELECT start_time FROM span WHERE trace_id=$1 AND span_id=$2"
)

var LogTag = "zk_trace_persistence_repo"

type TracePersistenceRepo interface {
	SaveTraceList([]dto.IssuesDetailDto) error
	SaveErrors(errors []dto.ErrorsDataTableDto) error
	SaveEBPFData(rawData []dto.SpanRawDataTableDto, list []dto.SpanTableDto) error
	Close() error
	SaveNewRootSpan(traceId, newRootSpanId, oldRootSpanId, newRootSpanKind, newRootSpanParentId string) error
}

type tracePersistenceRepo struct {
	dbRepo sqlDB.DatabaseRepo
}

func NewTracePersistenceRepo(dbRepo sqlDB.DatabaseRepo) TracePersistenceRepo {
	return &tracePersistenceRepo{dbRepo: dbRepo}
}

func (z tracePersistenceRepo) Close() error {
	return z.dbRepo.Close()
}

func (z tracePersistenceRepo) SaveTraceList(issuesDetailList []dto.IssuesDetailDto) error {
	issueTableData := make([]interfaces.DbArgs, 0)
	traceTableData := make([]interfaces.DbArgs, 0)
	spanTableData := make([]interfaces.DbArgs, 0)
	spanTableRawData := make([]interfaces.DbArgs, 0)

	uniqueIssues := make(map[string]bool)
	uniqueTraces := make(map[string]bool)
	uniqueSpans := make(map[string]bool)
	uniqueRawSpans := make(map[string]bool)

	for _, issue := range issuesDetailList {

		for _, v := range issue.IssueTableDtoList {
			if _, ok := uniqueIssues[v.IssueHash]; !ok {
				uniqueIssues[v.IssueHash] = true
				issueTableData = append(issueTableData, v)
			}
		}

		for _, v := range issue.IncidentTableDtoList {
			key := v.IssueHash + v.TraceId
			if _, ok := uniqueTraces[key]; !ok {
				uniqueTraces[key] = true
				traceTableData = append(traceTableData, v)
			}
		}

		for _, v := range issue.SpanTableDtoList {
			key := v.TraceID + v.SpanID
			if _, ok := uniqueSpans[key]; !ok {
				uniqueSpans[key] = true
				spanTableData = append(spanTableData, v)
			}
		}

		for _, v := range issue.SpanRawDataTableList {
			key := v.TraceID + v.SpanID
			if _, ok := uniqueRawSpans[key]; !ok {
				uniqueRawSpans[key] = true
				spanTableRawData = append(spanTableRawData, v)
			}
		}
	}

	tx, err := z.dbRepo.CreateTransaction()
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating transaction")
		return err
	}

	err = doBulkUpsertForTraceList(tx, z.dbRepo, issueTableData, traceTableData, spanTableData, spanTableRawData)
	if err == nil {
		tx.Commit()
		return nil
	}

	tx.Rollback()
	return err
}

func (z tracePersistenceRepo) SaveErrors(errors []dto.ErrorsDataTableDto) error {
	uniqueErrors := make(map[string]bool)
	errorData := make([]interfaces.DbArgs, 0)
	for _, v := range errors {
		if _, ok := uniqueErrors[v.Id]; !ok {
			uniqueErrors[v.Id] = true
			errorData = append(errorData, v)
		}
	}

	tx, err := z.dbRepo.CreateTransaction()
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating transaction")
		return err
	}

	err = bulkUpsert(tx, z.dbRepo, UpsertErrorQuery, errorData)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for errors table", err)
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (z tracePersistenceRepo) SaveEBPFData(rawData []dto.SpanRawDataTableDto, list []dto.SpanTableDto) error {
	uniqueRawData := make(map[string]bool)
	data := make([]interfaces.DbArgs, 0)

	tx, err := z.dbRepo.CreateTransaction()
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating transaction")
		return err
	}

	for i, v := range list {
		stmt := z.dbRepo.CreateStatement(UpdateWorkloadIdList)
		result, err := z.dbRepo.Update(stmt, []any{list[i].WorkloadIDList, v.TraceID, v.SpanID})
		if err != nil {
			zkLogger.Error(LogTag, "Error in bulk upsert for raw data table", err)
			tx.Rollback()
			return err
		} else {
			zkLogger.Info(LogTag, "Update workload id list count:", result)
		}
	}

	for _, v := range rawData {
		key := v.TraceID + "_" + v.SpanID
		if _, ok := uniqueRawData[key]; !ok {
			uniqueRawData[key] = true
			data = append(data, v)
		}
	}

	for k := range uniqueRawData {
		stmt := z.dbRepo.CreateStatement(UpdateHasRawData)
		traceSpanArr := strings.Split(k, "_")
		traceId, spanId := traceSpanArr[0], traceSpanArr[1]
		result, err := z.dbRepo.Update(stmt, []any{true, traceId, spanId})
		if err != nil {
			zkLogger.Error(LogTag, "Error in bulk upsert for raw data table for has raw data", err)
			tx.Rollback()
			return err
		} else {
			zkLogger.Info(LogTag, "Update has_raw_data count:", result)
		}
	}

	err = bulkUpsert(tx, z.dbRepo, UpsertSpanRawDataQuery, data)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for raw data table", err)
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (z tracePersistenceRepo) SaveNewRootSpan(traceId, newRootSpanId, oldRootSpanId, newRootSpanKind, newRootSpanParentId string) error {
	tx, err := z.dbRepo.CreateTransaction()
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating transaction")
		return err
	}

	result, err := updateHelper(tx, z.dbRepo, DuplicateSpanQuery, []any{newRootSpanParentId, newRootSpanId, true, newRootSpanKind, traceId, oldRootSpanId})
	affectedRows, err := result.RowsAffected()
	if affectedRows != 1 {
		zkLogger.Error(LogTag, "Error in duplicating span, affected row not equal to 1", traceId, newRootSpanId, oldRootSpanId, err)
		tx.Rollback()
		return err
	}

	var rootSpanTime time.Time
	err = z.dbRepo.Get(GetRootSpanTimeQuery, []any{traceId, oldRootSpanId}, []any{&rootSpanTime})
	if err != nil {
		zkLogger.Error(LogTag, "Error in getting root span time", traceId, newRootSpanId, oldRootSpanId, err)
		tx.Rollback()
		return err
	}

	result, err = updateHelper(tx, z.dbRepo, UpdateRootSpanTimeQuery, []any{rootSpanTime, traceId})
	affectedRows, err = result.RowsAffected()
	if affectedRows != 1 {
		zkLogger.Error(LogTag, "Error in updating incident root span time, affected row not equal to 1", traceId, newRootSpanId, oldRootSpanId, err)
		tx.Rollback()
		return err
	}

	result, err = updateHelper(tx, z.dbRepo, UpdateIsRootSpanQuery, []any{false, traceId, oldRootSpanId})
	affectedRows, err = result.RowsAffected()
	if affectedRows != 1 {
		zkLogger.Error(LogTag, "Error in update is root span for traceId and spanId, affected row not equal to 1", traceId, newRootSpanId, oldRootSpanId, err)
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}

func updateHelper(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, query string, param []any) (sql.Result, error) {
	stmt, err := GetStmtRawQuery(tx, query)
	if err != nil {
		zkLogger.Error(LogTag, "Error in creating statement for update root span", err)
		return nil, err
	}

	result, err := dbRepo.Update(stmt, param)
	if err != nil {
		zkLogger.ErrorF(LogTag, "Error in update command %s %v %v", query, param, err)
		tx.Rollback()
		return nil, err
	}

	_, err = result.RowsAffected()
	if err != nil {
		zkLogger.ErrorF(LogTag, "error in getting rows affected for update %s %v %v", query, param, err)
		tx.Rollback()
		return nil, err
	}

	return result, nil
}

func bulkInsert(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, table string, columns []string, data []interfaces.DbArgs) error {

	stmt, err := GetStmtForCopyIn(tx, table, columns)
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating statement for copyIn", err)
		return err
	}

	err = dbRepo.BulkInsertUsingCopyIn(stmt, data)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk insert", err)
		return err
	}
	return nil
}

func doBulkUpsertForTraceList(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, issue, incident, span, spanRawData []interfaces.DbArgs) error {

	err := bulkUpsert(tx, dbRepo, UpsertIssueQuery, issue)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for issue table", err)
		return err
	}

	err = bulkUpsert(tx, dbRepo, UpsertIncidentQuery, incident)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for incident table", err)
		return err
	}

	err = bulkUpsert(tx, dbRepo, UpsertSpanQuery, span)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for span table", err)
		return err
	}

	err = bulkUpsert(tx, dbRepo, UpsertSpanRawDataQuery, spanRawData)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for span raw data", err)
		return err
	}

	return nil
}

func bulkUpsert(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, query string, data []interfaces.DbArgs) error {
	stmt, err := GetStmtRawQuery(tx, query)
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating statement for upsert", err)
		return err
	}

	results, err := dbRepo.BulkUpsert(stmt, data)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk upsert ", err)
		return err
	}

	var upsertCount int64
	for _, v := range results {
		c, _ := v.RowsAffected()
		upsertCount += c
	}
	zkLogger.Info(LogTag, "bulk upsert count:", upsertCount)

	return nil
}

func GetStmtForCopyIn(tx *sql.Tx, tableName string, columns []string) (*sql.Stmt, error) {
	stmt, err := tx.Prepare(pq.CopyIn(tableName, columns...))
	if err != nil {
		zkLogger.Error(LogTag, "Error preparing insert statement:", err)
		return nil, err
	}
	return stmt, nil
}

func GetStmtRawQuery(tx *sql.Tx, stmt string) (*sql.Stmt, error) {
	preparedStmt, err := tx.Prepare(stmt)
	if err != nil {
		zkLogger.Error(LogTag, "Error preparing insert statement:", err)
		return nil, err
	}
	return preparedStmt, nil
}
