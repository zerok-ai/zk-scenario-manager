package repository

import (
	"database/sql"
	"github.com/lib/pq"
	"github.com/zerok-ai/zk-utils-go/interfaces"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/sqlDB"
	"scenario-manager/internal/tracePersistence/model/dto"
)

const (
	ScenarioTraceTablePostgres = "scenario_trace"
	SpanTablePostgres          = "span"
	SpanRawDataTablePostgres   = "span_raw_data"

	ScenarioId      = "scenario_id"
	ScenarioVersion = "scenario_version"
	TraceId         = "trace_id"

	SpanId          = "span_id"
	ParentSpanId    = "parent_span_id"
	Source          = "source"
	Destination     = "destination"
	WorkloadIdList  = "workload_id_list"
	Metadata        = "metadata"
	LatencyMs       = "latency_ms"
	Protocol        = "protocol"
	RequestPayload  = "request_payload"
	ResponsePayload = "response_payload"

	UpsertTraceQuery       = "INSERT INTO scenario_trace (scenario_id, scenario_version, trace_id, scenario_title, scenario_type) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (scenario_id) DO NOTHING"
	UpsertSpanQuery        = "INSERT INTO span (trace_id, span_id, parent_span_id, source, destination, workload_id_list, metadata, latency_ms, protocol) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (trace_id, span_id) DO NOTHING"
	UpsertSpanRawDataQuery = "INSERT INTO span_raw_data (trace_id, span_id, request_payload, response_payload) VALUES ($1, $2, $3, $4) ON CONFLICT (trace_id, span_id) DO NOTHING"
)

var LogTag = "zk_trace_persistence_repo"

type TracePersistenceRepo interface {
	SaveTraceList([]dto.ScenarioTableDto, []dto.SpanTableDto, []dto.SpanRawDataTableDto) error
	Close() error
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

func (z tracePersistenceRepo) SaveTraceList(t []dto.ScenarioTableDto, tmd []dto.SpanTableDto, trd []dto.SpanRawDataTableDto) error {
	traceTableData := make([]interfaces.DbArgs, 0)
	traceTableMetadata := make([]interfaces.DbArgs, 0)
	traceTableRawData := make([]interfaces.DbArgs, 0)
	for i := range t {
		traceTableData = append(traceTableData, t[i])
	}

	for i := range tmd {
		traceTableMetadata = append(traceTableMetadata, tmd[i])
	}

	for i := range trd {
		traceTableRawData = append(traceTableRawData, trd[i])
	}

	tx, err := z.dbRepo.CreateTransaction()
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating transaction")
		return err
	}

	err = doBulkInsertForTraceList(tx, z.dbRepo, traceTableData, traceTableMetadata, traceTableRawData)
	if err == nil {
		tx.Commit()
		return nil
	}
	tx.Rollback()

	tx, err = z.dbRepo.CreateTransaction()
	if err != nil {
		zkLogger.Info(LogTag, "Error Creating transaction")
		return err
	}
	zkLogger.Info(LogTag, "CopyIn failed, starting upsert")
	err = doBulkUpsertForTraceList(tx, z.dbRepo, traceTableData, traceTableMetadata, traceTableRawData)
	if err == nil {
		tx.Commit()
		return nil
	}

	tx.Rollback()
	return err
}

func doBulkInsertForTraceList(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, traceData, span, spanRawData []interfaces.DbArgs) error {

	err := bulkInsert(tx, dbRepo, ScenarioTraceTablePostgres, []string{ScenarioId, ScenarioVersion, TraceId}, traceData)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk insert trace table", err)
		return err
	}

	err = bulkInsert(tx, dbRepo, SpanTablePostgres, []string{TraceId, SpanId, ParentSpanId, Source, Destination, WorkloadIdList, Metadata, LatencyMs, Protocol}, span)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk insert span table", err)
		return err
	}

	err = bulkInsert(tx, dbRepo, SpanRawDataTablePostgres, []string{TraceId, SpanId, RequestPayload, ResponsePayload}, spanRawData)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk insert spanRawData table", err)
		return err
	}

	return nil
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

func doBulkUpsertForTraceList(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, traceData, span, spanRawData []interfaces.DbArgs) error {

	err := bulkUpsert(tx, dbRepo, UpsertTraceQuery, traceData)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for trace table", err)
		return err
	}

	err = bulkUpsert(tx, dbRepo, UpsertSpanQuery, span)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for trace table", err)
		return err
	}

	err = bulkUpsert(tx, dbRepo, UpsertSpanRawDataQuery, spanRawData)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for trace table", err)
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
