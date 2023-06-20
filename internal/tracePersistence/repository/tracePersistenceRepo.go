package repository

import (
	"database/sql"
	"log"
	"scenario-manager/internal/tracePersistence/model/dto"

	"github.com/lib/pq"
	zkCommon "github.com/zerok-ai/zk-utils-go/common"
	"github.com/zerok-ai/zk-utils-go/interfaces"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/sqlDB"
)

const (
	ScenarioTraceTablePostgres = "scenario_trace"
	TraceMetadataTablePostgres = "trace_metadata"
	TraceRawDataTablePostgres  = "trace_raw_data"

	ScenarioId      = "scenario_id"
	ScenarioVersion = "scenario_version"
	TraceId         = "trace_id"
	ScenarioTitle   = "scenario_title"
	ScenarioType    = "scenario_type"

	SpanId          = "span_id"
	ParentSpanId    = "parent_span_id"
	Source          = "source"
	Destination     = "destination"
	Error           = "error"
	Metadata        = "metadata"
	LatencyMs       = "latency_ms"
	Protocol        = "protocol"
	RequestPayload  = "request_payload"
	ResponsePayload = "response_payload"

	GetIncidentData                            = "SELECT trace_id, count(*) as incident_count, destination, min(created_at) as first_seen, max(created_at) as last_seen  FROM trace_metadata where error_type=$1 AND source=$2 GROUP BY trace_id LIMIT $3 OFFSET $4"
	GetTraceQuery                              = "SELECT scenario_version, trace_id FROM scenario_trace WHERE scenario_id=$1 LIMIT $2 OFFSET $3"
	GetTraceRawDataQuery                       = "SELECT request_payload, response_payload FROM trace_raw_data WHERE trace_id=$1 AND span_id=$2 LIMIT $3 OFFSET $4"
	GetTraceMetadataQueryUsingTraceIdAndSpanId = "SELECT span_id, source, destination, error, metadata, latency_ms, protocol FROM trace_metadata WHERE trace_id=$1 AND span_id=$2 LIMIT $3 OFFSET $4"
	GetTraceMetadataQueryUsingTraceId          = "SELECT span_id, source, destination, error, metadata, latency_ms, protocol FROM trace_metadata WHERE trace_id=$1 LIMIT $2 OFFSET $3"

	InsertTraceQuery         = "INSERT INTO scenario_trace (scenario_id, scenario_version, trace_id, scenario_title, scenario_type) VALUES ($1, $2, $3, $4, $5)"
	InsertTraceMetadataQuery = "INSERT INTO trace_metadata (trace_id, span_id, source, destination, error, metadata, latency_ms, protocol) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
	InsertTraceRawDataQuery  = "INSERT INTO trace_raw_data (trace_id, span_id, request_payload, response_payload) VALUES ($1, $2, $3, $4)"

	UpsertTraceQuery         = "INSERT INTO scenario_trace (scenario_id, scenario_version, trace_id, scenario_title, scenario_type) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (scenario_id) DO NOTHING"
	UpsertTraceMetadataQuery = "INSERT INTO trace_metadata (trace_id, span_id, parent_span_id, source, destination, error, metadata, latency_ms, protocol) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (trace_id) DO NOTHING"
	UpsertTraceRawDataQuery  = "INSERT INTO trace_raw_data (trace_id, span_id, request_payload, response_payload) VALUES ($1, $2, $3, $4) ON CONFLICT (trace_id) DO NOTHING"
)

var LogTag = "zk_trace_persistence_repo"

type TracePersistenceRepo interface {
	GetIncidentData(source, errorType string, offset, limit int) ([]dto.IncidentDto, error)
	GetTraces(scenarioId string, offset, limit int) ([]dto.ScenarioTableDto, error)
	GetTracesMetadata(traceId, spanId string, offset, limit int) ([]dto.TraceMetadataTableDto, error)
	GetTracesRawData(traceId, spanId string, offset, limit int) ([]dto.TraceRawDataTableDto, error)
	SaveTraceList([]dto.ScenarioTableDto, []dto.TraceMetadataTableDto, []dto.TraceRawDataTableDto) error
}

type tracePersistenceRepo struct {
	dbRepo sqlDB.DatabaseRepo
}

func NewTracePersistenceRepo(dbRepo sqlDB.DatabaseRepo) TracePersistenceRepo {
	return &tracePersistenceRepo{dbRepo: dbRepo}
}

func (z tracePersistenceRepo) GetIncidentData(source, errorType string, offset, limit int) ([]dto.IncidentDto, error) {
	rows, err, closeRow := z.dbRepo.GetAll(GetIncidentData, []any{errorType, source, offset, limit})
	defer closeRow()
	if err != nil || rows == nil {
		zkLogger.Error(LogTag, err)
		return nil, err
	}

	var responseArr []dto.IncidentDto
	for rows.Next() {
		var rawData dto.IncidentDto
		err := rows.Scan(&rawData.ScenarioId, &rawData.ScenarioVersion, &rawData.TraceId, &rawData.Title,
			&rawData.Source, &rawData.Destination, &rawData.ScenarioType, &rawData.FirstSeen,
			&rawData.LastSeen, &rawData.Velocity, &rawData.TotalCount)
		if err != nil {
			log.Fatal(err)
		}

		responseArr = append(responseArr, rawData)
	}

	return responseArr, nil

}

func (z tracePersistenceRepo) GetTraces(scenarioId string, offset, limit int) ([]dto.ScenarioTableDto, error) {
	rows, err, closeRow := z.dbRepo.GetAll(GetTraceQuery, []any{scenarioId, limit, offset})
	defer closeRow()

	if err != nil || rows == nil {
		zkLogger.Error(LogTag, err)
		return nil, err
	}

	var responseArr []dto.ScenarioTableDto
	for rows.Next() {
		var rawData dto.ScenarioTableDto
		err := rows.Scan(&rawData.ScenarioVersion, &rawData.TraceId)
		if err != nil {
			log.Fatal(err)
		}
		rawData.ScenarioId = scenarioId

		responseArr = append(responseArr, rawData)
	}

	return responseArr, nil
}

func (z tracePersistenceRepo) GetTracesMetadata(traceId, spanId string, offset, limit int) ([]dto.TraceMetadataTableDto, error) {
	var query string
	var params []any
	if zkCommon.IsEmpty(spanId) {
		query = GetTraceMetadataQueryUsingTraceId
		params = []any{traceId, limit, offset}
	} else {
		query = GetTraceMetadataQueryUsingTraceIdAndSpanId
		params = []any{traceId, spanId, limit, offset}
	}

	rows, err, closeRow := z.dbRepo.GetAll(query, params)
	defer closeRow()

	if err != nil || rows == nil {
		zkLogger.Error(LogTag, err)
		return nil, err
	}

	var responseArr []dto.TraceMetadataTableDto
	for rows.Next() {
		var rawData dto.TraceMetadataTableDto
		err := rows.Scan(&rawData.SpanId, &rawData.Source, &rawData.Destination, &rawData.Error, &rawData.Metadata, &rawData.LatencyMs, &rawData.Protocol)
		if err != nil {
			log.Fatal(err)
		}

		rawData.TraceId = traceId
		responseArr = append(responseArr, rawData)
	}

	return responseArr, nil
}

func (z tracePersistenceRepo) GetTracesRawData(traceId, spanId string, offset, limit int) ([]dto.TraceRawDataTableDto, error) {
	rows, err, closeRow := z.dbRepo.GetAll(GetTraceRawDataQuery, []any{traceId, spanId, limit, offset})
	defer closeRow()

	if err != nil || rows == nil {
		zkLogger.Error(LogTag, err)
		return nil, err
	}

	var responseArr []dto.TraceRawDataTableDto
	for rows.Next() {
		var rawData dto.TraceRawDataTableDto
		err := rows.Scan(&rawData.RequestPayload, &rawData.ResponsePayload)
		if err != nil {
			log.Fatal(err)
		}
		rawData.TraceId = traceId
		rawData.SpanId = spanId
		responseArr = append(responseArr, rawData)
	}

	return responseArr, nil
}

func (z tracePersistenceRepo) SaveTraceList(t []dto.ScenarioTableDto, tmd []dto.TraceMetadataTableDto, trd []dto.TraceRawDataTableDto) error {
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

func doBulkInsertForTraceList(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, traceData, traceMetadata, traceRawData []interfaces.DbArgs) error {

	err := bulkInsert(tx, dbRepo, ScenarioTraceTablePostgres, []string{ScenarioId, ScenarioVersion, TraceId, ScenarioTitle, ScenarioType}, traceData)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk insert trace table", err)
		return err
	}

	err = bulkInsert(tx, dbRepo, TraceMetadataTablePostgres, []string{TraceId, SpanId, ParentSpanId, Source, Destination, Error, Metadata, LatencyMs, Protocol}, traceMetadata)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk insert traceMetadata table", err)
		return err
	}

	err = bulkInsert(tx, dbRepo, TraceRawDataTablePostgres, []string{TraceId, SpanId, RequestPayload, ResponsePayload}, traceRawData)
	if err != nil {
		zkLogger.Info(LogTag, "Error in bulk insert traceRawData table", err)
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

func doBulkUpsertForTraceList(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, traceData, traceMetadata, traceRawData []interfaces.DbArgs) error {

	err := bulkUpsert(tx, dbRepo, UpsertTraceQuery, traceData)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for trace table", err)
		return err
	}

	err = bulkUpsert(tx, dbRepo, UpsertTraceMetadataQuery, traceMetadata)
	if err != nil {
		zkLogger.Error(LogTag, "Error in bulk upsert for trace table", err)
		return err
	}

	err = bulkUpsert(tx, dbRepo, UpsertTraceRawDataQuery, traceRawData)
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

func doUpsert(tx *sql.Tx, dbRepo sqlDB.DatabaseRepo, query string, data interfaces.DbArgs) error {
	stmt, errT := GetStmtRawQuery(tx, query)
	if errT != nil {
		zkLogger.Error(LogTag, "Error Creating statement for upsert", errT)
		return errT
	}

	_, err := dbRepo.Upsert(stmt, data)
	if err != nil {
		zkLogger.Error(LogTag, "Error in upsert", err)
		return err
	}

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
