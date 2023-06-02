package repository

import (
	"context"
	"fmt"
	zklogger "github.com/zerok-ai/zk-utils-go/logs"
	"log"
	"scenario-manager/internal/tracePersistence/model"

	"github.com/zerok-ai/zk-utils-go/interfaces"
	zkUtilsPostgres "github.com/zerok-ai/zk-utils-go/storage/sqlDB/postgres"
	zkUtilsPostgresConfig "github.com/zerok-ai/zk-utils-go/storage/sqlDB/postgres/config"
)

const (
	TraceTablePostgres         = "trace"
	TraceMetaDataTablePostgres = "trace_meta_data"
	TraceRawDataTablePostgres  = "trace_raw_data"

	ScenarioId      = "scenario_id"
	ScenarioVersion = "scenario_version"
	TraceId         = "trace_id"
	SpanId          = "span_id"
	MetaData        = "meta_data"
	Protocol        = "protocol"
	RequestPayload  = "request_payload"
	ResponsePayload = "response_payload"

	GetTraceQuery         = "SELECT scenario_version, trace_id FROM trace WHERE scenario_id=$1 OFFSET=$2 LIMIT =$3"
	GetTraceRawDataQuery  = "SELECT request_payload, response_payload FROM trace_raw_data WHERE trace_id=$1 AND span_id=$2 OFFSET=$3 LIMIT =$4"
	GetTraceMetaDataQuery = "SELECT protocol, meta_data FROM trace_meta_data WHERE trace_id=$1 AND span_id=$2 OFFSET=$3 LIMIT =$4"

	InsertTraceQuery         = "INSERT INTO trace (scenario_id, scenario_version, trace_id) VALUES ($1, $2, $3)"
	InsertTraceMetaDataQuery = "INSERT INTO trace_meta_data (trace_id, span_id, meta_data, protocol) VALUES ($1, $2, $3, $4)"
	InsertTraceRawDataQuery  = "INSERT INTO trace_raw_data (trace_id, span_id, request_body, response_body) VALUES ($1, $2, $3, $4)"
)

var LogTag = "zk_trace_persistence_repo"

type TracePersistenceRepo interface {
	GetTraces(scenarioId string, offset, limit int) ([]model.TraceTable, error)
	GetTracesMetaData(traceId, spanId string, offset, limit int) ([]model.TraceMetaDataTable, error)
	GetTracesRawData(traceId, spanId string, offset, limit int) ([]model.TraceRawDataTable, error)
	SaveTraceList([]model.TraceDto) error
	SaveTrace(model.TraceDto) error
}

type tracePersistenceRepo struct {
}

func NewTracePersistenceRepo() TracePersistenceRepo {
	return &tracePersistenceRepo{}
}

func (z tracePersistenceRepo) GetTraces(scenarioId string, offset, limit int) ([]model.TraceTable, error) {
	c := zkUtilsPostgresConfig.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "pl",
		Password: "pl",
		Dbname:   "pl",
	}

	zkUtilsPostgres.Init(c)

	dbRepo := zkUtilsPostgres.NewZkPostgresRepo()
	db := dbRepo.CreateConnection()
	defer db.Close()

	rows, err, closeRow := dbRepo.GetAll(db, GetTraceQuery, []any{scenarioId, offset, limit})
	defer closeRow()

	if err != nil || rows == nil {
		fmt.Print("error")
	}

	var responseArr []model.TraceTable
	for rows.Next() {
		var rawData model.TraceTable
		err := rows.Scan(&rawData.ScenarioVersion, &rawData.TraceId)
		if err != nil {
			log.Fatal(err)
		}

		responseArr = append(responseArr, rawData)
	}

	return responseArr, nil
}

func (z tracePersistenceRepo) GetTracesMetaData(traceId, spanId string, offset, limit int) ([]model.TraceMetaDataTable, error) {
	c := zkUtilsPostgresConfig.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "pl",
		Password: "pl",
		Dbname:   "pl",
	}

	zkUtilsPostgres.Init(c)

	dbRepo := zkUtilsPostgres.NewZkPostgresRepo()
	db := dbRepo.CreateConnection()
	defer db.Close()

	rows, err, closeRow := dbRepo.GetAll(db, GetTraceMetaDataQuery, []any{traceId, spanId, offset, limit})
	defer closeRow()

	if err != nil || rows == nil {
		fmt.Print("error")
	}

	var responseArr []model.TraceMetaDataTable
	for rows.Next() {
		var rawData model.TraceMetaDataTable
		err := rows.Scan(&rawData.Protocol, &rawData.MetaData)
		if err != nil {
			log.Fatal(err)
		}

		responseArr = append(responseArr, rawData)
	}

	return responseArr, nil
}

func (z tracePersistenceRepo) GetTracesRawData(traceId, spanId string, offset, limit int) ([]model.TraceRawDataTable, error) {
	c := zkUtilsPostgresConfig.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "pl",
		Password: "pl",
		Dbname:   "pl",
	}

	zkUtilsPostgres.Init(c)

	dbRepo := zkUtilsPostgres.NewZkPostgresRepo()
	db := dbRepo.CreateConnection()
	defer db.Close()

	rows, err, closeRow := dbRepo.GetAll(db, GetTraceRawDataQuery, []any{traceId, spanId, offset, limit})
	defer closeRow()

	if err != nil || rows == nil {
		fmt.Print("error")
	}

	var responseArr []model.TraceRawDataTable
	for rows.Next() {
		var rawData model.TraceRawDataTable
		err := rows.Scan(&rawData.ResponsePayload, &rawData.ResponsePayload)
		if err != nil {
			log.Fatal(err)
		}

		responseArr = append(responseArr, rawData)
	}

	return responseArr, nil
}

func (z tracePersistenceRepo) SaveTraceList(traceDtoList []model.TraceDto) error {

	c := zkUtilsPostgresConfig.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "pl",
		Password: "pl",
		Dbname:   "pl",
	}

	zkUtilsPostgres.Init(c)

	dbRepo := zkUtilsPostgres.NewZkPostgresRepo()
	db := dbRepo.CreateConnection()
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		zklogger.Debug(LogTag, "unable to create txn, "+err.Error())
		return err
	}

	// TODO: understand this code below
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	//dbErr := {}.Build(zkErrors.ZK_ERROR_DB_ERROR, nil)

	traceTableData := make([]interfaces.DbArgs, 0)
	traceTableMetaData := make([]interfaces.DbArgs, 0)
	traceTableRawData := make([]interfaces.DbArgs, 0)
	for _, v := range traceDtoList {
		traceTableData = append(traceTableData, model.GetTraceTableData(v))
		traceTableMetaData = append(traceTableMetaData, model.GetTraceTableMetaData(v))
		traceTableRawData = append(traceTableRawData, model.GetTraceTableRawData(v))
	}

	err = dbRepo.BulkInsert(tx, TraceTablePostgres, []string{ScenarioId, ScenarioVersion, TraceId}, traceTableData)
	if err != nil {
		return err
	}

	err = dbRepo.BulkInsert(tx, TraceMetaDataTablePostgres, []string{TraceId, SpanId, MetaData, Protocol}, traceTableMetaData)
	if err != nil {
		return err
	}

	err = dbRepo.BulkInsert(tx, TraceRawDataTablePostgres, []string{TraceId, SpanId, RequestPayload, ResponsePayload}, traceTableRawData)
	if err != nil {
		return err
	}

	return err
}

func (z tracePersistenceRepo) SaveTrace(traceDto model.TraceDto) error {
	c := zkUtilsPostgresConfig.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "pl",
		Password: "pl",
		Dbname:   "pl",
	}

	zkUtilsPostgres.Init(c)

	dbRepo := zkUtilsPostgres.NewZkPostgresRepo()
	db := dbRepo.CreateConnection()
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)

	// TODO: understand this code below
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	if err != nil {
		return err
	}

	err = dbRepo.InsertInTransaction(tx, InsertTraceQuery, []any{traceDto.ScenarioId, traceDto.ScenarioVersion, traceDto.TraceId})
	if err != nil {
		return err
	}
	err = dbRepo.InsertInTransaction(tx, InsertTraceMetaDataQuery, []any{traceDto.TraceId, traceDto.SpanId, traceDto.MetaData, traceDto.Protocol})
	if err != nil {
		return err
	}
	err = dbRepo.InsertInTransaction(tx, InsertTraceRawDataQuery, []any{traceDto.TraceId, traceDto.SpanId, traceDto.RequestPayload, traceDto.RequestPayload})
	if err != nil {
		return err
	}

	return nil
}
