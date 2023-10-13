package filters

import (
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
)

func (scenarioManager *ScenarioManager) collectHTTPRawData(traceIds []string, startTime string) []models.HttpRawDataModel {
	rawData, err := scenarioManager.traceRawDataCollector.GetHTTPRawData(traceIds, startTime)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for http traces ", traceIds, err)
		return make([]models.HttpRawDataModel, 0)
	}
	return rawData.Results
}

func (scenarioManager *ScenarioManager) collectMySQLRawData(timeRange string, traceIds []string) []models.MySQLRawDataModel {
	rawData, err := scenarioManager.traceRawDataCollector.GetMySQLRawData(traceIds, timeRange)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for mysql traces ", traceIds, err)
		return make([]models.MySQLRawDataModel, 0)
	}
	return rawData.Results
}

func (scenarioManager *ScenarioManager) collectPostgresRawData(timeRange string, traceIds []string) []models.PgSQLRawDataModel {
	rawData, err := scenarioManager.traceRawDataCollector.GetPgSQLRawData(traceIds, timeRange)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error getting raw spans for postgres traces ", traceIds, err)
		return make([]models.PgSQLRawDataModel, 0)
	}
	return rawData.Results
}
