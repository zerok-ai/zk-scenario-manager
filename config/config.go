package config

import (
	zkHttpConfig "github.com/zerok-ai/zk-utils-go/http/config"
	zkLogsConfig "github.com/zerok-ai/zk-utils-go/logs/config"
	storage "github.com/zerok-ai/zk-utils-go/storage/redis/config"
)

type ServerConfig struct {
	Host string `yaml:"host" env:"SRV_HOST,HOST" env-description:"Server host" env-default:"localhost"`
	Port string `yaml:"port" env:"SRV_PORT,PORT" env-description:"Server port" env-default:"80"`
}

type RouterConfigs struct {
	ZkApiServer string `yaml:"zkApiServer" env-description:"Auth token expiry in seconds"`
	ZkDashboard string `yaml:"zkDashboard" env-description:"Zk dashboard url"`
}

type ScenariosConfig struct {
	ProcessingIntervalInSeconds int `yaml:"processingIntervalInSeconds" env-description:"Time interval in seconds for processing scenarios"`
}

type WorkLoadConfig struct {
	WorkLoadTtl            int `yaml:"ttl"`
	WorkLoadTickerDuration int `yaml:"tickerDuration"`
}

type Exporter struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

// AppConfigs is an application configuration structure
type AppConfigs struct {
	Redis                storage.RedisConfig     `yaml:"redis"`
	Server               ServerConfig            `yaml:"server"`
	LogsConfig           zkLogsConfig.LogsConfig `yaml:"logs"`
	Http                 zkHttpConfig.HttpConfig `yaml:"http"`
	ScenarioConfig       ScenariosConfig         `yaml:"scenarioConfig"`
	Workload             WorkLoadConfig          `yaml:"workLoad"`
	Exporter             Exporter                `yaml:"exporter"`
	OtelQueueWorkerCount int                     `yaml:"otelQueueWorkerCount"`
}
