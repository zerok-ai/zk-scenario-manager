package config

import (
	zkHttpConfig "github.com/zerok-ai/zk-utils-go/http/config"
	zkLogsConfig "github.com/zerok-ai/zk-utils-go/logs/config"
	storage "github.com/zerok-ai/zk-utils-go/storage/redis/config"
	zkPostgresConfig "github.com/zerok-ai/zk-utils-go/storage/sqlDB/postgres/config"
)

type SuprSendConfig struct {
	WorkspaceKey    string `yaml:"workspaceKey" env:"SSW_KEY" env-description:"SuprSend Workspace Key"`
	WorkspaceSecret string `yaml:"workspaceSecret" env:"SSW_SECRET" env-description:"SuprSend Workspace Secret"`
}

type ServerConfig struct {
	Host string `yaml:"host" env:"SRV_HOST,HOST" env-description:"Server host" env-default:"localhost"`
	Port string `yaml:"port" env:"SRV_PORT,PORT" env-description:"Server port" env-default:"80"`
}

type AuthConfig struct {
	Expiry     int    `yaml:"expiry" env-description:"Auth token expiry in seconds"`
	AdminEmail string `yaml:"adminEmail"`
}

type PixieConfig struct {
	Config string `yaml:"config" env-description:"Auth token expiry in seconds"`
	Local  bool   `yaml:"local" env-description:"Auth token expiry in seconds"`
}

type RouterConfigs struct {
	ZkApiServer string `yaml:"zkApiServer" env-description:"Auth token expiry in seconds"`
	ZkDashboard string `yaml:"zkDashboard" env-description:"Zk dashboard url"`
}

type ScenariosConfig struct {
	RedisRuleSetCount           int `yaml:"redisRuleSetCount" env-description:"No of possible sets for a rule in redis"`
	ProcessingIntervalInSeconds int `yaml:"processingIntervalInSeconds" env-description:"Time interval in seconds for processing scenarios"`

	VZCloudAddr  string `yaml:"vzCloudAddr" env-description:"vz cloud address"`
	VZClusterId  string `yaml:"vzClusterId" env-description:"vz cluster id"`
	VZClusterKey string `yaml:"vzClusterKey" env-description:"vz cluster key"`
}

// AppConfigs is an application configuration structure
type AppConfigs struct {
	Redis          *storage.RedisConfig            `yaml:"redis"`
	Postgres       zkPostgresConfig.PostgresConfig `yaml:"postgres"`
	Server         ServerConfig                    `yaml:"server"`
	AuthConfig     AuthConfig                      `yaml:"auth"`
	LogsConfig     zkLogsConfig.LogsConfig         `yaml:"logs"`
	Http           zkHttpConfig.HttpConfig         `yaml:"http"`
	Pixie          PixieConfig                     `yaml:"pixie"`
	Router         RouterConfigs                   `yaml:"router"`
	Greeting       string                          `env:"GREETING" env-description:"Greeting phrase" env-default:"Hello!"`
	SuprSend       SuprSendConfig                  `yaml:"suprsend"`
	ScenarioConfig ScenariosConfig                 `yaml:"scenarioConfig"`
}
