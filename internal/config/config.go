package config

import (
	"flag"
	"fmt"
	"github.com/ilyakaznacheev/cleanenv"
	"os"
	config4 "scenario-manager/zk-utils-go/http/config"
	config3 "scenario-manager/zk-utils-go/logs/config"
	config2 "scenario-manager/zk-utils-go/redis/config"
)

type SuprSendConfig struct {
	WorkspaceKey    string `yaml:"workspaceKey" env:"SSW_KEY" env-description:"SuprSend Workspace Key"`
	WorkspaceSecret string `yaml:"workspaceSecret" env:"SSW_SECRET" env-description:"SuprSend Workspace Secret"`
}

type ServerConfig struct {
	Host string `yaml:"host" env:"SRV_HOST,HOST" env-description:"Server host" env-default:"localhost"`
	Port string `yaml:"port" env:"SRV_PORT,PORT" env-description:"Server port" env-default:"8080"`
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

// AppConfigs is an application configuration structure
type AppConfigs struct {
	Redis      config2.RedisConfig `yaml:"redis"`
	Server     ServerConfig        `yaml:"server"`
	AuthConfig AuthConfig          `yaml:"auth"`
	LogsConfig config3.LogsConfig  `yaml:"logs"`
	Http       config4.HttpConfig  `yaml:"http"`
	Pixie      PixieConfig         `yaml:"pixie"`
	Router     RouterConfigs       `yaml:"router"`
	Greeting   string              `env:"GREETING" env-description:"Greeting phrase" env-default:"Hello!"`
	SuprSend   SuprSendConfig      `yaml:"suprsend"`
}

// Args command-line parameters
type Args struct {
	ConfigPath string
}

// ProcessArgs processes and handles CLI arguments
func ProcessArgs(cfg interface{}) error {
	var a Args

	flagSet := flag.NewFlagSet("server", 1)
	flagSet.StringVar(&a.ConfigPath, "c", "config.yaml", "Path to configuration file")

	fu := flagSet.Usage
	flagSet.Usage = func() {
		fu()
		envHelp, _ := cleanenv.GetDescription(cfg, nil)
		if _, err := fmt.Fprintln(flagSet.Output()); err != nil {
			return
		}

		_, err := fmt.Fprintln(flagSet.Output(), envHelp)
		if err != nil {
			return
		}
	}

	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return err
	}

	return cleanenv.ReadConfig(a.ConfigPath, &cfg)
}
