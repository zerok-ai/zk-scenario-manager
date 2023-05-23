package config

import (
	"flag"
	"fmt"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/zerok-ai/zk-utils-go/storage/model"
	"os"
)

// AppConfigs is an application configuration structure
type AppConfigs struct {
	Redis *model.RedisConfig `yaml:"redis"`
}

// Args command-line parameters
type Args struct {
	ConfigPath string
}

// ProcessArgs processes and handles CLI arguments
func ProcessArgs() (*AppConfigs, error) {
	var a Args

	var cfg AppConfigs

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
		return nil, err
	}

	err := cleanenv.ReadConfig(a.ConfigPath, &cfg)
	return &cfg, err
}
