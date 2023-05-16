package main

import (
	config "scenario-manager/internal/config"
	"scenario-manager/internal/filters"
)

func main() {
	// read configuration from the file and environment variables
	var cfg config.AppConfigs
	if err := config.ProcessArgs(&cfg); err != nil {
		panic(err)
	}

	//start business logic
	if err := filters.Start(cfg); err != nil {
		panic(err)
	}
}
