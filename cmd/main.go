package main

import (
	"fmt"
	"scenario-manager/internal/config"
	"scenario-manager/internal/filters"
)

func start(cfg config.AppConfigs) {

	//start business logic
	done := make(chan bool)
	filters.NewScenarioManager(cfg).Init()

	// Block the main goroutine until termination signal is received
	<-done

	// Execution continues here when termination signal is received
	fmt.Println("Main function has terminated.")
}

func main() {
	fmt.Printf("Hello from zk-scenario-manager\n")

	// read configuration from the file and environment variables
	cfg, err := config.ProcessArgs()
	if err != nil {
		panic(err)
	}

	//populatedata.PopulateScenarios(cfg.Redis)
	//populatedata.PopulateTraces(cfg.Redis)

	start(*cfg)

}
