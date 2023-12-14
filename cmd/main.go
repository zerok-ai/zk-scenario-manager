package main

import (
	"fmt"
	"github.com/kataras/iris/v12"
	zkConfig "github.com/zerok-ai/zk-utils-go/config"
	zkHttpConfig "github.com/zerok-ai/zk-utils-go/http/config"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"net/http"
	"os"
	"scenario-manager/config"
	sm "scenario-manager/internal/scenarioManager"
	"scenario-manager/internal/timedWorkers"
	"strconv"
	"time"
)

var LogTag = "main"

func main() {

	obfuscateEnv := "false" //os.Getenv("OBFUSCATE")
	zkLogger.Info(LogTag, "OBFUSCATE: ", obfuscateEnv)
	shouldObfuscate, err := strconv.ParseBool(obfuscateEnv)
	if err != nil {
		panic(err)
	}

	if shouldObfuscate {
		waitForPresidio()
	} else {
		zkLogger.Info(LogTag, "Not obfuscating, skipping Presidio healthcheck")
	}

	// read configuration from the file and environment variables
	var cfg config.AppConfigs
	if err := zkConfig.ProcessArgs[config.AppConfigs](&cfg); err != nil {
		panic(err)
	}

	zkLogger.Info(LogTag, "")
	zkLogger.Info(LogTag, "********* Initializing Application *********")
	zkHttpConfig.Init(cfg.Http.Debug)
	zkLogger.Init(cfg.LogsConfig)

	scenarioProcessor, err := sm.NewScenarioProcessor(cfg)
	if err != nil {
		panic(err)
	}
	defer scenarioProcessor.Close()

	// start OTel worker
	oTelWorker := sm.GetQueueWorkerOTel(cfg)
	defer oTelWorker.Close()

	configurator := iris.WithConfiguration(iris.Configuration{
		DisablePathCorrection: true,
		LogLevel:              cfg.LogsConfig.Level,
	})

	workloadKeyHandler, err := timedWorkers.NewWorkloadKeyHandler(&cfg, scenarioProcessor.GetScenarioStore())
	if err != nil {
		zkLogger.Info(LogTag, "Failed to start workloadKeyHandler")
	}
	defer workloadKeyHandler.Close()

	if err = newApp().Listen(":"+cfg.Server.Port, configurator); err != nil {
		panic(err)
	}
}

func waitForPresidio() {
	apiURL := "http://localhost:9103/healthz"
	maxAttempts := 10
	interval := 10 * time.Second

	fmt.Printf("Waiting for API at %s to return a 200 status...\n", apiURL)

	for i := 0; i < maxAttempts; i++ {
		resp, err := http.Get(apiURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			zkLogger.Info(LogTag, "Presidio is up and healthy status 200")
			return
		}

		if err != nil {
			zkLogger.Error(LogTag, "Presidio Error: %v\n", err)
		} else {
			zkLogger.Info(LogTag, "Presidio Status code: %d\n", resp.StatusCode)
		}

		time.Sleep(interval)
	}

	fmt.Println("API did not return a 200 status after multiple attempts")
	os.Exit(1)
}

func newApp() *iris.Application {
	app := iris.Default()

	crs := func(ctx iris.Context) {
		ctx.Header("Access-Control-Allow-Credentials", "true")

		if ctx.Method() == iris.MethodOptions {
			ctx.Header("Access-Control-Methods",
				"POST, PUT, PATCH, DELETE")

			ctx.Header("Access-Control-Allow-Headers",
				"Access-Control-Allow-Origin,Content-Type")

			ctx.Header("Access-Control-Max-Age",
				"86400")

			ctx.StatusCode(iris.StatusNoContent)
			return
		}

		ctx.Next()
	}

	app.UseRouter(crs)
	app.AllowMethods(iris.MethodOptions)

	app.Get("/healthz", func(ctx iris.Context) {
		ctx.StatusCode(iris.StatusOK)
		ctx.WriteString("pong")
	}).Describe("healthcheck")

	return app
}
