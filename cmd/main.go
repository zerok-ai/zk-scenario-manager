package main

import (
	"github.com/kataras/iris/v12"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	zkConfig "github.com/zerok-ai/zk-utils-go/config"
	zkHttpConfig "github.com/zerok-ai/zk-utils-go/http/config"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"scenario-manager/config"
	sm "scenario-manager/internal/scenarioManager"
	"scenario-manager/internal/timedWorkers"
)

var LogTag = "main"

func main() {

	// read configuration from the file and environment variables
	var cfg config.AppConfigs
	if err := zkConfig.ProcessArgs[config.AppConfigs](&cfg); err != nil {
		panic(err)
	}

	zkLogger.Info(LogTag, "********* Initializing Application *********")
	zkHttpConfig.Init(cfg.Http.Debug)
	zkLogger.Init(cfg.LogsConfig)

	scenarioProcessor, err := sm.NewScenarioProcessor(cfg)
	if err != nil {
		panic(err)
	}
	defer scenarioProcessor.Close()

	// start OTel worker
	oTelWorker := sm.GetQueueWorkerOTel(cfg, scenarioProcessor.GetScenarioStore())
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

	//scraping metrics for prometheus
	app.Get("/metrics", iris.FromStd(promhttp.Handler()))

	return app
}
