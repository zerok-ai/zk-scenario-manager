package main

import (
	"scenario-manager/internal/config"
	"scenario-manager/internal/filters"
	"scenario-manager/internal/tracePersistence"
	"scenario-manager/internal/tracePersistence/handler"
	"scenario-manager/internal/tracePersistence/repository"
	"scenario-manager/internal/tracePersistence/service"

	"github.com/kataras/iris/v12"
	zkConfig "github.com/zerok-ai/zk-utils-go/config"
	zkHttpConfig "github.com/zerok-ai/zk-utils-go/http/config"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkPostgres "github.com/zerok-ai/zk-utils-go/storage/sqlDB/postgres"
)

var LogTag = "main"

func main() {

	// read configuration from the file and environment variables
	var cfg config.AppConfigs
	if err := zkConfig.ProcessArgs[config.AppConfigs](&cfg); err != nil {
		panic(err)
	}

	zkLogger.Info(LogTag, "")
	zkLogger.Info(LogTag, "********* Initializing Application *********")
	zkHttpConfig.Init(cfg.Http.Debug)
	zkLogger.Init(cfg.LogsConfig)
	zkPostgresRepo, err := zkPostgres.NewZkPostgresRepo(cfg.Postgres)
	if err != nil {
		panic(err)
	}

	zkLogger.Debug(LogTag, "Parsed Configuration", cfg)

	tpr := repository.NewTracePersistenceRepo(zkPostgresRepo)
	tps := service.NewScenarioPersistenceService(tpr)
	tph := handler.NewTracePersistenceHandler(tps)

	scenarioManager, err := filters.NewScenarioManager(cfg, tps)
	if err != nil {
		panic(err)
	}

	scenarioManager.Init().ProcessScenarios()

	configurator := iris.WithConfiguration(iris.Configuration{
		DisablePathCorrection: true,
		LogLevel:              "debug",
	})

	if err = newApp(tph).Listen(":"+cfg.Server.Port, configurator); err != nil {
		panic(err)
	}
}

func newApp(persistenceHandler handler.TracePersistenceHandler) *iris.Application {
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
		ctx.WriteString("pong")
	}).Describe("healthcheck")

	v1 := app.Party("/v1")
	tracePersistence.Initialize(v1, persistenceHandler)

	return app
}
