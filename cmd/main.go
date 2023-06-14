package main

import (
	"fmt"
	"scenario-manager/internal/config"
	"scenario-manager/internal/filters"
	"scenario-manager/internal/tracePersistence"

	"github.com/kataras/iris/v12"
	"github.com/zerok-ai/zk-utils-go/storage/sqlDB"

	zkConfig "github.com/zerok-ai/zk-utils-go/config"
	zkHttpConfig "github.com/zerok-ai/zk-utils-go/http/config"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	zkPostgres "github.com/zerok-ai/zk-utils-go/storage/sqlDB/postgres"
)

var LogTag = "main"

func start(cfg config.AppConfigs) {

	//start business logic
	done := make(chan bool)
	filterProcessor, err := filters.NewScenarioManager(cfg)
	if err != nil {
		panic(err)
	}
	filterProcessor.Init()

	// Block the main goroutine until termination signal is received
	<-done

	// Execution continues here when termination signal is received
	fmt.Println("Main function has terminated.")
}

func main() {
	fmt.Printf("Hello from zk-scenario-manager\n")

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
		return
	}

	zkLogger.Debug(LogTag, "Parsed Configuration", cfg)

	//start business logic
	if err := filters.Start(cfg); err != nil {
		panic(err)
	}

	filterProcessor, err := filters.NewScenarioManager(cfg)
	if err != nil {
		panic(err)
	}
	filterProcessor.Init()

	app := newApp(zkPostgresRepo)

	config := iris.WithConfiguration(iris.Configuration{
		DisablePathCorrection: true,
		LogLevel:              "debug",
	})
	app.Listen(":"+cfg.Server.Port, config)
}

func newApp(db sqlDB.DatabaseRepo) *iris.Application {
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
	tracePersistence.Initialize(v1, db)

	return app
}
