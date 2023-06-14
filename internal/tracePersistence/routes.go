package tracePersistence

import (
	"github.com/kataras/iris/v12/core/router"
	"github.com/zerok-ai/zk-utils-go/storage/sqlDB"
	"scenario-manager/internal/tracePersistence/handler"
	"scenario-manager/internal/tracePersistence/repository"
	"scenario-manager/internal/tracePersistence/service"
)

func Initialize(app router.Party, db sqlDB.DatabaseRepo) {

	tpr := repository.NewTracePersistenceRepo(db)
	tps := service.NewScenarioPersistenceService(tpr)
	tph := handler.NewTracePersistenceHandler(tps)

	ruleEngineAPI := app.Party("/u/trace")
	{
		ruleEngineAPI.Get("/", tph.GetTraces)
		ruleEngineAPI.Get("/metadata", tph.GetTracesMetadata)
		ruleEngineAPI.Get("/raw-data", tph.GetTracesRawData)
	}
}
