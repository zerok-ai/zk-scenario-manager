package tracePersistence

import (
	"github.com/kataras/iris/v12/core/router"
	"scenario-manager/internal/tracePersistence/handler"
)

func Initialize(app router.Party, tph handler.TracePersistenceHandler) {

	ruleEngineAPI := app.Party("/u/trace")
	{
		ruleEngineAPI.Get("/incident", tph.GetIncidents)
		ruleEngineAPI.Get("/", tph.GetTraces)
		ruleEngineAPI.Get("/metadata", tph.GetTracesMetadata)
		ruleEngineAPI.Get("/raw-data", tph.GetTracesRawData)
	}
}
