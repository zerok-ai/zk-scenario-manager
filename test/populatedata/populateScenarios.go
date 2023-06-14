package populatedata

import (
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	storage "github.com/zerok-ai/zk-utils-go/storage/redis"
)

type ScenarioPopulator struct {
	versionedStore *storage.VersionedStore[model.Scenario]
}

func GetScenarioPopulator(dbname string, redisConfig *storage.RedisConfig) *ScenarioPopulator {
	vs, err := storage.GetVersionedStore(redisConfig, dbname, false, model.Scenario{})
	if err != nil {
		return nil
	}
	sp := ScenarioPopulator{
		versionedStore: vs,
	}
	return &sp
}

func (sp ScenarioPopulator) AddScenario(scenario model.Scenario) error {
	err := sp.versionedStore.SetValue(scenario.ScenarioId, scenario)
	return err
}
