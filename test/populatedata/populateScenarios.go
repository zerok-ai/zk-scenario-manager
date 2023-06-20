package populatedata

import (
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"github.com/zerok-ai/zk-utils-go/storage/redis"
	redisConfig "github.com/zerok-ai/zk-utils-go/storage/redis/config"
)

type ScenarioPopulator struct {
	versionedStore *redis.VersionedStore[model.Scenario]
}

func GetScenarioPopulator(dbname string, redisConfig redisConfig.RedisConfig) *ScenarioPopulator {
	vs := redis.GetVersionedStore(redisConfig, dbname, false, model.Scenario{})
	sp := ScenarioPopulator{
		versionedStore: vs,
	}
	return &sp
}

func (sp ScenarioPopulator) AddScenario(scenario model.Scenario) error {
	err := sp.versionedStore.SetValue(scenario.ScenarioId, scenario)
	return err
}
