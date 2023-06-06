package filters

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"github.com/zerok-ai/zk-utils-go/storage"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"scenario-manager/internal/config"
	"time"
)

const filterProcessingTickInterval = 10 * time.Minute

type ScenarioManager struct {
	scenarioStore *storage.VersionedStore[model.Scenario]

	traceStore  *TraceStore
	redisClient *redis.Client
}

func NewScenarioManager(cfg config.AppConfigs) (*ScenarioManager, error) {
	vs, err := storage.GetVersionedStore(cfg.Redis, "scenarios", true, model.Scenario{})
	if err != nil {
		return nil, err
	}

	ts := GetTraceStore(cfg.Redis)

	fp := ScenarioManager{
		scenarioStore: vs,
		traceStore:    ts,
	}
	return &fp, nil
}

func (f ScenarioManager) Init() {
	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", filterProcessingTickInterval, f.processFilters)
	tickerTask.Start()
	f.processFilters()
}

func (f ScenarioManager) processFilters() {
	scenarios := f.scenarioStore.GetAllValues()
	namesOfAllSets, err := f.traceStore.GetAllKeys()
	if err != nil {
		fmt.Println("Error getting all keys from traceStore ", err)
		return
	}
	for _, scenario := range scenarios {
		_, err := TraceEvaluator{scenario, f.traceStore, namesOfAllSets}.EvalScenario()
		if err != nil {
			continue
		}
	}
}
