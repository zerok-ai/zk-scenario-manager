package filters

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	store "github.com/zerok-ai/zk-utils-go/storage/redis"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"scenario-manager/internal/config"
	"time"
)

const (
	FilterProcessingTickInterval               = 10 * time.Minute
	TTLForTransientSets          time.Duration = 2 * time.Minute
)

type ScenarioManager struct {
	scenarioStore *store.VersionedStore[model.Scenario]

	traceStore  *TraceStore
	redisClient *redis.Client
}

func NewScenarioManager(cfg config.AppConfigs) (*ScenarioManager, error) {
	vs, err := store.GetVersionedStore(cfg.Redis, "scenarios", true, model.Scenario{})
	if err != nil {
		return nil, err
	}

	ts := GetTraceStore(cfg.Redis, TTLForTransientSets)

	fp := ScenarioManager{
		scenarioStore: vs,
		traceStore:    ts,
	}
	return &fp, nil
}

func (f ScenarioManager) Init() {
	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", FilterProcessingTickInterval, f.processFilters)
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
		set, err := TraceEvaluator{scenario, f.traceStore, namesOfAllSets}.EvalScenario()
		if err != nil {
			continue
		}
	}

	//TODO	 push data to scenario collector
}
