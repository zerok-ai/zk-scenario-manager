package filters

import (
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"github.com/zerok-ai/zk-utils-go/storage"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"scenario-manager/internal/config"
)

type FilterProcessor struct {
	versionedStore *storage.VersionedStore[model.Scenario]

	traceStore  *TraceStore
	redisClient *redis.Client
}

func NewFilterProcessor(appConfig config.AppConfigs, vs *storage.VersionedStore[model.Scenario]) (*FilterProcessor, error) {
	fp := FilterProcessor{
		versionedStore: vs,
	}
	fp.init()
	return &fp, nil
}

func (f FilterProcessor) init() {
	// trigger recurring processing of trace data against filters
	tickerTask := ticker.GetNewTickerTask("filter-processor", filterProcessingTickInterval, f.processFilters)
	tickerTask.Start()
	f.processFilters()
}

func (f FilterProcessor) processFilters() {

}
