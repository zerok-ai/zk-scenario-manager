package timedWorkers

import (
	"github.com/pkg/errors"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	logger "github.com/zerok-ai/zk-utils-go/logs"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"scenario-manager/config"
	"scenario-manager/internal/filters"
	"scenario-manager/internal/stores"
	"time"
)

const (
	tickerInterval   = 1 * time.Minute
	upid_ticker_name = "upid_ticker"
)

type UPIDToServiceMapWorker struct {
	cfg             config.AppConfigs
	vzReader        *vzReader.VzReader
	tickerTask      *ticker.TickerTask
	podDetailsStore *stores.PodDetailsStore
}

func NewUPIDToServiceMapWorker(cfg config.AppConfigs) (*UPIDToServiceMapWorker, error) {
	tw := UPIDToServiceMapWorker{
		cfg: cfg,
	}

	reader, err := filters.GetNewVZReader(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get new VZ reader")
	}
	tw.vzReader = reader
	tw.tickerTask = ticker.GetNewTickerTask(upid_ticker_name, tickerInterval, tw.PopulateUPIDToServiceMap)
	tw.tickerTask.Start()
	tw.podDetailsStore = stores.GetPodDetailsStore(cfg.Redis)
	return &tw, nil
}

func (tw *UPIDToServiceMapWorker) Close() {
	tw.tickerTask.Stop()
}

func (tw *UPIDToServiceMapWorker) PopulateUPIDToServiceMap() {
	upidToServiceMapResponse, err := tw.vzReader.GetUPIDToServiceMap()
	if err != nil {
		return
	}

	existingUPIDToServiceMap, _ := tw.podDetailsStore.GetUPIDToServiceMap()
	upidChangesMap := tw.getUPIDChangesMap(upidToServiceMapResponse.Results, existingUPIDToServiceMap)
	if upidChangesMap == nil {
		return
	}

	tw.podDetailsStore.UpdateUPIDToServiceMap(upidChangesMap)
}

func (tw *UPIDToServiceMapWorker) getUPIDChangesMap(upidToServiceMap []models.UPIDToServiceMapModel, existingUPIDToServiceMap map[string]string) map[string]string {
	upidChangesMap := make(map[string]string)
	for _, item := range upidToServiceMap {
		if value, ok := existingUPIDToServiceMap[item.UPID]; ok && value == item.Service {
			continue
		}
		upidChangesMap[item.UPID] = item.Service
	}
	logger.Debug("UPID changes map: %v", upidChangesMap)
	return upidChangesMap
}
