package timedWorkers

import (
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/models"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"scenario-manager/config"
	"scenario-manager/internal/stores"
	"time"
)

const (
	tickerInterval   = 1 * time.Minute
	upid_ticker_name = "upid_ticker"
)

var LoggerTag = "UPIDToServiceMapWorker"

type UPIDToServiceMapWorker struct {
	cfg             config.AppConfigs
	tickerTask      *ticker.TickerTask
	podDetailsStore *stores.PodDetailsStore
}

func (tw *UPIDToServiceMapWorker) Close() {
	tw.tickerTask.Stop()
}

func (tw *UPIDToServiceMapWorker) getUPIDChangesMap(upidToServiceMap []models.UPIDToServiceMapModel, existingUPIDToServiceMap map[string]string) map[string]string {
	upidChangesMap := make(map[string]string)
	for _, item := range upidToServiceMap {
		if value, ok := existingUPIDToServiceMap[item.UPID]; ok && value == item.Service {
			continue
		}
		upidChangesMap[item.UPID] = item.Service
	}
	return upidChangesMap
}
