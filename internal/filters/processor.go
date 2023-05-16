package filters

import (
	"time"

	"scenario-manager/internal/config"
	zktime "scenario-manager/zk-utils-go/time"
)

const (
	filterPullTickInterval       time.Duration = 10 * time.Minute
	filterProcessingTickInterval time.Duration = 10 * time.Minute
)

var (
	filterProcessor *FilterProcessor

	//	tickers
	tickerFilterPull     *time.Ticker
	tickerTraceProcessor *time.Ticker
)

func Start(cfg config.AppConfigs) error {

	// initialize the image store
	filterProcessor := NewFilterProcessor(cfg)

	// trigger recurring filter pull
	tickerFilterPull = time.NewTicker(filterPullTickInterval)
	zktime.RunTaskOnTicks(tickerFilterPull, filterProcessor.FetchNewFilters)

	// trigger recurring processing of trace data against filters
	tickerTraceProcessor = time.NewTicker(filterProcessingTickInterval)
	zktime.RunTaskOnTicks(tickerTraceProcessor, processFilters)

	return nil
}

func processFilters() {

}
