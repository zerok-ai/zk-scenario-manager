package filters

import (
	"time"

	"scenario-manager/internal/config"
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
	//filterProcessor := NewFilterProcessor(cfg)
	//
	//// trigger recurring filter pull
	//tickerFilterPull = time.NewTicker(filterPullTickInterval)
	//zkTicker.RunTaskOnTicks(tickerFilterPull, filterProcessor.FetchNewFilters)
	//
	//// trigger recurring processing of trace data against filters
	//tickerTraceProcessor = time.NewTicker(filterProcessingTickInterval)
	//zkTicker.RunTaskOnTicks(tickerTraceProcessor, processFilters)

	return nil
}

func processFilters() {

}
