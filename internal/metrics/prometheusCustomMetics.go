package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (

	// total traces with no root span found for each scenario
	RootSpanNotFoundTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zerok_sm_root_spans_not_found_total",
			Help: "No Root Span Found Error",
		},
		[]string{"scenario", "trace"}, // Multiple labels
	)

	//rate limited total incidents for each scenario
	RateLimitedTotalIncidentsPerScenario = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zerok_sm_scenario_traces_rate_limited_total",
			Help: "Total rate limited incidents for each scenario",
		},
		[]string{"scenario"},
	)

	//span count mismatch for incident
	SpanCountMismatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zerok_sm_traces_span_count_mismatch_total",
			Help: "Total span count mismatch for traces",
		},
		[]string{"scenario", "trace"},
	)

	//total traces received for scenario to process metric
	TotalTracesReceivedForScenarioToProcess = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zerok_sm_traces_received_for_scenario_to_process_total",
			Help: "Total traces received for scenario to process",
		},
		[]string{"scenario"},
	)

	//total traces processed by sm and pushed to pipeline metric
	TotalTracesProcessedForScenario = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zerok_sm_traces_processed_scenario_total",
			Help: "Total traces processed by scenario manager for scenario",
		},
		[]string{"scenario"},
	)

	//total spans sent to collector
	TotalSpansSentToCollector = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "zerok_sm_spans_sent_to_collector_total",
			Help: "Total spans sent to collector",
		},
	)

	// total spans processed metric for each scenario
	TotalSpansProcessedForScenario = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zerok_sm_spans_processed_scenario_total",
			Help: "Total spans processed by scenario manager for scenario",
		},
		[]string{"scenario"},
	)

	//total export calls failed for scenario
	TotalExportDataFailedForScenario = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zerok_sm_export_data_failed_scenario_total",
			Help: "Total export data failed for scenario",
		},
		[]string{"scenario"},
	)

	//total span data fetch calls success for scenario
	TotalSpanDataFetchCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_span_data_fetch_calls",
		Help: "Total spans data fetch calls to receiver by scenario manager",
	},
		[]string{"nodeIp"})

	//total span data fetch calls errors for scenario
	TotalSpanDataFetchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_span_data_fetch_errors",
		Help: "Total spans data fetch errors to receiver by scenario manager",
	},
		[]string{"nodeIp"})

	//total span data fetch calls success for scenario
	TotalSpanDataFetchSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_span_data_fetch_success",
		Help: "Total spans data fetch success to receiver by scenario manager",
	},
		[]string{"nodeIp"})

	//total traces span data requested from receiver
	TotalTracesSpanDataRequestedFromReceiver = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_traces_span_data_requested_from_receiver",
		Help: "Total traces span data requested from receiver by scenario manager",
	},
		[]string{"nodeIp"})

	TimeTakenByOtelWorkerToProcessATrace = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "zerok_sm_time_taken_by_otel_worker_to_process_a_trace",
			Help: "Time taken by otel worker to process a trace",
		},
		[]string{"scenario"},
	)

	TimeTakenToProcessEachScenarioByQueue1Worker = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "zerok_sm_time_taken_to_process_each_scenario_by_queue1_worker",
			Help: "Time taken to process each scenario by queue1 worker",
		},
		[]string{"scenario"},
	)
)
