package scenarioManager

import (
	"time"
)

const (
	LoggerTag = "scenario-processor"

	MAX_SUFFIX_COUNT = 60

	RateLimitLoggerTag = "scenario-manager-rate-limit"
	INTERNAL           = "INTERNAL"
	CLIENT             = "CLIENT"
	SERVER             = "SERVER"

	ScenarioRefreshInterval  = 20 * time.Minute
	TTLForTransientSets      = 30 * time.Second
	TTLForScenarioSets       = 15 * time.Minute
	scenarioProcessingTime   = 5 * time.Minute
	timeRangeForRawDataQuery = "-15m" // -5m, -10m, -1h etc

	/*
		Workload id sets are created by OTLP receiver
		-----------
		<workloadId>_<number>
		Ex. cc13b872-b3d9-52ed-aa64-210114c0cbef_1
		-----------

		OTel
		-----------
		OTel_P_<scenarioID>_<time>
		OTel_P_All_<scenarioID>_<time>
	*/
	SetPrefixOTelProcessed          = "OTel_P"
	SetPrefixOTelProcessedAggregate = "OTel_P_All"

	/*
		filter evaluation
		-----------
		a) union set for each workload
		workload_<scenarioID>_<workloadId>_<time_nano>

		b) union set for each filter
		filter_<scenarioID>_<time_nano>
	*/
	SetPrefixWorkload     = "workload"
	SetPrefixFilterResult = "filter"

	/*
		ebpf
		-----------
		ebpf_temp_<workerID>_<time>
		ebpf_All_P_<workerID>
		ebpf_P_<workerID>_<time>
	*/
	SetPrefixEBPFTemp               = "ebpf_temp"
	SetPrefixEBPFProcessed          = "ebpf_P"
	SetPrefixEBPFProcessedAggregate = "ebpf_All_P"
)
