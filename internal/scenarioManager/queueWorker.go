package scenarioManager

import (
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	typedef "scenario-manager/internal"
)

const (
	oTelProducerName = "oTel_Producer"
	oTelConsumerName = "oTel_Consumer"

	ebpfProducerName = "eBPF_Producer"
	ebpfConsumerName = "eBPF_Consumer"
)

type OTELTraceMessage struct {
	Scenario   model.Scenario     `json:"scenario"`
	Traces     []typedef.TTraceid `json:"traces"`
	ProducerId string             `json:"producer_id"`
}

type EBPFTraceMessage struct {
	Scenario   model.Scenario  `json:"scenario"`
	Traces     []TraceFromOTel `json:"traces"`
	ProducerId string          `json:"producer_id"`
}

type TraceFromOTel struct {
	TraceId        string `json:"trace_id"`
	RootSpanId     string `json:"root_span_id"`
	RootSpanParent string `json:"root_span_parent"`
	RootSpanKind   string `json:"root_span_kind"`
}
