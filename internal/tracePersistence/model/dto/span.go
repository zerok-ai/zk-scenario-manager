package dto

import (
	"github.com/lib/pq"
)

type SpanTableDto struct {
	TraceId        string         `json:"trace_id"`
	SpanId         string         `json:"span_id"`
	ParentSpanId   string         `json:"parent_span_id"`
	Source         string         `json:"source"`
	Destination    string         `json:"destination"`
	WorkloadIdList pq.StringArray `json:"workload_id_list"`
	Metadata       string         `json:"metadata"`
	LatencyMs      float32        `json:"latency_ms"`
	Protocol       string         `json:"protocol"`
}

func (t SpanTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.SpanId, t.ParentSpanId, t.Source, t.Destination, t.WorkloadIdList, t.Metadata, t.LatencyMs, t.Protocol}
}
