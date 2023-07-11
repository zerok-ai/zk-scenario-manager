package dto

import (
	"github.com/lib/pq"
	"time"
)

type SpanTableDto struct {
	TraceId        string         `json:"trace_id"`
	SpanId         string         `json:"span_id"`
	ParentSpanId   string         `json:"parent_span_id"`
	Source         string         `json:"source"`
	Destination    string         `json:"destination"`
	WorkloadIdList pq.StringArray `json:"workload_id_list"`
	Status         string         `json:"status"`
	Metadata       string         `json:"metadata"`
	LatencyMs      *float32       `json:"latency_ms"`
	Protocol       string         `json:"protocol"`
	IssueHashList  pq.StringArray `json:"issue_hash_list"`
	Time           time.Time      `json:"time"`
}

func (t SpanTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.SpanId, t.ParentSpanId, t.Source, t.Destination, t.WorkloadIdList, t.Status, t.Metadata, t.LatencyMs, t.Protocol, t.IssueHashList, t.Time}
}
