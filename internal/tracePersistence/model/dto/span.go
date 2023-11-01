package dto

import (
	"github.com/lib/pq"
	zkCommon "github.com/zerok-ai/zk-utils-go/common"
	"time"
)

type SpanTableDto struct {
	TraceID             string         `json:"trace_id"`
	ParentSpanID        string         `json:"parent_span_id"`
	SpanID              string         `json:"span_id"`
	SpanName            string         `json:"span_name"`
	IsRoot              bool           `json:"is_root"`
	Kind                string         `json:"kind"`
	StartTime           time.Time      `json:"start_time"`
	Latency             uint64         `json:"latency"`
	Source              string         `json:"source"`
	Destination         string         `json:"destination"`
	WorkloadIDList      pq.StringArray `json:"workload_id_list"`
	Protocol            string         `json:"protocol"`
	IssueHashList       pq.StringArray `json:"issue_hash_list"`
	RequestPayloadSize  uint64         `json:"request_payload_size"`
	ResponsePayloadSize uint64         `json:"response_payload_size"`
	Method              string         `json:"method"`
	Route               string         `json:"route"`
	Scheme              string         `json:"scheme"`
	Path                string         `json:"path"`
	Query               string         `json:"query"`
	Status              *float64       `json:"status"`
	Username            string         `json:"username"`
	SourceIP            string         `json:"source_ip"`
	DestinationIP       string         `json:"destination_ip"`
	ServiceName         string         `json:"service_name"`
	Errors              string         `json:"errors"`

	SpanAttributes     zkCommon.GenericMap `json:"span_attributes"`
	ResourceAttributes zkCommon.GenericMap `json:"resource_attributes"`
	ScopeAttributes    zkCommon.GenericMap `json:"scope_attributes"`
	HasRawData         bool                `json:"has_raw_data"`
}

func (t SpanTableDto) GetAllColumns() []any {
	return []any{t.TraceID, t.ParentSpanID, t.SpanID, t.SpanName, t.IsRoot, t.Kind, t.StartTime, t.Latency, t.Source,
		t.Destination, t.WorkloadIDList, t.Protocol, t.IssueHashList, t.RequestPayloadSize, t.ResponsePayloadSize,
		t.Method, t.Route, t.Scheme, t.Path, t.Query, t.Status, t.Username, t.SourceIP, t.DestinationIP, t.ServiceName,
		t.Errors, t.SpanAttributes, t.ResourceAttributes, t.ScopeAttributes, t.HasRawData}
}
