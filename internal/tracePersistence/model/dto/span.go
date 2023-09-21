package dto

import (
	"github.com/lib/pq"
	"time"
)

type SpanTableDto struct {
	TraceID             string         `json:"trace_id"`
	ParentSpanID        string         `json:"parent_span_id"`
	SpanID              string         `json:"span_id"`
	IsRoot              bool           `json:"is_root"`
	Kind                string         `json:"kind"`
	StartTime           time.Time      `json:"start_time"`
	Latency             float64        `json:"latency"`
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
	Status              int            `json:"status"`
	Username            string         `json:"username"`
	SourceIP            string         `json:"source_ip"`
	DestinationIP       string         `json:"destination_ip"`
	ServiceName         string         `json:"service_name"`
	ErrorType           string         `json:"error_type"`
	ErrorTableId        string         `json:"error_table_id"`
}

func (t SpanTableDto) GetAllColumns() []any {
	return []any{t.TraceID, t.ParentSpanID, t.SpanID, t.IsRoot, t.Kind, t.StartTime, t.Latency, t.Source, t.Destination, t.WorkloadIDList, t.Protocol, t.IssueHashList, t.RequestPayloadSize, t.ResponsePayloadSize, t.Method, t.Route, t.Scheme, t.Path, t.Query, t.Status, t.Username, t.SourceIP, t.DestinationIP, t.ServiceName, t.ErrorType, t.ErrorTableId}
}
