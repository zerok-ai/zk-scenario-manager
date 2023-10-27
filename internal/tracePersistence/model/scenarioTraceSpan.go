package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	typedef "scenario-manager/internal"
	"time"
)

var LogTag = "zk_trace_model"

type ErrorData struct {
	Id   string `json:"id"`
	Data string `json:"data"`
}

type IncidentWithIssues struct {
	Incident       Incident     `json:"incident"`
	IssueGroupList []IssueGroup `json:"issue_group_list"`
}

type IssueGroup struct {
	ScenarioId      string  `json:"scenario_id"`
	ScenarioVersion string  `json:"scenario_version"`
	Issues          []Issue `json:"issues"`
}

type Issue struct {
	IssueHash  string `json:"issue_hash"`
	IssueTitle string `json:"issue_title"`
}

type Incident struct {
	TraceId                string    `json:"trace_id"`
	Spans                  []*Span   `json:"spans"`
	IssueHash              string    `json:"issue_hash"`
	IncidentCollectionTime time.Time `json:"incident_collection_time"`
}

type Span struct {
	TraceID             string               `json:"trace_id"`
	ParentSpanID        string               `json:"parent_span_id"`
	SpanID              string               `json:"span_id"`
	SpanName            string               `json:"span_name"`
	IsRoot              bool                 `json:"is_root"`
	Kind                string               `json:"kind"`
	StartTime           time.Time            `json:"start_time"`
	Latency             uint64               `json:"latency"`
	Source              string               `json:"source"`
	Destination         string               `json:"destination"`
	WorkloadIDList      []string             `json:"workload_id_list"`
	Protocol            typedef.ProtocolType `json:"protocol"`
	IssueHashList       []string             `json:"issue_hash_list"`
	RequestPayloadSize  uint64               `json:"request_payload_size"`
	ResponsePayloadSize uint64               `json:"response_payload_size"`
	Method              string               `json:"method"`
	Route               string               `json:"route"`
	Scheme              string               `json:"scheme"`
	Path                string               `json:"path"`
	Query               string               `json:"query"`
	Status              *float64             `json:"status"`
	Username            string               `json:"username"`
	SourceIP            string               `json:"source_ip"`
	DestinationIP       string               `json:"destination_ip"`
	ServiceName         string               `json:"service_name"`
	Errors              string               `json:"errors"`

	SpanAttributes     typedef.GenericMap `json:"span_attributes"`
	ResourceAttributes typedef.GenericMap `json:"resource_attributes"`
	ScopeAttributes    typedef.GenericMap `json:"scope_attributes"`

	SpanRawData
	OTelSchemaVersion string
	EBPFSchemaVersion string
	SpanAsMap         typedef.GenericMap
	GroupByMap        GroupByMap
}

type GroupByValueItem struct {
	WorkloadId string `json:"workload_id"`
	Title      string `json:"title"`
	Hash       string `json:"hash"`
}

type GroupByValues []*GroupByValueItem
type ScenarioId string
type GroupByMap map[ScenarioId]GroupByValues

func (s Span) ToMap() (typedef.GenericMap, error) {
	if s.SpanAsMap == nil {
		spanAsString, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}

		var spanAsMap typedef.GenericMap
		err = json.Unmarshal(spanAsString, &spanAsMap)
		if err != nil {
			return nil, err
		}

		s.SpanAsMap = spanAsMap
	}
	return s.SpanAsMap, nil
}

type SpanRawData struct {
	TraceID     string `json:"trace_id"`
	SpanID      string `json:"span_id"`
	ReqHeaders  string `json:"req_headers"`
	RespHeaders string `json:"resp_headers"`
	IsTruncated bool   `json:"is_truncated"`
	ReqBody     string `json:"req_body"`
	RespBody    string `json:"resp_body"`
}

type ResponsePayload interface {
	GetStatus() string
	GetString() string
}

type RequestPayload interface {
	GetString() string
}

type Metadata typedef.GenericMap

// Value Make the Attrs struct implement the driver.Valuer interface. This method
// simply returns the JSON-encoded representation of the struct.
func (a Metadata) Value() (driver.Value, error) {
	return json.Marshal(a)
}

// Scan Make the Attrs struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (a *Metadata) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &a)
}
