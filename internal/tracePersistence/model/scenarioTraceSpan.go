package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

var LogTag = "zk_trace_model"

type IncidentIssuesMapping struct {
	IssueList       []Issue  `json:"issue_list"`
	ScenarioId      string   `json:"scenario_id"`
	ScenarioVersion string   `json:"scenario_version"`
	Incident        Incident `json:"incident"`
}

type Issue struct {
	IssueId    string `json:"issue_id"`
	IssueTitle string `json:"issue_title"`
}

type Incident struct {
	TraceId                string    `json:"trace_id"`
	Spans                  []Span    `json:"spans"`
	IncidentCollectionTime time.Time `json:"incident_collection_time"`
}

type Span struct {
	SpanId          string          `json:"span_id"`
	ParentSpanId    string          `json:"parent_span_id"`
	Source          string          `json:"source"`
	Destination     string          `json:"destination"`
	WorkloadIdList  []string        `json:"workload_id_list"`
	Metadata        Metadata        `json:"metadata"`
	LatencyMs       *float32        `json:"latency_ms"`
	Protocol        string          `json:"protocol"`
	RequestPayload  RequestPayload  `json:"request_payload"`
	ResponsePayload ResponsePayload `json:"response_payload"`
	Time            time.Time       `json:"time"`
}

type ResponsePayload interface {
	GetStatus() string
	GetString() string
}

type RequestPayload interface {
	GetString() string
}

type HTTPResponsePayload struct {
	RespStatus  string `json:"resp_status"`
	RespMessage string `json:"resp_message"`
	RespHeaders string `json:"resp_headers"`
	RespBody    string `json:"resp_body"`
}

type HTTPRequestPayload struct {
	ReqPath    string `json:"req_path"`
	ReqMethod  string `json:"req_method"`
	ReqHeaders string `json:"req_headers"`
	ReqBody    string `json:"req_body"`
}

func (res HTTPResponsePayload) GetStatus() string {
	return res.RespStatus
}

func (res HTTPRequestPayload) GetString() string {
	x, _ := json.Marshal(res)
	return string(x)
}

func (res HTTPResponsePayload) GetString() string {
	x, _ := json.Marshal(res)
	return string(x)
}

type Metadata map[string]interface{}

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
