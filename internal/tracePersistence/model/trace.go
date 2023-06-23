package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"github.com/lib/pq"
	"time"
)

var LogTag = "zk_trace_model"

type Scenario struct { // all the non pointer fields are mandatory
	ScenarioId      string            `json:"scenario_id"`
	ScenarioVersion string            `json:"scenario_version"`
	ScenarioType    string            `json:"scenario_type"`
	ScenarioTitle   string            `json:"scenario_title"`
	CreatedAt       time.Time         `json:"created_at"`
	TraceToSpansMap map[string][]Span `json:"trace_to_spans_map"`
}

type Span struct {
	SpanId          string         `json:"span_id"`
	ParentSpanId    string         `json:"parent_span_id"`
	Source          string         `json:"source"`
	Destination     string         `json:"destination"`
	WorkloadIdList  pq.StringArray `json:"error"`
	Metadata        Metadata       `json:"metadata"`
	LatencyMs       *float32       `json:"latency_ms"`
	Protocol        string         `json:"protocol"`
	RequestPayload  string         `json:"request_payload"`
	ResponsePayload string         `json:"response_payload"`
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
