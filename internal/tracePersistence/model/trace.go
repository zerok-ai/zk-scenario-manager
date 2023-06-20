package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

var LogTag = "zk_trace_model"

type Trace struct { // all the non pointer fields are mandatory
	ScenarioId      string   `json:"scenario_id"`
	ScenarioVersion string   `json:"scenario_version"`
	TraceId         string   `json:"trace_id"`
	SpanId          string   `json:"span_id"`
	Source          string   `json:"source"`
	Destination     string   `json:"destination"`
	Error           *bool    `json:"error"`      // marked pointer as they should not be nil, if nil return error
	Metadata        Metadata `json:"metadata"`   // could be empty, so not check here
	LatencyMs       *float32 `json:"latency_ms"` // marked pointer as they should not be nil, if nil return error
	Protocol        string   `json:"protocol"`
	RequestPayload  string   `json:"request_payload"`
	ResponsePayload string   `json:"response_payload"`
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

func (t Trace) GetAllColumns() []any {
	return []any{t.ScenarioId, t.ScenarioVersion, t.TraceId}
}
