package dto

type SpanRawDataTableDto struct {
	TraceId         string `json:"trace_id"`
	SpanId          string `json:"span_id"`
	RequestPayload  []byte `json:"request_payload"`
	ResponsePayload []byte `json:"response_payload"`
}

func (t SpanRawDataTableDto) GetAllColumns() []any {
	return []any{t.TraceId, t.SpanId, t.RequestPayload, t.ResponsePayload}
}
