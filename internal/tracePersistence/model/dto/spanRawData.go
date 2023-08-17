package dto

type SpanRawDataTableDto struct {
	TraceID     string `json:"trace_id"`
	SpanID      string `json:"span_id"`
	ReqHeaders  string `json:"req_headers"`
	RespHeaders string `json:"resp_headers"`
	IsTruncated bool   `json:"is_truncated"`
	ReqBody     []byte `json:"req_body"`
	RespBody    []byte `json:"resp_body"`
}

func (t SpanRawDataTableDto) GetAllColumns() []any {
	return []any{t.TraceID, t.SpanID, t.ReqHeaders, t.RespHeaders, t.IsTruncated, t.ReqBody, t.RespBody}
}
