package traceresponse

import (
	"scenario-manager/internal/tracePersistence/model/dto"
)

type SpansMetadataDetailsMap map[string]SpanMetadataDetails

type TraceMetadataResponse struct {
	Spans SpansMetadataDetailsMap `json:"spans"`
}

type SpanMetadataDetails struct {
	ParentSpanId string  `json:"parent_span_id"`
	Source       string  `json:"source"`
	Destination  string  `json:"destination"`
	Error        bool    `json:"error"`
	Metadata     string  `json:"metadata,omitempty"`
	LatencyMs    float32 `json:"latency_ms"`
	Protocol     string  `json:"protocol"`
}

func ConvertTraceMetadataToTraceMetadataResponse(t []dto.TraceMetadataTableDto) (*TraceMetadataResponse, *error) {
	respMap := make(map[string]SpanMetadataDetails, 0)
	for _, v := range t {

		s := SpanMetadataDetails{
			ParentSpanId: v.ParentSpanId,
			Source:       v.Source,
			Destination:  v.Destination,
			Error:        v.Error,
			Metadata:     v.Metadata,
			LatencyMs:    v.LatencyMs,
			Protocol:     v.Protocol,
		}

		respMap[v.SpanId] = s
	}

	resp := TraceMetadataResponse{Spans: respMap}

	return &resp, nil
}
