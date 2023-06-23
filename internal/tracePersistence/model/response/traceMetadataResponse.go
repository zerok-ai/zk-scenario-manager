package traceresponse

import (
	"github.com/lib/pq"
	"scenario-manager/internal/tracePersistence/model/dto"
)

type SpansMetadataDetailsMap map[string]SpanDetails

type SpanResponse struct {
	Spans SpansMetadataDetailsMap `json:"spans"`
}

type SpanDetails struct {
	ParentSpanId   string         `json:"parent_span_id"`
	Source         string         `json:"source"`
	Destination    string         `json:"destination"`
	WorkloadIdList pq.StringArray `json:"workload_id_list"`
	Metadata       string         `json:"metadata,omitempty"`
	LatencyMs      float32        `json:"latency_ms"`
	Protocol       string         `json:"protocol"`
}

func ConvertSpanToSpanResponse(t []dto.SpanTableDto) (*SpanResponse, *error) {
	respMap := make(map[string]SpanDetails, 0)
	for _, v := range t {

		s := SpanDetails{
			ParentSpanId:   v.ParentSpanId,
			Source:         v.Source,
			Destination:    v.Destination,
			WorkloadIdList: v.WorkloadIdList,
			Metadata:       v.Metadata,
			LatencyMs:      v.LatencyMs,
			Protocol:       v.Protocol,
		}

		respMap[v.SpanId] = s
	}

	resp := SpanResponse{Spans: respMap}

	return &resp, nil
}
