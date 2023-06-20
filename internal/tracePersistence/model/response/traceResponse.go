package traceresponse

import (
	"scenario-manager/internal/tracePersistence/model/dto"
)

var LogTag = "trace_response"

type TraceResponse struct {
	TraceIdList []string `json:"trace_id_list"`
}

func ConvertTraceToTraceResponse(t []dto.TraceTableDto) (*TraceResponse, *error) {
	traceIdList := make([]string, 0)
	for _, v := range t {
		traceIdList = append(traceIdList, v.TraceId)
	}

	resp := TraceResponse{TraceIdList: traceIdList}

	return &resp, nil
}
