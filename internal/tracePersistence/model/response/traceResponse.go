package traceresponse

import (
	"scenario-manager/internal/tracePersistence/model/dto"
)

var LogTag = "trace_response"

type TraceResponse struct {
	TraceIdList []string `json:"trace_id_list"`
}

func ConvertTraceToTraceResponse(t []dto.ScenarioTableDto) (*TraceResponse, *error) {
	traceIdList := make([]string, 0)
	for _, v := range t {
		traceIdList = append(traceIdList, v.TraceId)
	}

	resp := TraceResponse{TraceIdList: traceIdList}

	return &resp, nil
}

type IncidentResponse struct {
	IncidentList []dto.IncidentDto `json:"incidents"`
}

func ConvertIncidentToIncidentResponse(t []dto.IncidentDto) (*IncidentResponse, *error) {
	resp := IncidentResponse{IncidentList: t}
	return &resp, nil
}
