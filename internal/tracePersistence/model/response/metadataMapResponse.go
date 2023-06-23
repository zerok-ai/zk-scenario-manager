package traceresponse

import (
	"scenario-manager/internal/tracePersistence/model/dto"
	"strings"
)

type MetadataMapResponse struct {
	MetadataMapList []dto.MetadataMap `json:"metadata_map_list"`
}

func ConvertMetadataMapToMetadataMapResponse(t []dto.MetadataMap) (*MetadataMapResponse, *error) {
	var resList []dto.MetadataMap
	for _, v := range t {
		x := dto.MetadataMap{
			Source:       removeLastTwoStrings(v.Source),
			Destination:  removeLastTwoStrings(v.Destination),
			TraceCount:   v.TraceCount,
			ProtocolList: v.ProtocolList,
		}
		resList = append(resList, x)
	}
	resp := MetadataMapResponse{MetadataMapList: resList}
	return &resp, nil
}

func removeLastTwoStrings(input string) string {
	parts := strings.Split(input, "-")
	if len(parts) <= 2 {
		return input
	}

	trimmedParts := parts[:len(parts)-2]
	return strings.Join(trimmedParts, "-")
}
