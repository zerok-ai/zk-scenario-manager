package dto

import "github.com/lib/pq"

type MetadataMap struct {
	Source       string         `json:"source"`
	Destination  string         `json:"destination"`
	TraceCount   int            `json:"trace_count"`
	ProtocolList pq.StringArray `json:"protocol_list"`
}
