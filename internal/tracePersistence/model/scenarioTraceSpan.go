package model

import (
	"scenario-manager/internal/stores"
	"time"
)

type ErrorData struct {
	Id   string `json:"id"`
	Data string `json:"data"`
}

// TODO change this data structure by removing IssueGroupList to IssueGroup
type IncidentWithIssues struct {
	Incident       Incident     `json:"incident"`
	IssueGroupList []IssueGroup `json:"issue_group_list"`
}

type IssueGroup struct {
	ScenarioId      string  `json:"scenario_id"`
	ScenarioVersion string  `json:"scenario_version"`
	Issues          []Issue `json:"issues"`
}

type Issue struct {
	IssueHash  string `json:"issue_hash"`
	IssueTitle string `json:"issue_title"`
}

type Incident struct {
	TraceId                string                 `json:"trace_id"`
	Spans                  []*stores.SpanFromOTel `json:"spans"`
	IncidentCollectionTime time.Time              `json:"incident_collection_time"`
}

type SpanRawData struct {
	TraceID     string `json:"trace_id"`
	SpanID      string `json:"span_id"`
	ReqHeaders  string `json:"req_headers"`
	RespHeaders string `json:"resp_headers"`
	IsTruncated bool   `json:"is_truncated"`
	ReqBody     string `json:"req_body"`
	RespBody    string `json:"resp_body"`
}

type ResponsePayload interface {
	GetStatus() string
	GetString() string
}

type RequestPayload interface {
	GetString() string
}
