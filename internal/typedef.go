package internal

import (
	"github.com/zerok-ai/zk-utils-go/ds"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	"time"
)

type TTraceid string
type TSpanId string

type TWorkloadId string

type TIssueHash string

type TProtocol string

type ProtocolType string

const (
	ProtocolTypeHTTP    ProtocolType = "HTTP"
	ProtocolTypeDB      ProtocolType = "DB"
	ProtocolTypeGRPC    ProtocolType = "GRPC"
	ProtocolTypeUnknown ProtocolType = "UNKNOWN"
)

type TTraceIdSetPerProtocol map[ProtocolType]ds.Set[string]

type TScenarioID string
type ScenarioTraces struct {
	Scenario *scenarioGeneratorModel.Scenario
	Traces   ds.Set[TTraceid]
}
type ScenarioToScenarioTracesMap map[TScenarioID]ScenarioTraces

type IssueBucket map[TIssueHash]int
type IssuesCounter struct {
	IssueCountMap    IssueBucket
	ExpiryTime       time.Time
	BucketMaxSize    int
	BucketRefillSize int
	TickDuration     time.Duration
}

type IssueRateMap map[TScenarioID][]IssuesCounter

type ExecutorToSchemaVersionMap map[scenarioGeneratorModel.ExecutorName]string

type GenericMap map[string]interface{}
