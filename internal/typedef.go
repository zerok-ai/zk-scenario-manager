package internal

import (
	"github.com/zerok-ai/zk-utils-go/ds"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
	"time"
)

type TTraceid string
type TSpanId string
type TMapOfSpanIdToSpan map[TSpanId]*tracePersistenceModel.Span

type TWorkspaceID string
type TIssueHash string

type TProtocol string

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
