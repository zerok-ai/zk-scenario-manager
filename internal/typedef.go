package internal

import (
	"github.com/zerok-ai/zk-utils-go/ds"
	scenarioGeneratorModel "github.com/zerok-ai/zk-utils-go/scenario/model"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
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
