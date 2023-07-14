package internal

import tracePersistenceModel "scenario-manager/internal/tracePersistence/model"

type TTraceid string
type TSpanId string
type TSpanIdToSpanMap map[TSpanId]*tracePersistenceModel.Span

type TWorkspaceID string
type TIssueHash string

type TProtocol string
