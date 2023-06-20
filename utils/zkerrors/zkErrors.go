package zkerrorsScenarioManager

import (
	"github.com/kataras/iris/v12"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
)

var (
	ZkErrorBadRequestScenarioIdEmpty        = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "ScenarioId cannot be empty"}
	ZkErrorBadRequestTraceIdIdEmpty         = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "TraceId cannot be empty"}
	ZkErrorBadRequestSpanIdEmpty            = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "SpanId cannot be empty"}
	ZkErrorBadRequestScenarioIdIsNotInteger = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "ScenarioId is not integer"}
	ZkErrorBadRequestTraceIdIsNotUUID       = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "TraceId is not UUID"}
)
