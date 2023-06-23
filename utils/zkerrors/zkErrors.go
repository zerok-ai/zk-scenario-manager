package errors

import (
	"github.com/kataras/iris/v12"
	zkErrors "github.com/zerok-ai/zk-utils-go/zkerrors"
)

var (
	ZkErrorBadRequestScenarioIdEmpty        = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "ScenarioId cannot be empty"}
	ZkErrorBadRequestTraceIdIdEmpty         = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "TraceId cannot be empty"}
	ZkErrorBadRequestDurationEmpty          = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "Duration cannot be empty"}
	ZkErrorBadRequestSpanIdEmpty            = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "SpanId cannot be empty"}
	ZkErrorBadRequestSourceEmpty            = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "Source cannot be empty"}
	ZkErrorBadRequestScenarioTypeEmpty      = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "Scenario type cannot be empty"}
	ZkErrorBadRequestScenarioIdIsNotInteger = zkErrors.ZkErrorType{Status: iris.StatusBadRequest, Type: "BAD_REQUEST", Message: "ScenarioId is not integer"}
)
