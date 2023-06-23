package validation

import (
	"github.com/google/uuid"
	zkCommon "github.com/zerok-ai/zk-utils-go/common"
	"github.com/zerok-ai/zk-utils-go/zkerrors"
	zkErrorsScenarioManager "scenario-manager/utils/zkerrors"
	"strconv"
)

var LogTag = "trace_persistence_validation"

func ValidateGetIncidentsDataApi(scenarioType, source, offset, limit string) *zkerrors.ZkError {
	if zkCommon.IsEmpty(scenarioType) {
		zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestScenarioTypeEmpty, nil)
		return &zkErr
	}

	if zkCommon.IsEmpty(source) {
		zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestSourceEmpty, nil)
		return &zkErr
	}

	if !zkCommon.IsEmpty(limit) {
		_, err := strconv.Atoi(limit)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestLimitIsNotInteger, nil)
			return &zkErr
		}
	}

	if !zkCommon.IsEmpty(offset) {
		_, err := strconv.Atoi(offset)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestOffsetIsNotInteger, nil)
			return &zkErr
		}
	}

	return nil
}

func ValidateGetTracesApi(scenarioId, offset, limit string) *zkerrors.ZkError {
	if zkCommon.IsEmpty(scenarioId) {
		zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestScenarioIdEmpty, nil)
		return &zkErr
	} else {
		_, err := strconv.Atoi(limit)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestScenarioIdIsNotInteger, nil)
			return &zkErr
		}
	}

	if !zkCommon.IsEmpty(limit) {
		_, err := strconv.Atoi(limit)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestLimitIsNotInteger, nil)
			return &zkErr
		}
	}

	if !zkCommon.IsEmpty(offset) {
		_, err := strconv.Atoi(offset)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestOffsetIsNotInteger, nil)
			return &zkErr
		}
	}

	return nil
}

func IsValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

func ValidateGetTracesRawDataApi(traceId, spanId, offset, limit string) *zkerrors.ZkError {
	if zkCommon.IsEmpty(traceId) {
		zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestTraceIdIdEmpty, nil)
		return &zkErr
	}

	if zkCommon.IsEmpty(spanId) {
		zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestSpanIdEmpty, nil)
		return &zkErr
	}

	if !zkCommon.IsEmpty(limit) {
		_, err := strconv.Atoi(limit)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestLimitIsNotInteger, nil)
			return &zkErr
		}
	}

	if !zkCommon.IsEmpty(offset) {
		_, err := strconv.Atoi(offset)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestOffsetIsNotInteger, nil)
			return &zkErr
		}
	}

	return nil
}

func ValidateGetTracesMetadataApi(traceId, offset, limit string) *zkerrors.ZkError {
	if zkCommon.IsEmpty(traceId) {
		zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestTraceIdIdEmpty, nil)
		return &zkErr
	}

	if !zkCommon.IsEmpty(limit) {
		_, err := strconv.Atoi(limit)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestLimitIsNotInteger, nil)
			return &zkErr
		}
	}

	if !zkCommon.IsEmpty(offset) {
		_, err := strconv.Atoi(offset)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestOffsetIsNotInteger, nil)
			return &zkErr
		}
	}

	return nil
}

func ValidateGetMetadataMapApi(duration, offset, limit string) *zkerrors.ZkError {
	if zkCommon.IsEmpty(duration) {
		zkErr := zkerrors.ZkErrorBuilder{}.Build(zkErrorsScenarioManager.ZkErrorBadRequestDurationEmpty, nil)
		return &zkErr
	}

	if !zkCommon.IsEmpty(limit) {
		_, err := strconv.Atoi(limit)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestLimitIsNotInteger, nil)
			return &zkErr
		}
	}

	if !zkCommon.IsEmpty(offset) {
		_, err := strconv.Atoi(offset)
		if err != nil {
			zkErr := zkerrors.ZkErrorBuilder{}.Build(zkerrors.ZkErrorBadRequestOffsetIsNotInteger, nil)
			return &zkErr
		}
	}

	return nil
}
