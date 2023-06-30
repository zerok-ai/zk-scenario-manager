package dto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

//func TestConvertScenarioToTraceDto(t *testing.T) {
//	type args struct {
//		s model.Scenario
//	}
//	tests := []struct {
//		name  string
//		args  args
//		want  []ScenarioTableDto
//		want1 []SpanTableDto
//		want2 []SpanRawDataTableDto
//		want3 *error
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, got1, got2, got3 := ConvertScenarioToTraceDto(tt.args.s)
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("ConvertScenarioToTraceDto() got = %v, want %v", got, tt.want)
//			}
//			if !reflect.DeepEqual(got1, tt.want1) {
//				t.Errorf("ConvertScenarioToTraceDto() got1 = %v, want %v", got1, tt.want1)
//			}
//			if !reflect.DeepEqual(got2, tt.want2) {
//				t.Errorf("ConvertScenarioToTraceDto() got2 = %v, want %v", got2, tt.want2)
//			}
//			if !reflect.DeepEqual(got3, tt.want3) {
//				t.Errorf("ConvertScenarioToTraceDto() got3 = %v, want %v", got3, tt.want3)
//			}
//		})
//	}
//}

func TestScenarioTableDto_GetAllColumns(t1 *testing.T) {
	s := ScenarioTableDto{
		ScenarioId:      "s1",
		ScenarioVersion: "v1",
		TraceId:         "t1",
		CreatedAt:       time.Time{},
	}

	c := s.GetAllColumns()
	assert.Equal(t1, 5, len(c))
	assert.Equal(t1, s.ScenarioId, c[0])
	assert.Equal(t1, s.ScenarioVersion, c[1])
	assert.Equal(t1, s.TraceId, c[2])
}

func TestSpanRawDataTableDto_GetAllColumns(t1 *testing.T) {

	req, resp := "request_pay_load", "response_pay_load"

	s := SpanRawDataTableDto{
		TraceId:         "t1",
		SpanId:          "s1",
		RequestPayload:  []byte(req),
		ResponsePayload: []byte(resp),
	}

	c := s.GetAllColumns()
	assert.Equal(t1, 4, len(c))
	assert.Equal(t1, s.TraceId, c[0])
	assert.Equal(t1, s.SpanId, c[1])
	assert.Equal(t1, s.RequestPayload, c[2])
	assert.Equal(t1, s.ResponsePayload, c[3])
}

func TestSpanTableDto_GetAllColumns(t1 *testing.T) {
	s := SpanTableDto{
		TraceId:        "t1",
		SpanId:         "span1",
		ParentSpanId:   "parent_span1",
		Source:         "s1",
		Destination:    "d1",
		WorkloadIdList: []string{"id1", "id2"},
		Metadata:       "{'k':'value}",
		LatencyMs:      67.9,
		Protocol:       "HTTP",
	}

	c := s.GetAllColumns()
	assert.Equal(t1, 9, len(c))
	assert.Equal(t1, c[0], s.TraceId)
	assert.Equal(t1, c[1], s.SpanId)
	assert.Equal(t1, c[2], s.ParentSpanId)
	assert.Equal(t1, c[3], s.Source)
	assert.Equal(t1, c[4], s.Destination)
	assert.Equal(t1, c[5], s.WorkloadIdList)
	assert.Equal(t1, c[6], s.Metadata)
	assert.Equal(t1, c[7], s.LatencyMs)
	assert.Equal(t1, c[8], s.Protocol)
}

//func TestValidateScenario(t *testing.T) {
//	type args struct {
//		s model.Scenario
//	}
//	tests := []struct {
//		name  string
//		args  args
//		want  bool
//		want1 *zkerrors.ZkError
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, got1 := ValidateScenario(tt.args.s)
//			if got != tt.want {
//				t.Errorf("ValidateScenario() got = %v, want %v", got, tt.want)
//			}
//			if !reflect.DeepEqual(got1, tt.want1) {
//				t.Errorf("ValidateScenario() got1 = %v, want %v", got1, tt.want1)
//			}
//		})
//	}
//}
