package dto

//func TestConvertScenarioToTraceDto(t *testing.T) {
//	type args struct {
//		s model.Scenario
//	}
//	tests := []struct {
//		name  string
//		args  args
//		want  []IncidentTableDto
//		want1 []SpanTableDto
//		want2 []SpanRawDataTableDto
//		want3 *error
//	}{
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, got1, got2, got3 := ConvertIncidentIssuesToIssueDto(tt.args.s)
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("ConvertIncidentIssuesToIssueDto() got = %v, want %v", got, tt.want)
//			}
//			if !reflect.DeepEqual(got1, tt.want1) {
//				t.Errorf("ConvertIncidentIssuesToIssueDto() got1 = %v, want %v", got1, tt.want1)
//			}
//			if !reflect.DeepEqual(got2, tt.want2) {
//				t.Errorf("ConvertIncidentIssuesToIssueDto() got2 = %v, want %v", got2, tt.want2)
//			}
//			if !reflect.DeepEqual(got3, tt.want3) {
//				t.Errorf("ConvertIncidentIssuesToIssueDto() got3 = %v, want %v", got3, tt.want3)
//			}
//		})
//	}
//}
//
//func TestScenarioTableDto_GetAllColumns(t1 *testing.T) {
//	s := IncidentTableDto{
//		ScenarioId:      "s1",
//		ScenarioVersion: "v1",
//		TraceId:         "t1",
//		IncidentCollectionTime:       time.Time{},
//	}
//
//	c := s.GetAllColumns()
//	assert.Equal(t1, 5, len(c))
//	assert.Equal(t1, s.ScenarioId, c[0])
//	assert.Equal(t1, s.ScenarioVersion, c[1])
//	assert.Equal(t1, s.TraceId, c[2])
//}
//
//func TestSpanRawDataTableDto_GetAllColumns(t1 *testing.T) {
//
//	req, resp := "request_pay_load", "response_pay_load"
//
//	s := SpanRawDataTableDto{
//		TraceId:         "t1",
//		SpanId:          "s1",
//		RequestPayload:  []byte(req),
//		ResponsePayload: []byte(resp),
//	}
//
//	c := s.GetAllColumns()
//	assert.Equal(t1, 4, len(c))
//	assert.Equal(t1, s.TraceId, c[0])
//	assert.Equal(t1, s.SpanId, c[1])
//	assert.Equal(t1, s.RequestPayload, c[2])
//	assert.Equal(t1, s.ResponsePayload, c[3])
//}
//
//func TestSpanTableDto_GetAllColumns(t1 *testing.T) {
//	s := SpanTableDto{
//		TraceId:        "t1",
//		SpanId:         "span1",
//		ParentSpanId:   "parent_span1",
//		Source:         "s1",
//		Destination:    "d1",
//		WorkloadIdList: []string{"id1", "id2"},
//		Metadata:       "{'k':'value}",
//		LatencyNs:      67.9,
//		Protocol:       "HTTP",
//	}
//
//	c := s.GetAllColumns()
//	assert.Equal(t1, 9, len(c))
//	assert.Equal(t1, c[0], s.TraceId)
//	assert.Equal(t1, c[1], s.SpanId)
//	assert.Equal(t1, c[2], s.ParentSpanId)
//	assert.Equal(t1, c[3], s.Source)
//	assert.Equal(t1, c[4], s.Destination)
//	assert.Equal(t1, c[5], s.WorkloadIdList)
//	assert.Equal(t1, c[6], s.Metadata)
//	assert.Equal(t1, c[7], s.LatencyNs)
//	assert.Equal(t1, c[8], s.Protocol)
//}

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
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, got1 := ValidateAndSanitiseIssue(tt.args.s)
//			if got != tt.want {
//				t.Errorf("ValidateAndSanitiseIssue() got = %v, want %v", got, tt.want)
//			}
//			if !reflect.DeepEqual(got1, tt.want1) {
//				t.Errorf("ValidateAndSanitiseIssue() got1 = %v, want %v", got1, tt.want1)
//			}
//		})
//	}
//}
