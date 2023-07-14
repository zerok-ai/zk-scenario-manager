package main

import (
	"fmt"
	zkcommon "github.com/zerok-ai/zk-utils-go/common"
	zkPostgres "github.com/zerok-ai/zk-utils-go/storage/sqlDB/postgres"
	"github.com/zerok-ai/zk-utils-go/storage/sqlDB/postgres/config"
	"scenario-manager/internal/tracePersistence/model"
	"scenario-manager/internal/tracePersistence/repository"
	"scenario-manager/internal/tracePersistence/service"
	"time"
)

func main() {
	c := config.PostgresConfig{
		Host:                           "localhost",
		Port:                           5432,
		User:                           "pl",
		Password:                       "pl",
		Dbname:                         "pl",
		MaxConnections:                 0,
		MaxIdleConnections:             0,
		ConnectionMaxLifetimeInMinutes: 0,
	}

	db, _ := zkPostgres.NewZkPostgresRepo(c)

	tpr := repository.NewTracePersistenceRepo(db)
	svc := service.NewScenarioPersistenceService(tpr)

	mInner := map[string]interface{}{
		"first_name": "zk_12",
		"last_name":  "ai_34",
	}

	m1 := map[string]interface{}{
		"name": mInner,
	}

	req1 := model.HTTPRequestPayload{
		ReqPath:    "/exc",
		ReqMethod:  "get",
		ReqHeaders: "Token: 1234",
		ReqBody:    "{'key': 'MYREQUEST'}",
	}

	res1 := model.HTTPResponsePayload{
		RespBody:    "{'key': 'MYRESPONSE'}",
		RespStatus:  "200",
		RespHeaders: "v",
		RespMessage: "{NO MESSAGE}",
	}

	issueHashList := []string{"i1", "i2"}

	t1span1 := model.Span{
		SpanId:          "t1s4",
		ParentSpanId:    "",
		Source:          "source1",
		Destination:     "destination1",
		WorkloadIdList:  []string{"id4", "id5"},
		Metadata:        m1,
		LatencyNs:       zkcommon.ToPtr(float32(542.5754)),
		Protocol:        "HTTP",
		RequestPayload:  req1,
		IssueHashList:   issueHashList,
		ResponsePayload: res1,
	}

	req2 := model.HTTPRequestPayload{
		ReqPath:    "/error",
		ReqMethod:  "post",
		ReqHeaders: "Token: 123422222222222222",
		ReqBody:    "{'key': 'MYREQUEST222222222'}",
	}

	res2 := model.HTTPResponsePayload{
		RespBody:    "{'key': 'MYRESPONSE2222222'}",
		RespStatus:  "200",
		RespHeaders: "v",
		RespMessage: "{NO MESSAGE}",
	}

	t1span2 := model.Span{
		SpanId:          "t1s5",
		ParentSpanId:    "t1s1",
		Source:          "source2",
		Destination:     "destination2",
		WorkloadIdList:  []string{"id1", "id2"},
		Metadata:        m1,
		LatencyNs:       zkcommon.ToPtr(float32(542.5754)),
		Protocol:        "HTTP",
		RequestPayload:  req2,
		ResponsePayload: res2,
		IssueHashList:   issueHashList,
		Time:            time.Now(),
	}

	traceToSpanMap := map[string][]model.Span{}
	traceToSpanMap2 := map[string][]model.Span{}
	traceToSpanMap["t1"] = []model.Span{t1span1, t1span2}

	traceToSpanMap2["t2"] = []model.Span{t1span1, t1span2}

	issueList1 := []model.Issue{{IssueHash: "i1", IssueTitle: "it1"}, {IssueHash: "i2", IssueTitle: "it2"}}
	incident := model.Incident{TraceId: "t1", Spans: []model.Span{t1span1, t1span2}, IncidentCollectionTime: time.Now()}

	s1 := model.IssueGroup{
		ScenarioId:      "3",
		ScenarioVersion: "v1",
		Issues:          issueList1,
	}

	data := model.IncidentWithIssues{
		Incident:       incident,
		IssueGroupList: []model.IssueGroup{s1},
	}

	err := svc.SaveIncidents([]model.IncidentWithIssues{data})
	if err != nil {
		fmt.Print(err)
		return
	}
	fmt.Println("success")
}
