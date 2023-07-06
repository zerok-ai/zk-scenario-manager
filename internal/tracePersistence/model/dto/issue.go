package dto

type IssueDto struct {
	IssueId         string `json:"issue_id"`
	IssueTitle      string `json:"issue_title"`
	ScenarioId      string `json:"scenario_id"`
	ScenarioVersion string `json:"scenario_version"`
}

func (t IssueDto) GetAllColumns() []any {
	return []any{t.IssueId, t.IssueTitle, t.ScenarioId, t.ScenarioVersion}
}
