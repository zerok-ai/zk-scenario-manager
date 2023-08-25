package filters

import (
	"fmt"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"scenario-manager/internal"
	"scenario-manager/internal/tracePersistence/model"
	"strconv"
	"strings"
	"time"
)

const (
	Minute = "m"
	Hour   = "h"
	Day    = "d"
)

func returnNewRateLimitObject(currTime time.Time, issueRate internal.IssuesCounter) internal.IssuesCounter {
	issueRate.IssueCountMap = make(map[internal.TIssueHash]int, 0)
	issueRate.ExpiryTime = currTime.Add(issueRate.TickDuration)
	return issueRate
}

func parseTimeString(input string) (time.Duration, error) {
	var duration time.Duration
	var multiplier time.Duration
	var suffixToRemove string

	switch {
	case strings.HasSuffix(input, Minute):
		suffixToRemove = Minute
		multiplier = time.Minute
	case strings.HasSuffix(input, Hour):
		suffixToRemove = Hour
		multiplier = time.Hour
	case strings.HasSuffix(input, Day):
		suffixToRemove = Day
		multiplier = 24 * time.Hour
	default:
		return 0, fmt.Errorf("unsupported input format")
	}

	numericPart := strings.TrimSuffix(input, suffixToRemove)
	val, err := strconv.Atoi(numericPart)
	if err != nil {
		return 0, err
	}

	duration = time.Duration(val) * multiplier
	return duration, nil
}

func (scenarioManager *ScenarioManager) initRateLimit(scenarioWithTraces internal.ScenarioToScenarioTracesMap) {
	defer scenarioManager.mutex.Unlock()
	scenarioManager.mutex.Lock()
	for tScenarioId, traceMap := range scenarioWithTraces {
		// if the scenario is not present in the issueRateMap or if the scenario is updated and a new rate limit is added, then len of previous rate limit and current rate limit will be different
		if v, ok := scenarioManager.issueRateMap[tScenarioId]; !ok || len(v) != len(traceMap.Scenario.RateLimit) {
			// for every rateLimit object in scenario, create a new entry in the issueRateMap
			rateLimiters := make([]internal.IssuesCounter, 0)
			for _, rateLimit := range traceMap.Scenario.RateLimit {
				issueBucketArr := make(internal.IssueBucket, 0)
				duration, err := parseTimeString(rateLimit.TickDuration)
				if err != nil {
					zkLogger.ErrorF(LoggerTag, "Error parsing time string %s", rateLimit.TickDuration)
					duration = time.Minute * 5 // default to 5 minutes
				}

				issuesCounter := internal.IssuesCounter{
					IssueCountMap:    issueBucketArr,
					ExpiryTime:       time.Now().Add(duration),
					BucketMaxSize:    rateLimit.BucketMaxSize,
					BucketRefillSize: rateLimit.BucketRefillSize,
					TickDuration:     duration,
				}
				rateLimiters = append(rateLimiters, issuesCounter)
			}
			scenarioManager.issueRateMap[tScenarioId] = rateLimiters
		}
	}
}

func (scenarioManager *ScenarioManager) updateValueInRateLimitMap(incident model.IncidentWithIssues) ds.Set[internal.TIssueHash] {
	// create a list to store the issues to be removed
	issuesToRemove := ds.Set[internal.TIssueHash]{}

	defer scenarioManager.mutex.Unlock()
	scenarioManager.mutex.Lock()

	// for each issueGroup in the incident, there is a list of issues
	for _, issue := range incident.IssueGroupList {
		tScenarioId := internal.TScenarioID(issue.ScenarioId)
		// update the count of issues in the rateLimitMap only if the value is less than the bucketMaxSize, else add the issue to the issuesToRemove list
		for _, rateLimiter := range scenarioManager.issueRateMap[tScenarioId] {
			for _, i := range issue.Issues {
				if rateLimiter.IssueCountMap[internal.TIssueHash(i.IssueHash)] > rateLimiter.BucketMaxSize {
					issuesToRemove.Add(internal.TIssueHash(i.IssueHash))
				}
				rateLimiter.IssueCountMap[internal.TIssueHash(i.IssueHash)]++
			}
		}
	}
	return issuesToRemove
}

func (scenarioManager *ScenarioManager) rateLimitIssue(incident model.IncidentWithIssues, scenarioWithTraces internal.ScenarioToScenarioTracesMap) *model.IncidentWithIssues {

	issuesToRemove := scenarioManager.updateValueInRateLimitMap(incident)
	// if there are issues to be removed, then remove them from the incident
	if len(issuesToRemove) != 0 {
		newIssueGroupList := make([]model.IssueGroup, 0)
		for _, issueGroup := range incident.IssueGroupList {
			newIssuesList := make([]model.Issue, 0)
			for _, issue := range issueGroup.Issues {
				if issuesToRemove.Contains(internal.TIssueHash(issue.IssueHash)) {
					continue
				}
				newIssuesList = append(newIssuesList, issue)
			}
			issueGroup.Issues = newIssuesList
			newIssueGroupList = append(newIssueGroupList, issueGroup)
		}
		sanitizedIssueGroup := sanitizeIssueGroupList(newIssueGroupList)
		if len(sanitizedIssueGroup) == 0 {
			return nil
		}
		incident.IssueGroupList = sanitizedIssueGroup
	}
	return &incident
}

// sanitizeIssueGroupList removes the issueGroup from the list if the issueGroup has no issues
func sanitizeIssueGroupList(issueGroupList []model.IssueGroup) []model.IssueGroup {
	l := make([]model.IssueGroup, 0)
	for _, v := range issueGroupList {
		if len(v.Issues) == 0 {
			continue
		}
		l = append(l, v)
	}
	return l
}

func (scenarioManager *ScenarioManager) processRateLimit() {
	currTime := time.Now()
	if scenarioManager.issueRateMap == nil {
		scenarioManager.issueRateMap = make(internal.IssueRateMap, 0)
	}
	defer scenarioManager.mutex.Unlock()
	scenarioManager.mutex.Lock()
	for scenarioID, issuesCounterArr := range scenarioManager.issueRateMap {
		for i, issuesCounter := range issuesCounterArr {
			if currTime.After(issuesCounter.ExpiryTime) {
				zkLogger.InfoF(RateLimitLoggerTag, "Scenario id: %s, IssueRate: %s", scenarioID, fmt.Sprintf("%+v", issuesCounter))
				scenarioManager.issueRateMap[scenarioID][i] = returnNewRateLimitObject(currTime, issuesCounter)
			}
		}
	}
}
