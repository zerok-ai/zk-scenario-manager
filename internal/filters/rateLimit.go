package filters

import (
	"encoding/json"
	"fmt"
	"github.com/zerok-ai/zk-utils-go/logs"
	"scenario-manager/internal"
	"scenario-manager/internal/tracePersistence/model"
	"strconv"
	"strings"
	"time"
)

func returnNewRateLimitObject(currTime time.Time, issueRate internal.IssuesCounter) internal.IssuesCounter {
	issueRate.IssueCountMap = make(map[internal.TIssueHash]int, 0)
	issueRate.ExpiryTime = currTime.Add(issueRate.TickDuration)
	return issueRate
}

func parseTimeString(input string) (time.Duration, error) {
	var duration time.Duration
	var multiplier time.Duration

	switch {
	case strings.HasSuffix(input, "m"):
		multiplier = time.Minute
	case strings.HasSuffix(input, "h"):
		multiplier = time.Hour
	case strings.HasSuffix(input, "d"):
		multiplier = 24 * time.Hour
	default:
		return 0, fmt.Errorf("unsupported input format")
	}

	numericPart := strings.TrimSuffix(input, string(input[len(input)-1]))
	val, err := strconv.Atoi(numericPart)
	if err != nil {
		return 0, err
	}

	duration = time.Duration(val) * multiplier
	return duration, nil
}

func (scenarioManager *ScenarioManager) rateLimitIssue(incident model.IncidentWithIssues, scenarioWithTraces internal.ScenarioToScenarioTracesMap) model.IncidentWithIssues {
	// create a list to store the issues to be removed
	issuesToRemove := make(map[internal.TIssueHash]bool, 0)

	// acquire the lock
	scenarioManager.mutex.Lock()
	defer scenarioManager.mutex.Unlock()

	// for each issueGroup in the incident, there is a list of issues
	for _, issue := range incident.IssueGroupList {
		tScenarioId := internal.TScenarioID(issue.ScenarioId)
		// if the scenario is not present in the issueRateMap or if the scenario is updated and a new rate limit is added, then len of previous rate limit and current rate limit will be different
		if v, ok := scenarioManager.issueRateMap[tScenarioId]; !ok || len(v) != len(scenarioWithTraces[tScenarioId].Scenario.RateLimit) {
			// for every rateLimit object in scenario, create a new entry in the issueRateMap
			rateLimiters := make([]internal.IssuesCounter, 0)
			for _, rateLimit := range scenarioWithTraces[tScenarioId].Scenario.RateLimit {
				m := make(internal.IssueBucket, 0)
				duration, err := parseTimeString(rateLimit.TickDuration)
				if err != nil {
					logger.ErrorF(LoggerTag, "Error parsing time string %s", rateLimit.TickDuration)
					continue
				}
				t := internal.IssuesCounter{
					IssueCountMap:    m,
					ExpiryTime:       time.Now().Add(duration),
					BucketMaxSize:    rateLimit.BucketMaxSize,
					BucketRefillSize: rateLimit.BucketRefillSize,
					TickDuration:     duration,
				}
				rateLimiters = append(rateLimiters, t)
			}
			scenarioManager.issueRateMap[tScenarioId] = rateLimiters
		}

		// update the count of issues in the rateLimitMap only if the value is less than the bucketMaxSize, else add the issue to the issuesToRemove list
		for _, rateLimiter := range scenarioManager.issueRateMap[tScenarioId] {
			for _, i := range issue.Issues {
				if rateLimiter.IssueCountMap[internal.TIssueHash(i.IssueHash)] > rateLimiter.BucketMaxSize {
					issuesToRemove[internal.TIssueHash(i.IssueHash)] = true
				}
				rateLimiter.IssueCountMap[internal.TIssueHash(i.IssueHash)]++
			}
		}
	}

	// if there are issues to be removed, then remove them from the incident
	if len(issuesToRemove) != 0 {
		newIssueGroupList := make([]model.IssueGroup, 0)
		for _, issueGroup := range incident.IssueGroupList {
			newIssuesList := make([]model.Issue, 0)
			for _, issue := range issueGroup.Issues {
				if _, ok := issuesToRemove[internal.TIssueHash(issue.IssueHash)]; ok {
					continue
				}
				newIssuesList = append(newIssuesList, issue)
			}
			issueGroup.Issues = newIssuesList
			newIssueGroupList = append(newIssueGroupList, issueGroup)
		}
		incident.IssueGroupList = sanitizeIssueGroupList(newIssueGroupList)
		return incident
	}
	return incident
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
	for scenarioID, issuesCounter := range scenarioManager.issueRateMap {
		for i, issueRate := range issuesCounter {
			if currTime.After(issueRate.ExpiryTime) {
				b, _ := json.Marshal(issueRate)
				logger.InfoF(RateLimitLoggerTag, "Scenario id: %s, IssueRate: %s", scenarioID, string(b))
				scenarioManager.issueRateMap[scenarioID][i] = returnNewRateLimitObject(currTime, issueRate)
			}
		}
	}
}
