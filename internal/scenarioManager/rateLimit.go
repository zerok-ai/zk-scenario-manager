package scenarioManager

import (
	"fmt"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"scenario-manager/internal"
	"scenario-manager/internal/stores"
	tracePersistenceModel "scenario-manager/internal/tracePersistence/model"
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

func parseRateLimitsForScenario(scenario model.Scenario) []internal.IssuesCounter {
	rateLimiters := make([]internal.IssuesCounter, 0)
	for _, rateLimit := range scenario.RateLimit {
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
	return rateLimiters
}

type IssueLimit map[string]int

type NewLimit map[string]Limit
type Limit struct {
	limit      int
	expiryTime time.Duration
}

func copyIssueLimit(original IssueLimit) IssueLimit {
	copied := make(IssueLimit)
	for key, value := range original {
		copied[key] = value
	}
	return copied
}

func copyNewLimit(original NewLimit) NewLimit {
	copied := make(map[string]Limit)
	for key, value := range original {
		copied[key] = copyLimit(value)
	}
	return copied
}

func copyLimit(original Limit) Limit {
	newLimit := Limit{
		limit:      original.limit,
		expiryTime: original.expiryTime,
	}
	return newLimit
}

func getPostLastUnderScoreValue(input string) string {
	lastUnderscoreIndex := strings.LastIndex(input, "_")
	if lastUnderscoreIndex == -1 {
		// No dot found, no suffix
		return ""
	}

	// Extract the substring after the last dot
	suffix := input[lastUnderscoreIndex+1:]
	return suffix
}

func (worker *QueueWorkerOTel) rateLimitIncidents(incidents []tracePersistenceModel.IncidentWithIssues, scenario model.Scenario) []tracePersistenceModel.IncidentWithIssues {

	newIncidentList := make([]tracePersistenceModel.IncidentWithIssues, 0)

	// get the rate limits for the scenario
	newIssueAddLimit, newCurrentLimit := getLimitForIssue(scenario)

	limitsToAdd := make(map[string]NewLimit)
	limitsToDecrement := make(map[string]int)

	// get name of all the issues
	issueHashSet := make(ds.Set[string])
	for _, incident := range incidents {
		for _, issueGroup := range incident.IssueGroupList {
			for _, issue := range issueGroup.Issues {
				issueHashSet.Add(issue.IssueHash)
			}
		}
	}
	issueHashList := issueHashSet.GetAll()

	// get rate limits for all the issues
	savedIssuesCount := make(map[string]IssueLimit)
	for _, issueHash := range issueHashList {
		keyPattern := fmt.Sprintf("ratelimit_%s_*", issueHash)
		result, err := worker.traceStore.GetValuesForKeys(keyPattern)
		if err != nil {
			zkLogger.DebugF(RateLimitLoggerTag, "Error getting rate limits for issue: %v", issueHash)
		}

		savedCountPerIssue := make(IssueLimit)
		for key, value := range result {
			savedCountPerIssue[getPostLastUnderScoreValue(key)], _ = strconv.Atoi(value)
		}

		// add to savedIssuesCount
		savedIssuesCount[issueHash] = savedCountPerIssue
	}

	// rate limit incidents
	for _, incident := range incidents {
		shouldBeSaved := false
		for _, issueGroup := range incident.IssueGroupList {
			for _, issue := range issueGroup.Issues {
				savedCountPerIssue, ok := savedIssuesCount[issue.IssueHash]

				// limit not found: add the limits from the scenario
				if !ok {
					shouldBeSaved = true
					limitsToAdd[issue.IssueHash] = copyNewLimit(newIssueAddLimit)
					savedCountPerIssue = copyIssueLimit(newCurrentLimit)
				}

				// check if the allowed number of issues is still greater 0
				for key, value := range savedCountPerIssue {
					if value > 0 {
						shouldBeSaved = true

						limitsToDecrement[fmt.Sprintf("ratelimit_%s_%s", issue.IssueHash, key)]++
						savedCountPerIssue[key]--
					}
				}

				savedIssuesCount[issue.IssueHash] = savedCountPerIssue
			}
		}

		if shouldBeSaved {
			newIncidentList = append(newIncidentList, incident)
		}
	}

	worker.addLimits(limitsToAdd)
	worker.decrementLimits(limitsToDecrement)

	return newIncidentList
}

func getLimitForIssue(scenario model.Scenario) (NewLimit, IssueLimit) {
	newLimit := make(NewLimit)
	issueLimit := make(IssueLimit)

	for _, rateLimit := range scenario.RateLimit {

		expiryTime, err := parseTimeString(rateLimit.TickDuration)
		if err != nil {
			continue
		}

		newLimit[rateLimit.TickDuration] = Limit{limit: rateLimit.BucketMaxSize, expiryTime: expiryTime}
		issueLimit[rateLimit.TickDuration] = rateLimit.BucketMaxSize
	}

	return newLimit, issueLimit
}

func (worker *QueueWorkerOTel) addLimits(limitsToAdd map[string]NewLimit) {

	redisEntries := make([]stores.RedisEntry, 0)

	for hash, newLimit := range limitsToAdd {
		for duration, limit := range newLimit {
			keyPattern := fmt.Sprintf("ratelimit_%s_%s", hash, duration)
			entry := stores.RedisEntry{Key: keyPattern, Value: fmt.Sprintf("%d", limit.limit), ExpiryTime: limit.expiryTime}
			redisEntries = append(redisEntries, entry)
		}
	}

	worker.traceStore.SetKeysIfDoNotExist(redisEntries)
}

func (worker *QueueWorkerOTel) decrementLimits(limitsToDecrement map[string]int) {

	redisEntries := make([]stores.RedisDecrByEntry, 0)

	for key, value := range limitsToDecrement {
		entry := stores.RedisDecrByEntry{Key: key, Decrement: value}
		redisEntries = append(redisEntries, entry)
	}

	worker.traceStore.DecrementKeys(redisEntries)
}
