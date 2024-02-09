package timedWorkers

import (
	"fmt"
	"github.com/google/uuid"
	logger "github.com/zerok-ai/zk-utils-go/logs"
	zkmodel "github.com/zerok-ai/zk-utils-go/scenario/model"
	zkredis "github.com/zerok-ai/zk-utils-go/storage/redis"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	zktick "github.com/zerok-ai/zk-utils-go/ticker"
	"log"
	"scenario-manager/config"
	"scenario-manager/internal/redis"
	"time"
)

var workloadLogTag = "WorkloadKeyHandler"

// WorkloadKeyHandler handles periodic tasks to manage workload keys in Redis.
type WorkloadKeyHandler struct {
	RedisHandler   redis.RedisHandlerInterface
	UUID           string
	scenarioStore  *zkredis.VersionedStore[zkmodel.Scenario]
	ticker         *zktick.TickerTask
	workLoadTtl    time.Duration
	tickerInterval time.Duration
}

func NewWorkloadKeyHandler(cfg *config.AppConfigs, store *zkredis.VersionedStore[zkmodel.Scenario]) (*WorkloadKeyHandler, error) {
	// Generate a new ID for the instance
	uniqueId := uuid.New().String()

	redisHandler, err := redis.NewRedisHandler(&cfg.Redis, clientDBNames.FilteredTracesDBName)
	if err != nil {
		logger.Error(workloadLogTag, "Error while creating resource redis handler:", err)
		return nil, err
	}

	handler := &WorkloadKeyHandler{
		RedisHandler:  redisHandler,
		UUID:          uniqueId,
		scenarioStore: store,
	}

	handler.workLoadTtl = time.Duration(cfg.Workload.WorkLoadTtl) * time.Second
	handler.tickerInterval = time.Duration(cfg.Workload.WorkLoadTickerDuration) * time.Second

	handler.ticker = zktick.GetNewTickerTask("workload_rename", handler.tickerInterval, handler.manageWorkloadKeys)
	handler.ticker.Start()

	return handler, nil
}

// manageWorkloadKeys retrieves scenarios from scenario store and calls ManageWorkloadKey for all workloads.
func (wh *WorkloadKeyHandler) manageWorkloadKeys() {
	err := wh.RedisHandler.CheckRedisConnection()
	if err != nil {
		logger.Error(workloadLogTag, "Error caught while checking redis connection.")
		return
	}
	scenarios := wh.scenarioStore.GetAllValues()
	for _, scenario := range scenarios {
		if scenario == nil || scenario.Workloads == nil {
			logger.Error(workloadLogTag, "Scenario or workloads in nil.")
			continue
		}
		for workloadID, _ := range *scenario.Workloads {
			err := wh.ManageWorkloadKey(workloadID)
			if err != nil {
				logger.Error(workloadLogTag, "Error managing workload key for ", workloadID, " err: ", err)
			}
		}
	}
}

func (wh *WorkloadKeyHandler) ManageWorkloadKey(workloadID string) error {

	lockKeyName := fmt.Sprintf("rename_worker_%s", workloadID)
	currentValue, err := wh.RedisHandler.Get(lockKeyName)
	if err != nil {
		return fmt.Errorf("error getting value for lock key %s: %v", lockKeyName, err)
	}

	if currentValue != "" {
		//logger.Debug(workloadLogTag, "Another UUID already present, ignoring the operation for workload ", workloadID)
		return nil
	}

	// 2. Create a key with value(rename_worker_<workload_id>) as the UUID of the pod with ttl as 1min
	if err = wh.RedisHandler.SetNXWithTTL(lockKeyName, wh.UUID, wh.tickerInterval); err != nil {
		return fmt.Errorf("error setting key with TTL: %v", err)
	}

	// 3. Use the utility method to get keys by pattern
	pattern := fmt.Sprintf("%s_*", workloadID)
	keys, err := wh.RedisHandler.GetKeysByPattern(pattern)
	if err != nil {
		return fmt.Errorf("error retrieving keys with pattern %s: %v", pattern, err)
	}

	latestKeyPresent := false

	// Find the highest suffix.
	highestSuffix := -1
	for _, key := range keys {
		var suffix int

		if key == workloadID+"_latest" {
			latestKeyPresent = true
			continue
		}

		_, err = fmt.Sscanf(key, workloadID+"_%d", &suffix)
		if err == nil && suffix > highestSuffix {
			highestSuffix = suffix
		}
	}

	// <workloadID>_latest set is not present. Nothing to do here.
	if !latestKeyPresent {
		return nil
	}

	newKeyName := fmt.Sprintf("%s_%d", workloadID, (highestSuffix+1)%60)
	oldKeyName := fmt.Sprintf("%s_latest", workloadID)
	wh.renameKeyAndRemoveLock(newKeyName, lockKeyName, oldKeyName)

	return nil
}

func (wh *WorkloadKeyHandler) renameKeyAndRemoveLock(newKeyName, lockKeyName, oldKeyName string) {
	script := `
    local lockKeyName = KEYS[1]
	local oldKeyName = KEYS[2]
    local uuid = ARGV[1]
	local newKeyName = ARGV[2]
	local WorkloadTTL = ARGV[3]

    local currentValue = redis.call('GET', lockKeyName)
    if currentValue == nil then
    	return 'Error: Key does not exist'
	end

    if currentValue ~= uuid then
        return 'Error: UUID mismatch, ignoring rename operation.'
    end

	if redis.call('EXISTS', oldKeyName) == 1 then
        redis.call('RENAME', oldKeyName, newKeyName)
        redis.call('EXPIRE', newKeyName, WorkloadTTL)
	end

	redis.call('DEL', lockKeyName) 

    return 'Success'
    `

	_, err := wh.RedisHandler.Eval(script, []string{lockKeyName, oldKeyName}, wh.UUID, newKeyName, int(wh.workLoadTtl/time.Second)).Result()
	if err != nil {
		log.Fatalf("Error running Lua script: %v", err)
	}
}

func (wh *WorkloadKeyHandler) Close() {
	wh.ticker.Stop()
	wh.RedisHandler.Shutdown()
}
