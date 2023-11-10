package stores

import (
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
)

const (
	upid_map_key_name = "upid_service_map"
)

type PodDetailsStore struct {
	redisClient *redis.Client
}

func (p PodDetailsStore) initialize() *PodDetailsStore {
	return &p
}

func (p PodDetailsStore) Close() {
	p.redisClient.Close()
}

func GetPodDetailsStore(redisConfig config.RedisConfig) *PodDetailsStore {
	dbName := clientDBNames.PodDetailsDBName
	zkLogger.DebugF(LoggerTag, "GetTraceStore: redisConfig=%v", redisConfig)
	_redisClient := config.GetRedisConnection(dbName, redisConfig)
	podDetailsStore := PodDetailsStore{redisClient: _redisClient}.initialize()
	return podDetailsStore
}

func (t PodDetailsStore) UpdateUPIDToServiceMap(upidToServiceMap map[string]string) {
	_, err := t.redisClient.HMSet(ctx, upid_map_key_name, upidToServiceMap).Result()
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Error setting the upid_service_map: %v", err)
	}
}

func (t PodDetailsStore) GetUPIDToServiceMap() (map[string]string, error) {
	upidToServiceMap, err := t.redisClient.HGetAll(ctx, upid_map_key_name).Result()
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Error getting the upid_service_map: %v", err)
		return nil, err
	}
	return upidToServiceMap, nil
}
