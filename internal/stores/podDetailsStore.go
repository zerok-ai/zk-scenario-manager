package stores

import (
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"scenario-manager/internal/redis"
)

const (
	upid_map_key_name = "upid_service_map"
)

type PodDetailsStore struct {
	redisHandler redis.RedisHandlerInterface
}

func (p PodDetailsStore) initialize() *PodDetailsStore {
	return &p
}

func (p PodDetailsStore) Close() {
	p.redisHandler.Shutdown()
}

func GetPodDetailsStore(redisConfig config.RedisConfig) *PodDetailsStore {
	dbName := clientDBNames.PodDetailsDBName
	zkLogger.DebugF(LoggerTag, "GetTraceStore: redisConfig=%v", redisConfig)
	redisHandler, err := redis.NewRedisHandler(&redisConfig, dbName)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error while creating resource redis handler:", err)
		return nil
	}

	podDetailsStore := PodDetailsStore{redisHandler: redisHandler}.initialize()
	return podDetailsStore
}

func (t PodDetailsStore) UpdateUPIDToServiceMap(upidToServiceMap map[string]string) {
	err := t.redisHandler.HMSet(upid_map_key_name, upidToServiceMap)
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Error setting the upid_service_map: %v", err)
	}
}

func (t PodDetailsStore) GetUPIDToServiceMap() (map[string]string, error) {
	upidToServiceMap, err := t.redisHandler.HGetAll(upid_map_key_name)
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Error getting the upid_service_map: %v", err)
		return nil, err
	}
	return upidToServiceMap, nil
}
