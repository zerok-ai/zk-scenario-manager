package scenarioManager

import (
	"context"
	"fmt"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkRedis "github.com/zerok-ai/zk-utils-go/storage/redis"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	storage "github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"scenario-manager/config"
)

const (
	cacheSize int = 20
)

func GetLRUCacheStore(redisConfig storage.RedisConfig, csh zkRedis.CacheStoreHook[string], ctx context.Context) *zkRedis.LocalCacheKVStore[string] {

	dbName := clientDBNames.ErrorDetailDBName
	cache := ds.GetLRUCache[string](cacheSize)
	redisClient := storage.GetRedisConnection(dbName, redisConfig)
	localCache := zkRedis.GetLocalCacheStore[string](redisClient, cache, csh, ctx)

	return localCache
}

func GetNewVZReader(cfg config.AppConfigs) (*vzReader.VzReader, error) {
	reader := vzReader.VzReader{
		CloudAddr:  cfg.ScenarioConfig.VZCloudAddr,
		ClusterId:  cfg.ScenarioConfig.VZClusterId,
		ClusterKey: cfg.ScenarioConfig.VZClusterKey,
	}

	err := reader.Init()
	if err != nil {
		fmt.Printf("Failed to init reader, err: %v\n", err)
		return nil, err
	}

	return &reader, nil
}
