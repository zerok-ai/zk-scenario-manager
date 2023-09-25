package filters

import (
	"fmt"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader"
	"github.com/zerok-ai/zk-utils-go/ds"
	zkRedis "github.com/zerok-ai/zk-utils-go/storage/redis"
	storage "github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"scenario-manager/internal/config"
	"time"
)

func getLRUCacheStore(redisConfig storage.RedisConfig, csh zkRedis.CacheStoreHook[string]) *zkRedis.LocalCacheStore[string] {

	dbName := "exception"
	cache := ds.GetLRUCache[string](cacheSize)
	redisClient := storage.GetRedisConnection(dbName, redisConfig)
	localCache := zkRedis.GetLocalCacheStore[string](redisClient, cache, csh, ctx)

	return localCache
}

func getExpiryBasedCacheStore(redisConfig storage.RedisConfig) *zkRedis.LocalCacheStore[string] {

	dbName := "ip"
	expiry := int64(5 * time.Minute)
	cache := ds.GetCacheWithExpiry[string](expiry)
	redisClient := storage.GetRedisConnection(dbName, redisConfig)
	localCache := zkRedis.GetLocalCacheStore[string](redisClient, cache, nil, ctx)

	return localCache
}

func getNewVZReader(cfg config.AppConfigs) (*vzReader.VzReader, error) {
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
