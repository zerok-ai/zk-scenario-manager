package stores

import (
	"encoding/json"
	"github.com/zerok-ai/zk-rawdata-reader/vzReader/utils"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	typedef "scenario-manager/internal"
	"scenario-manager/internal/redis"
)

type AttributesStore struct {
	redisHandler redis.RedisHandlerInterface
}

func (a AttributesStore) initialize() *AttributesStore {
	return &a
}

func (a AttributesStore) Close() {
	a.redisHandler.Shutdown()
}

func GetAttributesStore(redisConfig config.RedisConfig) *AttributesStore {
	dbName := clientDBNames.ResourceAndScopeAttrDBName
	redisHandler, err := redis.NewRedisHandler(&redisConfig, dbName)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error while creating resource redis handler:", err)
		return nil
	}

	attributesStore := AttributesStore{redisHandler: redisHandler}.initialize()
	return attributesStore
}

func (a AttributesStore) GetValueForKey(key string) string {
	result, err := a.redisHandler.Get(key)
	if err != nil {
		return ""
	}
	return result
}

func (a AttributesStore) GetResourceScopeInfoForHashes(tracesFromOTelStore map[typedef.TTraceid]*TraceFromOTel) (map[string]map[string]interface{}, map[string]map[string]interface{}) {

	resourceHashToInfoMap := make(map[string]map[string]interface{})
	scopeHashToInfoMap := make(map[string]map[string]interface{})

	for _, spanFromOTel := range tracesFromOTelStore {
		for _, span := range spanFromOTel.Spans {
			resourceHash := span.ResourceAttributesHash
			scopeHash := span.ScopeAttributesHash

			if len(resourceHash) > 0 && resourceHashToInfoMap[resourceHash] == nil {
				resourceHashToInfoMap[resourceHash] = a.GetResourceScopeInfoFromRedisUsingHash(resourceHash)
			}

			if len(scopeHash) > 0 && scopeHashToInfoMap[scopeHash] == nil {
				scopeHashToInfoMap[scopeHash] = a.GetResourceScopeInfoFromRedisUsingHash(scopeHash)
			}
		}
	}

	return resourceHashToInfoMap, scopeHashToInfoMap
}

func (a AttributesStore) GetResourceScopeInfoFromRedisUsingHash(hash string) (attributes map[string]interface{}) {
	resourceScopeInfoStr := a.GetValueForKey(hash)
	if utils.IsEmpty(resourceScopeInfoStr) {
		return nil
	}

	var resourceScopeInfoMap map[string]interface{}
	err := json.Unmarshal([]byte(resourceScopeInfoStr), &resourceScopeInfoMap)
	if err != nil {
		zkLogger.Error(LoggerTag, "Error unmarshalling attributes:", err)
		return nil
	}

	return resourceScopeInfoMap
}
