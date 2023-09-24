package stores

import (
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/ds"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"scenario-manager/internal/tracePersistence/model"
	tracePersistence "scenario-manager/internal/tracePersistence/service"
)

const cacheSize int = 20

type ExceptionStore struct {
	redisClient             *redis.Client
	lruCache                ds.LRUCache[string]
	tracePersistenceService *tracePersistence.TracePersistenceService
}

func GetExceptionStore(redisConfig config.RedisConfig, tps *tracePersistence.TracePersistenceService) *ExceptionStore {
	dbName := "exception"
	exceptionStore := (&ExceptionStore{
		redisClient:             config.GetRedisConnection(dbName, redisConfig),
		lruCache:                *ds.GetLRUCache[string](cacheSize),
		tracePersistenceService: tps,
	}).initialize(dbName)

	return exceptionStore
}

func (exceptionStore *ExceptionStore) initialize(tickerName string) *ExceptionStore {
	return exceptionStore
}

func (exceptionStore *ExceptionStore) Close() {
	err := exceptionStore.redisClient.Close()
	if err != nil {
		return
	}
}

// Get returns the value for the given key. If the value is not present in the cache, it is fetched from the DB and stored in the cache
// The function returns the value and a boolean indicating if the value was fetched from the cache
func (exceptionStore *ExceptionStore) Get(key string) (*string, bool) {
	value, fromCache := exceptionStore.GetFromCache(key)
	if value == nil {
		fromCache = false
		valueFromDB, err := exceptionStore.GetFromRedis([]string{key})
		if err != nil {
			return nil, fromCache
		}
		defer exceptionStore.saveInPostgresAndCache(key, valueFromDB[0])
	}
	return value, fromCache
}

// Put puts the given key-value pair in the cache and DB
func (exceptionStore *ExceptionStore) saveInPostgresAndCache(key string, value *string) {
	err := (*exceptionStore.tracePersistenceService).SaveExceptions([]model.ExceptionData{{Id: key, ExceptionBody: *value}})
	if err == nil {
		exceptionStore.PutInCache(key, value)
	}
}

func (exceptionStore *ExceptionStore) PutInCache(key string, value *string) {
	exceptionStore.lruCache.Put(key, value)
}

func (exceptionStore *ExceptionStore) GetFromCache(key string) (*string, bool) {
	return exceptionStore.lruCache.Get(key)
}

func (exceptionStore *ExceptionStore) GetFromRedis(keys []string) ([]*string, error) {
	values, err := exceptionStore.redisClient.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	// Process the retrieved values
	responseArray := make([]*string, len(values))
	for i, value := range values {
		// Check if the value can be typecast to T
		if typeCastedValue, ok := value.(string); ok {
			responseArray[i] = &typeCastedValue
		} else {
			responseArray[i] = nil
		}
	}
	return responseArray, nil
}
