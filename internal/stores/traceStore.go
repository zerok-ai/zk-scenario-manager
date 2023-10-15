package stores

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	clientDBNames "github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"time"
)

type TraceStore struct {
	redisClient         *redis.Client
	ttlForTransientSets time.Duration
}

func (t TraceStore) initialize() *TraceStore {
	return &t
}

func (t TraceStore) Close() {
	t.redisClient.Close()
}

func GetTraceStore(redisConfig config.RedisConfig, ttlForTransientSets time.Duration) *TraceStore {
	dbName := clientDBNames.FilteredTracesDBName
	zkLogger.DebugF(LoggerTag, "GetTraceStore: redisConfig=%v", redisConfig)
	_redisClient := config.GetRedisConnection(dbName, redisConfig)
	traceStore := TraceStore{redisClient: _redisClient, ttlForTransientSets: ttlForTransientSets}.initialize()
	return traceStore
}

func (t TraceStore) GetAllKeys() ([]string, error) {
	var cursor uint64
	var allKeys []string
	var err error

	for {
		var scanResult []string
		scanResult, cursor, err = t.redisClient.Scan(ctx, cursor, "*", 0).Result()
		if err != nil {
			fmt.Println("Error scanning keys:", err)
			return nil, err
		}

		allKeys = append(allKeys, scanResult...)

		if cursor == 0 {
			break
		}
	}
	return allKeys, nil
}

func (t TraceStore) Add(setName string, key string) error {
	// set a value in a set
	_, err := t.redisClient.SAdd(ctx, setName, key).Result()
	if err != nil {
		fmt.Printf("Error setting the key %s in set %s : %v\n", key, setName, err)
	}
	return err
}

func (t TraceStore) GetAllValuesFromSet(setName string) ([]string, error) {
	// Get all members of a set
	return t.redisClient.SMembers(ctx, setName).Result()
}

func (t TraceStore) NewUnionSet(resultKey string, keys ...string) error {

	if !t.readyForSetAction(resultKey, keys...) {
		return nil
	}

	// Perform union of sets and store the result in a new set
	_, err := t.redisClient.SUnionStore(ctx, resultKey, keys...).Result()
	if err != nil {
		fmt.Println("Error performing union and store:", err)
	}
	return t.SetExpiryForSet(resultKey, t.ttlForTransientSets)
}

func (t TraceStore) NewIntersectionSet(resultKey string, keys ...string) error {

	if !t.readyForSetAction(resultKey, keys...) {
		return nil
	}

	// Perform intersection of sets and store the result in a new set
	_, err := t.redisClient.SInterStore(ctx, resultKey, keys...).Result()
	if err != nil {
		fmt.Println("Error performing intersection and store:", err)
		return err
	}

	return t.SetExpiryForSet(resultKey, t.ttlForTransientSets)
}

func (t TraceStore) readyForSetAction(resultSet string, keys ...string) bool {
	// if the resultSet is equal to the only set present in keys, then no need to perform intersection
	if len(keys) == 1 && keys[0] == resultSet {
		return false
	}

	// if resultset is not found in keys, then delete it
	foundInKeys := false
	for _, key := range keys {
		if key == resultSet {
			foundInKeys = true
		}
	}
	if !foundInKeys {
		t.redisClient.Del(ctx, resultSet)
	}

	return true
}

func (t TraceStore) SetExpiryForSet(resultKey string, expiration time.Duration) error {
	_, err := t.redisClient.Expire(ctx, resultKey, expiration).Result()
	if err != nil {
		fmt.Println("Error setting expiry for set :", resultKey, err)
	}
	return err
}

func (t TraceStore) RenameSet(key, newKey string) error {
	_, err := t.redisClient.Rename(ctx, key, newKey).Result()
	if err != nil {
		fmt.Println("Renaming set:", key, "to", newKey)
		zkLogger.Error(LoggerTag, "Error renaming set:", err, key)
	}
	return err
}

func (t TraceStore) SetExists(key string) bool {
	exists, err := t.redisClient.Exists(ctx, key).Result()
	if err != nil {
		zkLogger.Error(LoggerTag, "Error checking if set exists:", err)
	}
	return exists == 1
}

func (t TraceStore) DeleteSet(keys []string) {
	if len(keys) == 0 {
		return
	}
	t.redisClient.Del(ctx, keys...)
}
