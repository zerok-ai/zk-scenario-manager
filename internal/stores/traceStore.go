package stores

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"time"
)

const LoggerTagTraceStore = "traceStore"

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

func GetTraceStore(redisConfig *config.RedisConfig, ttlForTransientSets time.Duration) *TraceStore {
	dbName := "traces"
	zkLogger.Debug(LoggerTagTraceStore, "GetTraceStore: config=", redisConfig, "dbName=", dbName, "dbID=", redisConfig.DBs[dbName])
	readTimeout := time.Duration(redisConfig.ReadTimeout) * time.Second
	_redisClient := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprint(redisConfig.Host, ":", redisConfig.Port),
		Password:    "",
		DB:          redisConfig.DBs[dbName],
		ReadTimeout: readTimeout,
	})

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
		zkLogger.Error(LoggerTagTraceStore, "Error renaming set:", err, key)
	}
	return err
}

func (t TraceStore) SetExists(key string) bool {
	exists, err := t.redisClient.Exists(ctx, key).Result()
	if err != nil {
		zkLogger.Error(LoggerTagTraceStore, "Error checking if set exists:", err)
	}
	return exists == 1
}
