package stores

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	clientDBNames "github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"regexp"
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
	_redisClient := config.GetRedisConnection(dbName, redisConfig)
	traceStore := TraceStore{redisClient: _redisClient, ttlForTransientSets: ttlForTransientSets}.initialize()
	return traceStore
}

func (t TraceStore) GetIndexOfScenarioToProcess() (int64, error) {
	key := "current_scenario"
	result, err := t.redisClient.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return result, nil
}

type RedisEntry struct {
	Key        string
	Value      string
	ExpiryTime time.Duration
}

func (t TraceStore) SetKeysIfDoNotExist(entries []RedisEntry) bool {

	// Create a transaction pipeline
	pipe := t.redisClient.TxPipeline()

	// Queue commands within the transaction
	for _, entry := range entries {
		pipe.SetNX(ctx, entry.Key, entry.Value, entry.ExpiryTime)
	}

	// Execute the transaction
	_, err := pipe.Exec(ctx)

	if err != nil {
		fmt.Printf("Error executing transaction: %v\n", err)
		return false
	}
	return true
}

type RedisDecrByEntry struct {
	Key       string
	Decrement int
}

func (t TraceStore) DecrementKeys(entries []RedisDecrByEntry) bool {

	// Create a transaction pipeline
	pipe := t.redisClient.TxPipeline()

	// Queue commands within the transaction
	for _, entry := range entries {
		pipe.DecrBy(ctx, entry.Key, int64(entry.Decrement))
	}

	// Execute the transaction
	_, err := pipe.Exec(ctx)

	if err != nil {
		fmt.Printf("Error executing transaction: %v\n", err)
		return false
	}
	return true
}

func (t TraceStore) GetValuesForKeys(keyPattern string) (map[string]string, error) {

	// Use the SCAN command to get all keys matching the pattern
	iter := t.redisClient.Scan(ctx, 0, keyPattern, 0).Iterator()

	// Start a transaction
	pipe := t.redisClient.TxPipeline()

	// Iterate over the matched keys
	for iter.Next(ctx) {
		key := iter.Val()

		// Queue a GET command for each Key in the transaction
		pipe.Get(ctx, key)
	}

	// Execute the transaction
	results, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Printf("Error executing transaction: %v\n", err)
		return nil, err
	}

	// Process the results
	resultMap := make(map[string]string)
	for _, result := range results {
		if result.Err() == nil {
			key := iter.Val()
			value := result.(*redis.StringCmd).Val()
			fmt.Printf("Key: %s, Value: %s\n", key, value)
			resultMap[key] = value
		}
	}

	return resultMap, nil
}

func (t TraceStore) GetAllKeysWithPrefixAndRegex(prefix, regex string) ([]string, error) {
	//var cursor uint64
	var allKeys []string
	var err error

	//var scanResult []string
	//
	//// Specify the key pattern
	keyPattern := prefix + "*"
	//scanResult, cursor, err = t.redisClient.Scan(ctx, cursor, keyPattern, 0).Result()

	// Use the SCAN command to get all keys matching the pattern
	iter := t.redisClient.Scan(ctx, 0, keyPattern, 0).Iterator()

	// Compile the regex pattern
	re := regexp.MustCompile(prefix + regex)

	if err != nil {
		zkLogger.Error(LoggerTag, "Error scanning keys:", err)
		return nil, err
	}

	// Iterate over the matched keys
	for iter.Next(ctx) {
		key := iter.Val()

		// Check if the key matches the pattern
		if matches := re.FindStringSubmatch(key); matches != nil {
			allKeys = append(allKeys, key)
		}
	}

	if err := iter.Err(); err != nil {
		fmt.Printf("Error scanning keys: %v\n", err)
	}

	return allKeys, nil
}

func (t TraceStore) Add(setName string, members []interface{}) error {
	// set a Value in a set
	_, err := t.redisClient.SAdd(ctx, setName, members...).Result()
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Error setting the Keys %v in set %s : %v\n", members, setName, err)
	}
	return err
}

func (t TraceStore) GetAllValuesFromSet(setName string) ([]string, error) {
	// Get all members of a set
	return t.redisClient.SMembers(ctx, setName).Result()
}

func (t TraceStore) NewUnionSet(resultKey string, keys ...string) bool {

	if len(keys) == 0 || !t.readyForSetAction(resultKey, keys...) {
		return false
	}

	// Perform union of sets and store the result in a new set
	result, err := t.redisClient.SUnionStore(ctx, resultKey, keys...).Result()
	if err == nil && result > 0 {
		t.SetExpiryForSet(resultKey, t.ttlForTransientSets)
		return true
	}

	if err != nil {
		zkLogger.Error("Error performing union and store:", err)
	}
	return false
}

func (t TraceStore) NewIntersectionSet(resultKey string, keys ...string) bool {

	if len(keys) == 0 || !t.readyForSetAction(resultKey, keys...) {
		return false
	}

	// Perform intersection of sets and store the result in a new set
	result, err := t.redisClient.SInterStore(ctx, resultKey, keys...).Result()
	if err == nil && result > 0 {
		t.SetExpiryForSet(resultKey, t.ttlForTransientSets)
		return true
	}

	if err != nil {
		zkLogger.Error("Error performing intersection and store:", err)
	}
	return false

}

func (t TraceStore) GetValuesAfterSetDiff(setLeft, setRight string) []string {
	// Calculate the set difference: setLeft - setRight
	result, err := t.redisClient.SDiff(ctx, setLeft, setRight).Result()
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Failed to calculate the set difference: %v", err)
	}
	return result
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

func (t TraceStore) SetExpiryForSet(resultKey string, expiration time.Duration) bool {
	_, err := t.redisClient.Expire(ctx, resultKey, expiration).Result()
	if err != nil {
		zkLogger.Error(LoggerTag, "Error setting expiry for set :", resultKey, err)
		return false
	}
	return true
}

func (t TraceStore) RenameSet(key, newKey string) bool {
	_, err := t.redisClient.Rename(ctx, key, newKey).Result()
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Error renaming set:%s to %s err=%v", key, newKey, err)
		return false
	}
	return true
}

func (t TraceStore) SetExists(key string) bool {
	exists, err := t.redisClient.Exists(ctx, key).Result()
	if err != nil {
		zkLogger.ErrorF(LoggerTag, "Error checking if set %s exists: %v", key, err)
	}
	return exists == 1
}

func (t TraceStore) DeleteSets(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return t.redisClient.Del(ctx, keys...).Err()
}

func (t TraceStore) SetValueForKeyWithExpiry(key, value string, expiration time.Duration) error {
	err := t.redisClient.Set(ctx, key, value, expiration).Err()
	return err
}

func (t TraceStore) SetValueForKeyWithExpiryIfNotExist(key, value string, expiration time.Duration) (bool, error) {
	return t.redisClient.SetNX(ctx, key, value, expiration).Result()
}

func (t TraceStore) GetValueForKey(key string) string {
	result, err := t.redisClient.Get(ctx, key).Result()
	if err != nil {
		return ""
	}
	return result
}
