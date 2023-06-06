package filters

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/storage/model"
	"time"
)

type TraceStore struct {
	redisClient *redis.Client
}

func (t TraceStore) initialize() *TraceStore {
	return &t
}

func GetTraceStore(redisConfig *model.RedisConfig) *TraceStore {

	dbName := "trace"
	if redisConfig == nil {
		return nil
	}
	readTimeout := time.Duration(redisConfig.ReadTimeout) * time.Second
	_redisClient := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprint(redisConfig.Host, ":", redisConfig.Port),
		Password:    "",
		DB:          redisConfig.DBs[dbName],
		ReadTimeout: readTimeout,
	})

	traceStore := TraceStore{redisClient: _redisClient}.initialize()

	return traceStore
}

func (t TraceStore) GetAllKeys() ([]string, error) {
	var cursor uint64
	var allKeys []string
	for {
		scanResult, cursor, err := t.redisClient.Scan(context.Background(), cursor, "*", 0).Result()
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
	_, err := t.redisClient.SAdd(context.Background(), setName, key).Result()
	if err != nil {
		fmt.Printf("Error setting the key %s in set %s : %v\n", key, setName, err)
	}
	return err
}

func (t TraceStore) GetAllValues(setName string) (*[]string, error) {
	// Get all members of a set
	result, err := t.redisClient.SMembers(context.Background(), setName).Result()
	if err != nil {
		fmt.Println("Error performing union and store:", err)
		return nil, err
	}
	return &result, nil
}

func (t TraceStore) NewUnionSet(resultSet string, keys ...string) error {
	// Perform union of sets and store the result in a new set
	t.redisClient.Del(context.Background(), resultSet)
	_, err := t.redisClient.SUnionStore(context.Background(), resultSet, keys...).Result()
	if err != nil {
		fmt.Println("Error performing union and store:", err)
	}
	return err
}

func (t TraceStore) NewIntersectionSet(resultSet string, keys ...string) error {
	// Perform intersection of sets and store the result in a new set
	t.redisClient.Del(context.Background(), resultSet)
	_, err := t.redisClient.SInterStore(context.Background(), resultSet, keys...).Result()
	if err != nil {
		fmt.Println("Error performing intersection and store:", err)
	}
	return err
}
