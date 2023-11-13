package stores

import (
	"encoding/json"
	"github.com/adjust/rmq/v5"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
)

const (
	queueTag = "Scenario_Manager_Queue"
)

type TraceQueue struct {
	redisClient *redis.Client
	queue       rmq.Queue
}

func (t TraceQueue) Close() {
	t.redisClient.Close()
}

func GetTraceProducer(redisConfig config.RedisConfig, name string) (*TraceQueue, error) {
	return initialize(redisConfig, name)
}

func GetTraceConsumer(redisConfig config.RedisConfig, consumer rmq.Consumer, name string) (*TraceQueue, error) {
	queue, err := initialize(redisConfig, name)
	if err == nil {
		_, err = queue.queue.AddConsumer(name, consumer)
		if err != nil {
			return nil, err
		}
	}
	return queue, err
}

func initialize(redisConfig config.RedisConfig, queueName string) (*TraceQueue, error) {
	dbName := clientDBNames.FilteredTracesDBName

	// 1. get the redis client
	_redisClient := config.GetRedisConnection(dbName, redisConfig)

	// 2. get the connection and queue
	connection, err := rmq.OpenConnectionWithRedisClient(queueTag, _redisClient, nil)
	if err != nil {
		return nil, err
	}
	queue, err := connection.OpenQueue(queueName)
	if err != nil {
		return nil, err
	}

	// 3. create the TraceQueue
	telQueue := TraceQueue{redisClient: _redisClient, queue: queue}
	return &telQueue, nil
}

func (t TraceQueue) PublishTracesToQueue(message any) error {

	taskBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return t.queue.PublishBytes(taskBytes)
}
