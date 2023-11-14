package stores

import (
	"encoding/json"
	"github.com/adjust/rmq/v5"
	"github.com/redis/go-redis/v9"
	zkLogger "github.com/zerok-ai/zk-utils-go/logs"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"time"
)

const (
	queueTag = "Scenario_Manager_Queue"
)

type TraceQueue struct {
	redisClient *redis.Client
	taskQueue   rmq.Queue
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

		// 1. Start consuming (yes, you start consuming before setting the consumer)
		if err = queue.taskQueue.StartConsuming(2, time.Second); err != nil {
			return nil, err
		}

		// 2. Add the consumer
		if _, err = queue.taskQueue.AddConsumer(name, consumer); err != nil {
			return nil, err
		}
	}
	zkLogger.InfoF("Initialized the consumer: %s", name)
	return queue, err
}

func initialize(redisConfig config.RedisConfig, queueName string) (*TraceQueue, error) {
	dbName := clientDBNames.FilteredTracesDBName

	// 1. get the redis client
	_redisClient := config.GetRedisConnection(dbName, redisConfig)

	// 2. get the connection and taskQueue
	connection, err := rmq.OpenConnectionWithRedisClient(queueTag+"_"+queueName, _redisClient, nil)
	if err != nil {
		return nil, err
	}
	queue, err := connection.OpenQueue(queueName)
	if err != nil {
		return nil, err
	}

	// 3. create the TraceQueue
	telQueue := TraceQueue{redisClient: _redisClient, taskQueue: queue}
	return &telQueue, nil
}

func (t TraceQueue) PublishTracesToQueue(message any) error {

	taskBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return t.taskQueue.PublishBytes(taskBytes)
}
