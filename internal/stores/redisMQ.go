package stores

import (
	"encoding/json"
	"github.com/adjust/rmq/v5"
	"github.com/redis/go-redis/v9"
	"github.com/zerok-ai/zk-utils-go/storage/redis/clientDBNames"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"scenario-manager/internal/scenarioManager"
)

const (
	queueTag     = "OTel_Queue"
	producerName = "OTel_Producer"
	consumerName = "OTel_Consumer"
)

type OTelQueue struct {
	redisClient *redis.Client
	queue       rmq.Queue
}

func (t OTelQueue) Close() {
	t.redisClient.Close()
}

func GetOTelProducer(redisConfig config.RedisConfig) (*OTelQueue, error) {
	return initialize(redisConfig, producerName)
}

func GetOTelConsumer(redisConfig config.RedisConfig, consumer rmq.Consumer) (*OTelQueue, error) {
	queue, err := initialize(redisConfig, consumerName)
	if err == nil {
		_, err = queue.queue.AddConsumer(consumerName, consumer)
		if err != nil {
			return nil, err
		}
	}
	return queue, err
}

func initialize(redisConfig config.RedisConfig, queueName string) (*OTelQueue, error) {
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

	// 3. create the OTelQueue
	telQueue := OTelQueue{redisClient: _redisClient, queue: queue}
	return &telQueue, nil
}

func (t OTelQueue) PublishTracesToQueue(message scenarioManager.OTelMessage) error {

	taskBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return t.queue.PublishBytes(taskBytes)
}
