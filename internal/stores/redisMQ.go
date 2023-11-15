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

type TraceQueue struct {
	redisClient *redis.Client
	taskQueue   rmq.Queue
}

func (t TraceQueue) Close() {
	t.redisClient.Close()
}

func GetTraceProducer(redisConfig config.RedisConfig, name string) (*TraceQueue, error) {
	return initialize(redisConfig, "producer_"+name, name)
}

func GetTraceConsumer(redisConfig config.RedisConfig, consumer rmq.Consumer, name string) (*TraceQueue, error) {
	queue, err := initialize(redisConfig, "consumer_"+name, name)
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
	zkLogger.InfoF(LoggerTag, "Initialized the consumer: %s", name)
	return queue, err
}

func initialize(redisConfig config.RedisConfig, queueTag, queueName string) (*TraceQueue, error) {
	dbName := clientDBNames.FilteredTracesDBName

	// 1. get the redis client
	_redisClient := config.GetRedisConnection(dbName, redisConfig)

	errChan := make(chan error)
	go logErrors(queueTag, errChan)

	// 2. get the connection and taskQueue
	connection, err := rmq.OpenConnectionWithRedisClient(queueTag, _redisClient, errChan)
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

func logErrors(name string, errChan <-chan error) {
	for e := range errChan {
		switch err := e.(type) {
		case *rmq.HeartbeatError:
			if err.Count == rmq.HeartbeatErrorLimit {
				zkLogger.Error(LoggerTag, name+" heartbeat error (limit): ", err)
			} else {
				zkLogger.Error(LoggerTag, name+" heartbeat error: ", err)
			}
		case *rmq.ConsumeError:
			zkLogger.Error(LoggerTag, name+" consume error: ", err)
		case *rmq.DeliveryError:
			zkLogger.Error(LoggerTag, name+" delivery error: ", err.Delivery, err)
		default:
			zkLogger.Error(LoggerTag, name+" other error: ", err)
		}
	}
	zkLogger.Error(LoggerTag, "logErrors function exited")
}

func (t TraceQueue) PublishTracesToQueue(message any) error {

	taskBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return t.taskQueue.PublishBytes(taskBytes)
}
