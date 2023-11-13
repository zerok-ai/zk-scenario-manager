package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	logger "github.com/zerok-ai/zk-utils-go/logs"
	zkconfig "github.com/zerok-ai/zk-utils-go/storage/redis/config"
	zktick "github.com/zerok-ai/zk-utils-go/ticker"
	"time"
)

var redisHandlerLogTag = "RedisHandler"

type RedisHandler struct {
	RedisClient  *redis.Client
	ctx          context.Context
	config       *zkconfig.RedisConfig
	dbName       string
	Pipeline     redis.Pipeliner
	ticker       *zktick.TickerTask
	count        int
	startTime    time.Time
	batchSize    int
	syncInterval int
	taskName     string
}

func NewRedisHandlerWithoutTicker(redisConfig *zkconfig.RedisConfig, dbName string, taskName string) (*RedisHandler, error) {
	handler := RedisHandler{
		ctx:    context.Background(),
		config: redisConfig,
		dbName: dbName,
	}

	err := handler.InitializeRedisConn()
	if err != nil {
		logger.Error(redisHandlerLogTag, "Error while initializing redis connection ", err)
		return nil, err
	}
	handler.taskName = taskName
	return &handler, nil
}

func NewRedisHandler(redisConfig *zkconfig.RedisConfig, dbName string, syncInterval int, batchSize int, tag string) (*RedisHandler, error) {
	handler := RedisHandler{
		ctx:    context.Background(),
		config: redisConfig,
		dbName: dbName,
	}

	err := handler.InitializeRedisConn()
	if err != nil {
		logger.Error(redisHandlerLogTag, "Error while initializing redis connection ", err)
		return nil, err
	}

	handler.Pipeline = handler.RedisClient.Pipeline()

	timerDuration := time.Duration(syncInterval) * time.Second
	handler.ticker = zktick.GetNewTickerTask("sync_pipeline", timerDuration, handler.SyncPipeline)
	handler.ticker.Start()

	handler.syncInterval = syncInterval
	handler.batchSize = batchSize
	handler.ctx = context.Background()
	handler.taskName = tag

	return &handler, nil
}

func (h *RedisHandler) InitializeRedisConn() error {
	db := h.config.DBs[h.dbName]
	redisAddr := h.config.Host + ":" + h.config.Port
	opt := &redis.Options{
		Addr:     redisAddr,
		Password: h.config.Password,
		DB:       db,
	}
	redisClient := redis.NewClient(opt)

	h.RedisClient = redisClient
	err := h.PingRedis()
	if err != nil {
		return err
	}
	return nil
}

func (h *RedisHandler) Set(key string, value interface{}) error {
	statusCmd := h.RedisClient.Set(h.ctx, key, value, 0)
	return statusCmd.Err()
}

func (h *RedisHandler) Get(key string) (string, error) {
	result, err := h.RedisClient.Get(h.ctx, key).Result()
	if err == redis.Nil {
		// Key does not exist
		return "", nil
	} else if err != nil {
		logger.Error(redisHandlerLogTag, "Failed to get key from Redis:", err)
		return "", err
	}
	return result, nil
}

func (h *RedisHandler) SetNX(key string, value interface{}) error {
	statusCmd := h.RedisClient.SetNX(h.ctx, key, value, 0)
	return statusCmd.Err()
}

func (h *RedisHandler) HSet(key string, value interface{}) error {
	statusCmd := h.RedisClient.HSet(h.ctx, key, value, 0)
	return statusCmd.Err()
}

func (h *RedisHandler) HMSet(key string, value interface{}) error {
	statusCmd := h.RedisClient.HMSet(h.ctx, key, value)
	return statusCmd.Err()
}

func (h *RedisHandler) PingRedis() error {
	redisClient := h.RedisClient
	if redisClient == nil {
		logger.Error(redisHandlerLogTag, "Redis client is nil.")
		return fmt.Errorf("redis client is nil")
	}
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Error(redisHandlerLogTag, "Error caught while pinging redis ", err)
		return err
	}
	return nil
}

func (h *RedisHandler) HMSetPipeline(key string, value map[string]string, expiration time.Duration) error {
	cmd := h.Pipeline.HMSet(h.ctx, key, value)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return h.setExpiry(key, expiration)
}

func (h *RedisHandler) GetKeysByPattern(pattern string) ([]string, error) {
	keys, err := h.RedisClient.Keys(h.ctx, pattern).Result()
	if err != nil {
		logger.Error(redisHandlerLogTag, fmt.Sprintf("Error retrieving keys for pattern %s: ", pattern), err)
		return nil, err
	}
	return keys, nil
}

func (h *RedisHandler) SetWithTTL(key string, value interface{}, ttl time.Duration) error {
	return h.RedisClient.Set(h.ctx, key, value, ttl).Err()
}

func (h *RedisHandler) RemoveKey(key string) error {
	_, err := h.RedisClient.Del(h.ctx, key).Result()
	if err != nil {
		return err
	}
	return nil
}

func (h *RedisHandler) RenameKeyWithTTL(oldKey string, newKey string, ttl time.Duration) error {
	// Use the RENAME command to rename the key.
	_, err := h.RedisClient.Rename(h.ctx, oldKey, newKey).Result()
	if err != nil {
		if err == redis.Nil {
			logger.Error(redisHandlerLogTag, "Key does not exist in redis ", oldKey)
		}
		return err
	}

	// Set the TTL on the new key.
	_, err = h.RedisClient.Expire(h.ctx, newKey, ttl).Result()
	if err != nil {
		return err
	}

	return nil
}

func (h *RedisHandler) SetNXPipeline(key string, value interface{}, expiration time.Duration) error {
	cmd := h.Pipeline.SetNX(h.ctx, key, value, expiration)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return h.setExpiry(key, expiration)
}

func (h *RedisHandler) SAddPipeline(key string, value interface{}, expiration time.Duration) error {
	cmd := h.Pipeline.SAdd(h.ctx, key, value)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return h.setExpiry(key, expiration)
}

func (h *RedisHandler) setExpiry(key string, expiration time.Duration) error {
	if expiration > 0 {
		cmd := h.Pipeline.Expire(h.ctx, key, expiration)
		if cmd.Err() != nil {
			return cmd.Err()
		}
	}
	h.count++
	return nil
}

func (h *RedisHandler) CheckRedisConnection() error {
	err := h.PingRedis()
	if err != nil {
		//Closing redis connection.
		err = h.CloseConnection()
		if err != nil {
			logger.Error(redisHandlerLogTag, "Failed to close Redis connection: ", err)
			return err
		}
		err = h.InitializeRedisConn()
		if err != nil {
			logger.Error(redisHandlerLogTag, "Error while initializing redis connection ", err)
			return err
		}
	}
	return nil
}

func (h *RedisHandler) SyncPipeline() {
	syncDuration := time.Duration(h.syncInterval) * time.Second
	if h.count > h.batchSize || time.Since(h.startTime) >= syncDuration {
		_, err := h.Pipeline.Exec(h.ctx)
		if err != nil {
			logger.Error(redisHandlerLogTag, "Error while syncing data to redis ", err)
			return
		}
		logger.Debug(redisHandlerLogTag, "Pipeline synchronized on batchsize/syncDuration for taskName ", h.taskName)

		h.count = 0
		h.startTime = time.Now()
	}
}

func (h *RedisHandler) CloseConnection() error {
	return h.RedisClient.Close()
}

func (h *RedisHandler) forceSync() {
	_, err := h.Pipeline.Exec(h.ctx)
	if err != nil {
		logger.Error(redisHandlerLogTag, "Error while force syncing data to redis ", err)
		return
	}
}

func (h *RedisHandler) shutdown() {
	h.forceSync()
	err := h.CloseConnection()
	if err != nil {
		logger.Error(redisHandlerLogTag, "Error while closing redis conn.")
		return
	}
}
