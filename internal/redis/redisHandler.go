package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	logger "github.com/zerok-ai/zk-utils-go/logs"
	zkconfig "github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"time"
)

var redisHandlerLogTag = "RedisHandler"

type RedisHandler struct {
	RedisClient *redis.Client
	ctx         context.Context
	config      *zkconfig.RedisConfig
	dbName      string
}

func NewRedisHandler(redisConfig *zkconfig.RedisConfig, dbName string) (*RedisHandler, error) {
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

func (h *RedisHandler) HGetAll(key string) (map[string]string, error) {
	return h.RedisClient.HGetAll(h.ctx, key).Result()
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

// RenameKeyWithTTL Rename oldKey to newKey, only if the oldKey is present.
func (h *RedisHandler) RenameKeyWithTTL(oldKey string, newKey string, ttl time.Duration) error {
	// Use the RENAME command to rename the key.
	_, err := h.RedisClient.Rename(h.ctx, oldKey, newKey).Result()
	if err != nil {
		if err == redis.Nil {
			logger.Debug(redisHandlerLogTag, "Key does not exist in redis ", oldKey)
			return nil
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

func (h *RedisHandler) CloseConnection() error {
	return h.RedisClient.Close()
}

func (h *RedisHandler) Shutdown() {
	err := h.CloseConnection()
	if err != nil {
		logger.Error(redisHandlerLogTag, "Error while closing redis conn.")
		return
	}
}
