package redis

import (
	"github.com/redis/go-redis/v9"
	"time"
)

// RedisHandlerInterface defines the methods that the RedisHandler must implement.
type RedisHandlerInterface interface {
	InitializeRedisConn() error
	Set(key string, value interface{}) error
	Get(key string) (string, error)
	SetNX(key string, value interface{}) error
	SetNXWithTTL(key string, value interface{}, ttl time.Duration) error
	HSet(key string, value interface{}) error
	HMSet(key string, value interface{}) error
	GetKeysByPattern(pattern string) ([]string, error)
	SetWithTTL(key string, value interface{}, ttl time.Duration) error
	RemoveKey(key string) error
	RenameKeyWithTTL(oldKey string, newKey string, ttl time.Duration) error
	CheckRedisConnection() error
	HGetAll(key string) (map[string]string, error)
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	Shutdown()
}
