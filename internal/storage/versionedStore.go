package storage

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"scenario-manager/internal/config"
	"time"
)

type VersionedStore struct {
	redisClient        *redis.Client
	versionHashSetName string
	hmap               map[string]string
}

type Version struct {
	key     string
	version int
}

func GetVersionedStore(appConfig config.AppConfigs, db int) *VersionedStore {

	redisConfig := appConfig.Redis
	readTimeout := time.Duration(redisConfig.ReadTimeout) * time.Second
	_redisClient := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprint(redisConfig.Host, ":", redisConfig.Port),
		Password:    "",
		DB:          db,
		ReadTimeout: readTimeout,
	})

	//_redisClient.Expire(versionHashSetName, defaultExpiry)

	imgRedis := &VersionedStore{
		redisClient:        _redisClient,
		versionHashSetName: "zk_value_version",
	}

	//if _redisClient.Del(versionHashSetName).Err() != nil {
	//	fmt.Println("couldn't delete hashtable " + versionHashSetName)
	//}

	return imgRedis
}

func (zkRedis VersionedStore) SetValue(key string, value string) error {
	rdb := zkRedis.redisClient

	// get the old value
	oldVal := rdb.Get(context.Background(), key)
	if err := oldVal.Err(); err != nil {
		return err
	}
	oldString := oldVal.Val()

	// return if old value is same as new value
	if oldString == value {
		return nil
	}

	// Create a Redis transaction: this doesn't support rollback
	ctx := context.Background()
	tx := rdb.TxPipeline()

	// set value and version
	tx.Set(ctx, key, value, 0)
	tx.HIncrBy(ctx, zkRedis.versionHashSetName, key, 1)

	// Execute the transaction
	if _, err := tx.Exec(ctx); err != nil {
		return err
	}

	return nil
}

func (zkRedis VersionedStore) GetAllVersions() (map[string]string, error) {
	rdb := zkRedis.redisClient

	// get the old value
	versions := rdb.HGetAll(context.Background(), zkRedis.versionHashSetName)
	return versions.Val(), versions.Err()
}

func (zkRedis VersionedStore) GetValue(key string) (string, error) {
	rdb := zkRedis.redisClient

	// get the old value
	value := rdb.Get(context.Background(), key)
	return value.Val(), value.Err()
}

func (zkRedis VersionedStore) GetValuesForKeys(keys []string) ([]interface{}, error) {
	rdb := zkRedis.redisClient

	// get the values
	return rdb.MGet(context.Background(), keys...).Result()
}

func (zkRedis VersionedStore) GetVersion(key string) (string, error) {
	rdb := zkRedis.redisClient

	// get the old value
	version := rdb.HGet(context.Background(), zkRedis.versionHashSetName, key)
	return version.Val(), version.Err()
}

func (zkRedis VersionedStore) Delete(key string) error {
	rdb := zkRedis.redisClient

	// create a transaction
	ctx := context.Background()
	tx := rdb.TxPipeline()

	// delete version
	tx.HDel(context.Background(), zkRedis.versionHashSetName, key)
	tx.Del(context.Background(), key)

	// Execute the transaction
	if _, err := tx.Exec(ctx); err != nil {
		return err
	}
	return nil
}

func (zkRedis VersionedStore) Length() (int64, error) {
	// get the number of hash key-value pairs
	return zkRedis.redisClient.HLen(context.Background(), zkRedis.versionHashSetName).Result()
}
