package populatedata

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	storage "github.com/zerok-ai/zk-utils-go/storage/redis"
)

var (
	ctx = context.Background()
)

type SetPopulator struct {
	redisClient  *redis.Client
	setName      string
	batchSize    int
	totalRecords int
	startCounter int
	keyPrefix    string
}

func GetSetPopulator(redisConfig *storage.RedisConfig, dbName string, setName string, keyPrefix string, startCounter int, totalRecords int) *SetPopulator {
	_redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprint(redisConfig.Host, ":", redisConfig.Port),
		Password: "",
		DB:       redisConfig.DBs[dbName],
	})

	sp := SetPopulator{
		redisClient:  _redisClient,
		setName:      setName,
		batchSize:    1000,
		totalRecords: totalRecords,
		keyPrefix:    keyPrefix,
		startCounter: startCounter,
	}
	return &sp
}

// PopulateData populates data with odd keys
func (sp SetPopulator) PopulateData() {

	var index int
	data := []string{}

	for {
		for i := 0; index < sp.totalRecords && i < sp.batchSize; i++ {
			index++
			obj := fmt.Sprintf("%s%d", sp.keyPrefix, index+sp.startCounter-1)
			data = append(data, obj)
		}

		// Use SAdd command to add the data to the set
		_, err := sp.redisClient.SAdd(ctx, sp.setName, data).Result()
		if err != nil {
			fmt.Println("Error adding data to Redis set:", err)
			return
		}

		if index >= sp.totalRecords {
			break
		}
	}

}
