package filters

import (
	"fmt"
	"github.com/zerok-ai/zk-utils-go/interfaces"
	"github.com/zerok-ai/zk-utils-go/storage"
	"github.com/zerok-ai/zk-utils-go/storage/model"
	ticker "github.com/zerok-ai/zk-utils-go/ticker"
	"time"
)

type RedisVal string

func (s RedisVal) Equals(otherInterface interfaces.ZKComparable) bool {
	other, ok := otherInterface.(RedisVal)
	if !ok {
		return false
	}
	return s == other
}

type StringPopulator struct {
	counter        int
	versionedStore *storage.VersionedStore[RedisVal]
	taskName       string
	increment      int
	keyPrefix      string
}

func GetStringPopulator(taskName string, dbname string, redisConfig *model.RedisConfig, keyPrefix string, increment int) *StringPopulator {
	vs, err := storage.GetVersionedStore(redisConfig, dbname, false, RedisVal(""))
	if err != nil {
		return nil
	}
	sp := StringPopulator{
		versionedStore: vs,
		taskName:       taskName,
		increment:      increment,
		keyPrefix:      keyPrefix,
	}
	return &sp
}

func (sp StringPopulator) Start(timeInterval time.Duration) error {
	tickerTask := ticker.GetNewTickerTask(sp.taskName, timeInterval, func() {
		sp.populateData(sp.counter)
		sp.counter = sp.counter + sp.increment
	})
	tickerTask.Start()

	return nil
}

// populateData populates data with odd keys
func (sp StringPopulator) populateData(counter int) {
	key := fmt.Sprintf("%s%n", sp.keyPrefix, counter)
	err := sp.versionedStore.SetValue(key, RedisVal(key))
	if err != nil && err != storage.LATEST {
		fmt.Println("populateData error in setting value ", err.Error())
		return
	}
}
