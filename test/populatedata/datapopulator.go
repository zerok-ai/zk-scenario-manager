package populatedata

import (
	"encoding/json"
	"fmt"
	"github.com/zerok-ai/zk-utils-go/scenario/model"
	"github.com/zerok-ai/zk-utils-go/storage/redis/config"
	"io"
	"os"
)

func PopulateScenarios(redisConfig config.RedisConfig) {

	scenarioString := GetBytesFromFile("test/populatedata/files/Scenario-1.json")
	var scenario model.Scenario
	err := json.Unmarshal(scenarioString, &scenario)
	if err != nil {
		fmt.Println("Error in unmarshalling scenario", err)
		return
	}

	scenarioPopulator := GetScenarioPopulator("scenarios", redisConfig)
	err = scenarioPopulator.AddScenario(scenario)
}

func PopulateTraces(redisConfig config.RedisConfig) {
	GetSetPopulator(redisConfig, "traces", "idA1", "", 1, 40).PopulateData()
	GetSetPopulator(redisConfig, "traces", "idA2", "", 76, 15).PopulateData()

	GetSetPopulator(redisConfig, "traces", "idB1", "", 16, 50).PopulateData()
	GetSetPopulator(redisConfig, "traces", "idB2", "", 73, 13).PopulateData()

	GetSetPopulator(redisConfig, "traces", "idC1", "", 21, 10).PopulateData()
	GetSetPopulator(redisConfig, "traces", "idC2", "", 71, 10).PopulateData()
}

func GetBytesFromFile(path string) []byte {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	// Read the file content
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return nil
	}

	return content
}
