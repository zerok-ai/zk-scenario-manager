package filters

import (
	"scenario-manager/internal/config"
	//"scenario-manager/internal/storage"
)

const (
	filterDb int = 8
)

type FilterProcessor struct {
	Filters        map[string]string //TODO this should be map[string]Filter
	filterVersions map[string]string
	//versionedStore *storage.VersionedStore
}

func NewFilterProcessor(appConfig config.AppConfigs) *FilterProcessor {
	//vs := storage.GetVersionedStore(appConfig, filterDb)
	//fltrs, _ := vs.GetAllVersions()
	return &FilterProcessor{
		//versionedStore: vs,
		//Filters:        fltrs,
	}
}

func (fp FilterProcessor) FetchNewFilters() {

	// 1. get the new filter versions for each filter
	//newFilterVersions, err := fp.versionedStore.GetAllVersions()
	//if err != nil {
	//	log.Println("Error in getting filter versions:", err)
	//	return
	//}
	//
	//// 2. collect the filters which have the same version in a new map
	//newFilters := map[string]string{}
	//missingOrOldFilters := []string{}
	//for key, newVersion := range newFilterVersions {
	//	oldVersion, ok := fp.filterVersions[key]
	//	if ok {
	//		if oldVersion == newVersion {
	//			newFilters[key] = fp.Filters[key]
	//			continue
	//		}
	//	}
	//	missingOrOldFilters = append(missingOrOldFilters, key)
	//}
	//
	//// 3. get the filters which don't exist
	//newRawFilters, err := fp.versionedStore.GetValuesForKeys(missingOrOldFilters)
	//if err != nil {
	//	log.Println("Error in getting newFilters:", err)
	//	return
	//}
	//loadInToFilterMap(missingOrOldFilters, newRawFilters, newFilters)
	//
	//// 4. assign the new objects to filter processors
	//fp.Filters = newFilters
	//fp.filterVersions = newFilterVersions
}

func loadInToFilterMap(keys []string, interfaceSlice []interface{}, stringMap map[string]string) map[string]string {
	for i, v := range interfaceSlice {
		str, ok := v.(string)
		if !ok {
			continue
		}
		stringMap[keys[i]] = str
	}
	return stringMap
}
