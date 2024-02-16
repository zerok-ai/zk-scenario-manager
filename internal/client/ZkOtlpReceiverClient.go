package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	zkhttp "github.com/zerok-ai/zk-utils-go/http"
	zklogger "github.com/zerok-ai/zk-utils-go/logs"
	badgerResponse "github.com/zerok-ai/zk-utils-go/proto"
	"io"
	promMetrics "scenario-manager/internal/metrics"
	"time"
)

const ZkOtlpReceiverLogTag = "ZkOtlpReceiverClient"

func GetSpanData(nodeIp string, traceIdPrefixList []string, nodePort string) (*badgerResponse.BadgerResponseList, error) {
	url := "http://" + nodeIp + ":" + nodePort + "/get-trace-data" // Replace with your actual API endpoint
	requestBody, err := json.Marshal(traceIdPrefixList)
	if err != nil {
		zklogger.Error(fmt.Sprintf("Error marshaling request data: %s while calling OTLP receiver", traceIdPrefixList), err)
		return nil, err
	}
	promMetrics.TotalSpanDataFetchCalls.WithLabelValues(nodeIp).Inc()
	//total traces span data requested from receiver
	promMetrics.TotalTracesSpanDataRequestedFromReceiver.WithLabelValues(nodeIp).Add(float64(len(traceIdPrefixList)))

	response, zkErr := zkhttp.CreateWithTimeout(time.Second*20).Post(url, bytes.NewBuffer(requestBody))
	if zkErr != nil {
		promMetrics.TotalSpanDataFetchErrors.WithLabelValues(nodeIp).Inc()
		zklogger.ErrorF(ZkOtlpReceiverLogTag, "Error making HTTP request to OTLP receiver for ip = %s with err: %v.\n", nodeIp, err)
		zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Error Status  from OTLP receiver: %v", zkErr.Error.Status))
		return nil, errors.New(zkErr.Error.Message)
	}

	if response.StatusCode != 200 {
		promMetrics.TotalSpanDataFetchErrors.WithLabelValues(nodeIp).Inc()
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status  from OTLP receiver: %s", response.Status))
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status code from OTLP receiver: %v", response.StatusCode))
		return nil, errors.New(response.Status) //TODO: return error
	}

	promMetrics.TotalSpanDataFetchSuccess.WithLabelValues(nodeIp).Inc()

	// Read the response body
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Error while reading the response body from OTLP receiver"), err)
		return nil, err
	}

	var result badgerResponse.BadgerResponseList
	err = proto.Unmarshal(responseBody, &result)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, "Error while unmarshalling data from badger for given tracePrefixList", err)
	}

	return &result, nil
}
