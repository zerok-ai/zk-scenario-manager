package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	zkhttp "github.com/zerok-ai/zk-utils-go/http"
	zklogger "github.com/zerok-ai/zk-utils-go/logs"
	"io"
	promMetrics "scenario-manager/internal/prometheusMetrics"
	"time"
)

const ZkOtlpReceiverLogTag = "ZkOtlpReceiverClient"

func GetSpanData(nodeIp string, traceIdPrefixList []string, nodePort string) (map[string]string, error) {

	url := "http://" + nodeIp + ":" + nodePort + "/get-trace-data" // Replace with your actual API endpoint
	zklogger.Info(ZkOtlpReceiverLogTag, fmt.Sprintf("Calling OTLP receiver with traceIdPrefixList: %s on url: %s", traceIdPrefixList, url))
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
	promMetrics.TotalSpanDataFetchSuccess.WithLabelValues(nodeIp).Inc()
	zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status  from OTLP receiver: %s", response.Status))
	zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status code from OTLP receiver: %v", response.StatusCode))

	//zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response from OTLP receiver: %s", response.Body))
	// Read the response body
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Error while reading the response body from OTLP receiver"), err)
		return nil, err
	}
	//zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response body from OTLP receiver: %s", responseBody))
	var responseData map[string]string
	// Parse the response JSON
	err = json.Unmarshal(responseBody, &responseData)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Error unmarshaling response data: %s received from OTLP receiver", responseData), err)
		return nil, err
	}

	//zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response data from OTLP receiver: %s", responseData))

	return responseData, nil
}
