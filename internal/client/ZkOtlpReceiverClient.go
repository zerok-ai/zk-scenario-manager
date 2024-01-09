package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	zkhttp "github.com/zerok-ai/zk-utils-go/http"
	zklogger "github.com/zerok-ai/zk-utils-go/logs"
	"io"
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

	response, zkErr := zkhttp.Create().Post(url, bytes.NewBuffer(requestBody))
	zklogger.Info(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status  from OTLP receiver: %s", response.Status))
	zklogger.Info(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status code from OTLP receiver: %s", response.StatusCode))
	if zkErr != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, "Error making HTTP request to OTLP receiver: ", zkErr)
		return nil, nil //TODO: return error
	}

	zklogger.Info(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response from OTLP receiver: %s", response.Body))
	// Read the response body
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Error reading trace data request data from response body: %s", response.Body), err)
		return nil, err
	}
	zklogger.Info(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response body from OTLP receiver: %s", responseBody))
	var responseData map[string]string
	// Parse the response JSON
	err = json.Unmarshal(responseBody, &responseData)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Error unmarshaling response data: %s received from OTLP receiver", responseData), err)
		return nil, err
	}

	zklogger.Info(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response data from OTLP receiver: %s", responseData))

	return responseData, nil
}
