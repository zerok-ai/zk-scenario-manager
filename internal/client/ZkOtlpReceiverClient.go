package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	zkhttp "github.com/zerok-ai/zk-utils-go/http"
	zklogger "github.com/zerok-ai/zk-utils-go/logs"
	"io"
)

const ZkOtlpReceiverLogTag = "ZkOtlpReceiverClient"

var (
	totalSpanDataFetchCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_span_data_fetch_calls",
		Help: "Total spans data fetch calls to receiver by scenario manager",
	},
		[]string{"node_ip"})

	totalSpanDataFetchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_span_data_fetch_errors",
		Help: "Total spans data fetch errors to receiver by scenario manager",
	},
		[]string{"node_ip"})

	totalSpanDataFetchSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_span_data_fetch_success",
		Help: "Total spans data fetch success to receiver by scenario manager",
	},
		[]string{"node_ip"})

	totalTracesSpanDataRequestedFromReceiver = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zerok_sm_total_traces_span_data_requested_from_receiver",
		Help: "Total traces span data requested from receiver by scenario manager",
	},
		[]string{"node_ip"})
)

func GetSpanData(nodeIp string, traceIdPrefixList []string, nodePort string) (map[string]string, error) {
	totalSpanDataFetchCalls.WithLabelValues(nodeIp).Inc()
	url := "http://" + nodeIp + ":" + nodePort + "/get-trace-data" // Replace with your actual API endpoint
	zklogger.Info(ZkOtlpReceiverLogTag, fmt.Sprintf("Calling OTLP receiver with traceIdPrefixList: %s on url: %s", traceIdPrefixList, url))
	requestBody, err := json.Marshal(traceIdPrefixList)
	if err != nil {
		zklogger.Error(fmt.Sprintf("Error marshaling request data: %s while calling OTLP receiver", traceIdPrefixList), err)
		return nil, err
	}
	//total traces span data requested from receiver
	totalTracesSpanDataRequestedFromReceiver.WithLabelValues(nodeIp).Add(float64(len(traceIdPrefixList)))

	response, zkErr := zkhttp.Create().Post(url, bytes.NewBuffer(requestBody))
	if zkErr != nil {
		totalSpanDataFetchErrors.WithLabelValues(nodeIp).Inc()
		zklogger.ErrorF(ZkOtlpReceiverLogTag, "Error making HTTP request to OTLP receiver for ip = %s with err: %v.\n", nodeIp, err)
		zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Error Status  from OTLP receiver: %v", zkErr.Error.Status))
		return nil, errors.New(zkErr.Error.Message) //TODO: return error
	}
	totalSpanDataFetchSuccess.WithLabelValues(nodeIp).Inc()
	zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status  from OTLP receiver: %s", response.Status))
	zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received Status code from OTLP receiver: %v", response.StatusCode))

	zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response from OTLP receiver: %s", response.Body))
	// Read the response body
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Error reading trace data request data from response body: %s", response.Body), err)
		return nil, err
	}
	zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response body from OTLP receiver: %s", responseBody))
	var responseData map[string]string
	// Parse the response JSON
	err = json.Unmarshal(responseBody, &responseData)
	if err != nil {
		zklogger.Error(ZkOtlpReceiverLogTag, fmt.Sprintf("Error unmarshaling response data: %s received from OTLP receiver", responseData), err)
		return nil, err
	}

	zklogger.Debug(ZkOtlpReceiverLogTag, fmt.Sprintf("Received response data from OTLP receiver: %s", responseData))

	return responseData, nil
}
