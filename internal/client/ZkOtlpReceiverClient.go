package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func GetSpanData(nodeIp string, traceIdPrefixList []string, nodePort string) (map[string]string, error) {

	url := "http://" + nodeIp + ":" + nodePort + "/get-trace-data" // Replace with your actual API endpoint

	requestBody, err := json.Marshal(traceIdPrefixList)
	if err != nil {
		fmt.Println("Error marshaling request data:", err)
		return nil, err
	}

	// Make the HTTP POST request
	response, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		fmt.Println("Error making HTTP request:", err)
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing response body:", err)
		}
	}(response.Body)

	// Read the response body
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}

	var responseData map[string]string
	// Parse the response JSON
	err = json.Unmarshal(responseBody, &responseData)
	if err != nil {
		fmt.Println("Error unmarshaling response data:", err)
		return nil, err
	}

	return responseData, nil
}
