package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"relay/internal/types"
	"time"
)

type RelayClient struct {
	RelayApiUrl string
	HttpClient  http.Client
}

type EventResponseBody struct {
	Content string
}

type EventResponse struct {
	Dispatched bool
	Body       EventResponseBody
}

func NewRelayClient(host string, port string) (*RelayClient, error) {
	if host == "" || port == "" {
		return nil, fmt.Errorf("Host or port in not correct")
	}

	client := http.Client{
		Timeout: 10 * time.Second,
	}

	return &RelayClient{
		RelayApiUrl: "http://" + host + ":" + port,
		HttpClient:  client,
	}, nil
}

func (c *RelayClient) post(ctx context.Context, evt types.Event) ([]byte, error) {
	body, err := json.Marshal(evt)
	if err != nil {
		return []byte{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.RelayApiUrl, bytes.NewBuffer(body))
	if err != nil {
		return []byte{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}
	return respBody, nil
}

func (c *RelayClient) DispatchEvent(ctx context.Context, event types.Event) (EventResponse, error) {
	resp, err := c.post(ctx, event)
	if err != nil {
		return EventResponse{}, err
	}
	var respBody EventResponseBody
	jsonErr := json.Unmarshal(resp, &respBody)
	if jsonErr != nil {
		return EventResponse{}, fmt.Errorf("Json convert failed")
	}
	return EventResponse{
		Dispatched: err == nil,
		Body:       respBody,
	}, nil
}
