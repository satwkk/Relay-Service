package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"relay/internal/types"
)

type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Relay API returned %d: %s", e.StatusCode, e.Message)
}

func (c *RelayClient) requestJSON(ctx context.Context, method, path string, body, result any) error {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.RelayApiUrl, "/")+path, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&failure)
		if failure.Error == "" {
			failure.Error = http.StatusText(resp.StatusCode)
		}
		return &APIError{StatusCode: resp.StatusCode, Message: failure.Error}
	}
	if result == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func listQuery(options types.ListOptions) string {
	query := url.Values{}
	if options.Limit != 0 {
		query.Set("limit", strconv.Itoa(options.Limit))
	}
	if options.Offset != 0 {
		query.Set("offset", strconv.Itoa(options.Offset))
	}
	return "?" + query.Encode()
}

func (c *RelayClient) RegisterSubscriber(ctx context.Context, req types.SubscriberRequest) (types.Subscriber, error) {
	var row types.Subscriber
	err := c.requestJSON(ctx, http.MethodPost, "/subscribers", req, &row)
	return row, err
}

func (c *RelayClient) GetSubscriber(ctx context.Context, id int64) (types.Subscriber, error) {
	var row types.Subscriber
	err := c.requestJSON(ctx, http.MethodGet, fmt.Sprintf("/subscribers/%d", id), nil, &row)
	return row, err
}

func (c *RelayClient) ListSubscribers(ctx context.Context, options types.ListOptions) ([]types.Subscriber, error) {
	var rows []types.Subscriber
	err := c.requestJSON(ctx, http.MethodGet, "/subscribers"+listQuery(options), nil, &rows)
	return rows, err
}

func (c *RelayClient) UpdateSubscriber(ctx context.Context, id int64, req types.SubscriberRequest) (types.Subscriber, error) {
	var row types.Subscriber
	err := c.requestJSON(ctx, http.MethodPut, fmt.Sprintf("/subscribers/%d", id), req, &row)
	return row, err
}

func (c *RelayClient) DeleteSubscriber(ctx context.Context, id int64) error {
	return c.requestJSON(ctx, http.MethodDelete, fmt.Sprintf("/subscribers/%d", id), nil, nil)
}

func (c *RelayClient) Subscribe(ctx context.Context, req types.SubscriptionRequest) (types.Subscription, error) {
	var row types.Subscription
	err := c.requestJSON(ctx, http.MethodPost, "/subscriptions", req, &row)
	return row, err
}

func (c *RelayClient) GetSubscription(ctx context.Context, id int64) (types.Subscription, error) {
	var row types.Subscription
	err := c.requestJSON(ctx, http.MethodGet, fmt.Sprintf("/subscriptions/%d", id), nil, &row)
	return row, err
}

func (c *RelayClient) ListSubscriptions(ctx context.Context, options types.ListOptions) ([]types.Subscription, error) {
	var rows []types.Subscription
	err := c.requestJSON(ctx, http.MethodGet, "/subscriptions"+listQuery(options), nil, &rows)
	return rows, err
}

func (c *RelayClient) UpdateSubscription(ctx context.Context, id int64, req types.SubscriptionRequest) (types.Subscription, error) {
	var row types.Subscription
	err := c.requestJSON(ctx, http.MethodPut, fmt.Sprintf("/subscriptions/%d", id), req, &row)
	return row, err
}

func (c *RelayClient) Unsubscribe(ctx context.Context, id int64) error {
	return c.requestJSON(ctx, http.MethodDelete, fmt.Sprintf("/subscriptions/%d", id), nil, nil)
}
