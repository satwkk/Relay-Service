package types

import "time"

type Subscriber struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	WebhookURL string    `json:"webhook_url"`
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Subscription struct {
	ID           int64     `json:"id"`
	SubscriberID int64     `json:"subscriber_id"`
	EventType    string    `json:"event_type"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// PUT replaces the editable fields. Enabled defaults to true when omitted.
type SubscriberRequest struct {
	Name       string `json:"name"`
	WebhookURL string `json:"webhook_url"`
	Enabled    *bool  `json:"enabled,omitempty"`
}

type SubscriptionRequest struct {
	SubscriberID int64  `json:"subscriber_id"`
	EventType    string `json:"event_type"`
	Enabled      *bool  `json:"enabled,omitempty"`
}

// ListOptions uses a default limit of 100; the maximum is 1000.
type ListOptions struct {
	Limit  int
	Offset int
}
