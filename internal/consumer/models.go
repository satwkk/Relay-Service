package consumer

import "time"

type DeliveryStatus string

const (
	DeliveryStatusUnknown    DeliveryStatus = ""
	DeliveryStatusPending    DeliveryStatus = "pending"
	DeliveryStatusProcessing DeliveryStatus = "processing"
	DeliveryStatusDelivered  DeliveryStatus = "delivered"
	DeliveryStatusFailed     DeliveryStatus = "failed"
)

type Delivery struct {
	ID             int64 `gorm:"primaryKey;autoIncrement"`
	RecordID       int64
	SubscriptionID int64
	EventType      string
	WebhookURL     string
	Payload        string
	Status         DeliveryStatus
	AttemptCount   int `gorm:"default:0"`
	LastError      *string
	LastAttemptAt  *time.Time
	DeliveredAt    *time.Time
	CreatedAt      time.Time `gorm:"default:CURRENT_TIMESTAMP"`
	UpdatedAt      time.Time `gorm:"default:CURRENT_TIMESTAMP"`
	NextAttemptAt  time.Time `gorm:"default:CURRENT_TIMESTAMP"`
	ClaimToken     *string
	LeaseExpiresAt *time.Time
}
