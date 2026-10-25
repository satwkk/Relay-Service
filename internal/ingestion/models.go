package ingestion

import (
	"time"

	"relay/internal/types"
)

type EventModel struct {
	types.Event
}

type Status int

const (
	StatusUnknown Status = iota
	StatusPending
	StatusPublished
	StatusFailed
)

type OutboxRecordModel struct {
	EventModel
	ID            int64     `gorm:"primaryKey;autoIncrement"`
	CreatedAt     time.Time `gorm:"default:CURRENT_TIMESTAMP"`
	State         Status
	AttemptCount  int
	LastAttemptAt *time.Time
	LastError     *string
	NextAttemptAt time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}
