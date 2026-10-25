package ingestion

import "relay/internal/types"

type EventModel struct {
	types.Event
}

type Status int

const (
	StatusUnknown Status = iota
	StatusPending
	StatusPublished
)

type OutboxRecordModel struct {
	EventModel
	State Status
}
