package types

type Event struct {
	Type           string
	IdempotenceKey string
	Payload        string
}
