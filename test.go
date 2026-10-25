package main

import (
	"context"
	"log"
	"relay/internal/client"
	"relay/internal/types"
)

func main() {
	c, err := client.NewRelayClient("127.0.0.1", "8080")
	if err != nil {
		log.Fatal("Error during creating client")
		return
	}
	payload := "{\"ride_id\": \"ride_123\"}"
	resp, err := c.DispatchEvent(context.Background(), types.Event{
		Type:           "ride.completed.v1",
		IdempotenceKey: "idempotency-key",
		Payload:        payload,
	})
	if err != nil {
		log.Fatal("Event dispatch failed")
		return
	}
	log.Printf("Body: %s", resp.Body.Content)
}
