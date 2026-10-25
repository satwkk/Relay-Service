package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"relay/internal/dispatcher"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatalf("Relay worker failed: %v", err)
	}
}

func run(ctx context.Context) error {
	pollInterval := 500 * time.Millisecond
	if value := os.Getenv("OUTBOX_POLL_INTERVAL"); value != "" {
		var err error
		pollInterval, err = time.ParseDuration(value)
		if err != nil || pollInterval <= 0 {
			return fmt.Errorf("OUTBOX_POLL_INTERVAL must be a positive duration, for example 500ms")
		}
	}
	kfCli, err := kgo.NewClient(
		kgo.SeedBrokers(os.Getenv("KAFKA_ADDR")),
	)
	if err != nil {
		return err
	}
	defer kfCli.Close()
	service, err := dispatcher.NewService(os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer func() {
		if err := service.Close(); err != nil {
			log.Printf("Closing dispatcher database: %v", err)
		}
	}()

	log.Printf("Relay worker started; polling every %s", pollInterval)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		records, err := service.FetchPending(pollCtx, 100)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("Outbox poll failed: %v", err)
			}
		} else if len(records) > 0 {
			ids := make([]int64, len(records))
			for i, record := range records {
				ids[i] = record.ID
				eventJson, err := json.Marshal(record)
				if err != nil {
					log.Printf("Json marshaling failed for event %d", record.ID)
					continue
				}
				kfRecord := &kgo.Record{
					Topic: "relay.events",
					Value: eventJson,
				}
				produceErr := kfCli.ProduceSync(ctx, kfRecord).FirstErr()
				if produceErr != nil {
					if err := service.RecordFailure(ctx, record, produceErr); err != nil {
						log.Printf("Failed to record Kafka failure for outbox %d: %v", record.ID, err)
					}
					continue
				}
				if err := service.RecordSuccess(ctx, record); err != nil {
					log.Printf("Successfully recorded Kafka success for outbox %d: %v", record.ID, err)
				}
			}
			log.Printf("Fetched %d pending outbox records: ids=%v", len(records), ids)
		}

		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}

	log.Print("Relay worker stopped")
	return nil
}
