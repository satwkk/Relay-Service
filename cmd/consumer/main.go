package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"relay/internal/consumer"
	"relay/internal/ingestion"
	"relay/internal/subscription"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	kfClient, err := kgo.NewClient(
		kgo.SeedBrokers(os.Getenv("KAFKA_ADDR")),
		kgo.ConsumeTopics("relay.events"),
		kgo.ConsumerGroup("relay-fanout"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Printf("Kafka client creation failed")
		return
	}
	defer kfClient.Close()
	db, err := gorm.Open(postgres.Open(os.Getenv("DATABASE_URL")), &gorm.Config{})
	if err != nil {
		log.Printf("Opening db connection failed")
		return
	}
	subscriptionSvc, err := subscription.NewService(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Printf("Subscription service initialization failed")
		return
	}
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		fetches := kfClient.PollRecords(ctx, 100)
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var outboxRecord ingestion.OutboxRecordModel
			if err := json.Unmarshal(record.Value, &outboxRecord); err != nil {
				log.Fatalf("Decoding record %s failed", record.Value)
				continue
			}
			targets, err := subscriptionSvc.FindTargetsByEventType(ctx, outboxRecord.Event.Type)
			if err != nil {
				log.Fatalf("Cannot get subscription for record %s", outboxRecord.Event.Type)
				continue
			}
			for _, target := range targets {
				deliveryErr := db.WithContext(ctx).
					Model(&consumer.Delivery{}).
					Create(&consumer.Delivery{
						RecordID:       outboxRecord.ID,
						SubscriptionID: target.SubscriptionID,
						EventType:      outboxRecord.Event.Type,
						WebhookURL:     target.HookURL,
						Payload:        outboxRecord.Payload,
						Status:         consumer.DeliveryStatusPending,
					}).Error
				if deliveryErr != nil {
					log.Fatalf("Error while creating delivery record for event: %s", outboxRecord.Event.Type)
					continue
				}
			}
			var fetchedDeliveries []consumer.Delivery
			fetchDeliveryErr := db.WithContext(ctx).Model(&consumer.Delivery{}).
				Where("status = ? AND next_attempt_at < ?", consumer.DeliveryStatusPending, time.Now()).
				Scan(&fetchedDeliveries).Error
			if fetchDeliveryErr != nil {
				log.Printf("Error while fetching deliveries to triggering the webhook")
				continue
			}
			for _, delivery := range fetchedDeliveries {
				webhookErr := fireWebhook(ctx, httpClient, delivery)
				if webhookErr != nil {
					log.Printf("Webhook failed for delivery %d: %v", delivery.ID, webhookErr)
				}
				updateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				updateErr := consumer.RecordResult(updateCtx, db, delivery, webhookErr)
				cancel()
				if updateErr != nil {
					log.Printf("Failed to record webhook result for delivery %d: %v", delivery.ID, updateErr)
				}
			}
			log.Printf("topic=%s partition=%d offset=%d value=%s", record.Topic, record.Partition, record.Offset, record.Value)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// todo
		}
	}
}

func fireWebhook(ctx context.Context, client *http.Client, delivery consumer.Delivery) error {
	body, err := json.Marshal(delivery.Payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}
