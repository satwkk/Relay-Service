package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

func (s *Service) RunDeliveries(ctx context.Context, cfg Config) {
	client := &http.Client{
		Timeout:       cfg.RequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	for ctx.Err() == nil {
		dbCtx, cancel := context.WithTimeout(ctx, cfg.DBTimeout)
		delivery, err := s.Claim(dbCtx, cfg.LeaseDuration)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("Claiming delivery failed: %v", err)
			}
			if !wait(ctx, cfg.PollInterval) {
				return
			}
			continue
		}
		if delivery == nil {
			if !wait(ctx, cfg.PollInterval) {
				return
			}
			continue
		}
		if delivery.Status == DeliveryStatusFailed {
			log.Printf("Delivery %d exhausted its attempt limit after lease recovery", delivery.ID)
			continue
		}
		webhookErr := fireWebhook(ctx, client, *delivery)
		if webhookErr != nil {
			log.Printf("Webhook failed for delivery %d: %v", delivery.ID, webhookErr)
		}
		dbCtx, cancel = context.WithTimeout(ctx, cfg.DBTimeout)
		err = RecordResult(dbCtx, s.db, *delivery, webhookErr)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Printf("Recording result for delivery %d failed: %v", delivery.ID, err)
		}
	}
}

func fireWebhook(ctx context.Context, client *http.Client, delivery Delivery) error {
	// Stored JSON is sent directly. A plain-text payload becomes a JSON string.
	body := []byte(delivery.Payload)
	if !json.Valid(body) {
		var err error
		body, err = json.Marshal(delivery.Payload)
		if err != nil {
			return fmt.Errorf("marshal webhook payload: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Relay-Delivery-ID", strconv.FormatInt(delivery.ID, 10))
	req.Header.Set("X-Relay-Record-ID", strconv.FormatInt(delivery.RecordID, 10))
	req.Header.Set("X-Relay-Event-Type", delivery.EventType)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
