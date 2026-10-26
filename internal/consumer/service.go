package consumer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	"relay/internal/ingestion"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var ErrClaimLost = errors.New("delivery claim expired or was replaced")

type Service struct{ db *gorm.DB }

func NewService(dsn string) (*Service, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect consumer to PostgreSQL: %w", err)
	}
	pool, err := db.DB()
	if err != nil {
		return nil, err
	}
	pool.SetMaxOpenConns(10)
	pool.SetMaxIdleConns(2)
	pool.SetConnMaxLifetime(30 * time.Minute)
	return &Service{db: db}, nil
}

func (s *Service) Close() error {
	pool, err := s.db.DB()
	if err != nil {
		return err
	}
	return pool.Close()
}

func (s *Service) Fanout(ctx context.Context, record ingestion.OutboxRecordModel) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		receipt := tx.Exec("INSERT INTO consumer_fanouts(record_id) VALUES (?) ON CONFLICT DO NOTHING", record.ID)
		if receipt.Error != nil {
			return receipt.Error
		}
		if receipt.RowsAffected == 0 {
			return nil
		}
		return tx.Exec(`INSERT INTO deliveries (record_id, subscription_id, event_type, webhook_url, payload)
			SELECT ?, s.id, ?, sub.webhook_url, ?
			FROM subscriptions s JOIN subscribers sub ON sub.id = s.subscriber_id
			WHERE s.event_type = ? AND s.enabled AND sub.enabled
			ON CONFLICT (record_id, subscription_id) DO NOTHING`,
			record.ID, record.Type, record.Payload, record.Type).Error
	})
}

func (s *Service) Claim(ctx context.Context, lease time.Duration) (*Delivery, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	token[6] = (token[6] & 0x0f) | 0x40
	token[8] = (token[8] & 0x3f) | 0x80
	claim := fmt.Sprintf("%x-%x-%x-%x-%x", token[:4], token[4:6], token[6:8], token[8:10], token[10:])
	var rows []Delivery
	err := s.db.WithContext(ctx).Raw(`WITH candidate AS (
		SELECT id FROM deliveries
		WHERE (status = 'pending' AND next_attempt_at <= CURRENT_TIMESTAMP)
		   OR (status = 'processing' AND lease_expires_at <= CURRENT_TIMESTAMP)
		ORDER BY next_attempt_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
	)
	UPDATE deliveries d SET
		status = CASE WHEN d.attempt_count >= ? THEN 'failed' ELSE 'processing' END,
		claim_token = CASE WHEN d.attempt_count >= ? THEN NULL ELSE CAST(? AS uuid) END,
		lease_expires_at = CASE WHEN d.attempt_count >= ? THEN NULL ELSE CURRENT_TIMESTAMP + (? * INTERVAL '1 second') END,
		last_attempt_at = CASE WHEN d.attempt_count >= ? THEN d.last_attempt_at ELSE CURRENT_TIMESTAMP END,
		last_error = CASE WHEN d.attempt_count >= ? THEN
			COALESCE(d.last_error || '; ', '') || 'attempt limit reached; previous worker outcome may be unknown'
			ELSE d.last_error END,
		attempt_count = CASE WHEN d.attempt_count >= ? THEN d.attempt_count ELSE d.attempt_count + 1 END,
		updated_at = CURRENT_TIMESTAMP
	FROM candidate c WHERE d.id = c.id RETURNING d.*`,
		MaxDeliveryAttempts, MaxDeliveryAttempts, claim, MaxDeliveryAttempts, lease.Seconds(),
		MaxDeliveryAttempts, MaxDeliveryAttempts, MaxDeliveryAttempts).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("claim delivery: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}
