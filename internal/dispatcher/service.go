package dispatcher

import (
	"context"
	"fmt"
	"strings"

	"relay/internal/ingestion"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Service struct {
	db *gorm.DB
}

func NewService(dsn string) (*Service, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect dispatcher to PostgreSQL: %w", err)
	}
	return &Service{db: db}, nil
}

func (s *Service) FetchPending(ctx context.Context, limit int) ([]ingestion.OutboxRecordModel, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("outbox batch size must be between 1 and 1000")
	}
	var records []ingestion.OutboxRecordModel
	err := s.db.WithContext(ctx).
		Where("state = ? AND next_attempt_at <= CURRENT_TIMESTAMP", ingestion.StatusPending).
		Order("next_attempt_at ASC").
		Order("id ASC").
		Limit(limit).
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("fetch pending outbox records: %w", err)
	}
	return records, nil
}

func (s *Service) RecordFailure(ctx context.Context, record ingestion.OutboxRecordModel, err error) error {
	result := s.db.
		WithContext(ctx).
		Model(&ingestion.OutboxRecordModel{}).
		Where("ID = ? AND state = ?", record.ID, ingestion.StatusPending).
		Updates(map[string]any{
			"attempt_count":   gorm.Expr("attempt_count + 1"),
			"last_attempt_at": gorm.Expr("CURRENT_TIMESTAMP"),
			"last_error":      err.Error(),
			"next_attempt_at": gorm.Expr("CURRENT_TIMESTAMP + INTERVAL '5 seconds'"),
		})
	if result.Error != nil {
		return fmt.Errorf("record failure for outbox %d: %w", record.ID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("outbox %d was not found or is no longer pending", record.ID)
	}
	return nil
}

func (s *Service) RecordPermanentFailure(ctx context.Context, record ingestion.OutboxRecordModel) error {
	result := s.db.
		WithContext(ctx).
		Model(&ingestion.OutboxRecordModel{}).
		Where("ID = ? AND state = ?", record.ID, ingestion.StatusPending).
		Updates(map[string]any{
			"state":           ingestion.StatusFailed,
			"attempt_count":   gorm.Expr("attempt_count + 1"),
			"last_attempt_at": gorm.Expr("CURRENT_TIMESTAMP"),
			"last_error":      "",
			"next_attempt_at": gorm.Expr("CURRENT_TIMESTAMP + INTERVAL '5 seconds'"),
		})
	if result.Error != nil {
		return fmt.Errorf("record failure for outbox %d: %w", record.ID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("outbox %d was not found or is no longer pending", record.ID)
	}
	return nil
}

func (s *Service) RecordSuccess(ctx context.Context, record ingestion.OutboxRecordModel) error {
	result := s.db.
		WithContext(ctx).
		Model(&ingestion.OutboxRecordModel{}).
		Where("ID = ? AND state = ?", record.ID, ingestion.StatusPending).
		Updates(map[string]any{
			"state":           ingestion.StatusPublished,
			"attempt_count":   gorm.Expr("attempt_count + 1"),
			"last_attempt_at": gorm.Expr("CURRENT_TIMESTAMP"),
			"last_error":      nil,
		})
	if result.Error != nil {
		return fmt.Errorf("record failure for outbox %d: %w", record.ID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("outbox %d was not found or is no longer pending", record.ID)
	}
	return nil
}

func (s *Service) Close() error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}
