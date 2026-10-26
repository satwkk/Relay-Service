package ingestion

import (
	"context"
	"fmt"

	"relay/types"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Service struct {
	db *gorm.DB
}

func NewService(dsn string) (*Service, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}

	return &Service{
		db: db,
	}, nil
}

func (s *Service) PublishEvent(ctx context.Context, evt types.Event) bool {
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Create(&EventModel{
			Event: evt,
		}).Error
		if err != nil {
			return err
		}
		err = tx.Create(&OutboxRecordModel{
			EventModel: EventModel{
				Event: evt,
			},
			State: StatusPending,
		}).Error
		if err != nil {
			return err
		}
		return nil
	})
	return err == nil
}
