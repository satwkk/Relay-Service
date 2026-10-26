package subscription

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"relay/types"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrInvalid  = errors.New("invalid request")
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("resource already exists")
)

type Service struct{ db *gorm.DB }

func NewService(dsn string) (*Service, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect subscription registry to PostgreSQL: %w", err)
	}
	return &Service{db: db}, nil
}

func (s *Service) Close() error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func subscriberFromRequest(req types.SubscriberRequest) (types.Subscriber, error) {
	name, endpoint := strings.TrimSpace(req.Name), strings.TrimSpace(req.WebhookURL)
	u, err := url.Parse(endpoint)
	if name == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return types.Subscriber{}, fmt.Errorf("%w: name and an absolute HTTP(S) webhook_url without credentials or a fragment are required", ErrInvalid)
	}
	enabled := req.Enabled == nil || *req.Enabled
	return types.Subscriber{Name: name, WebhookURL: endpoint, Enabled: enabled}, nil
}

func subscriptionFromRequest(req types.SubscriptionRequest) (types.Subscription, error) {
	eventType := strings.TrimSpace(req.EventType)
	if req.SubscriberID <= 0 || eventType == "" {
		return types.Subscription{}, fmt.Errorf("%w: positive subscriber_id and nonempty event_type are required", ErrInvalid)
	}
	return types.Subscription{SubscriberID: req.SubscriberID, EventType: eventType, Enabled: req.Enabled == nil || *req.Enabled}, nil
}

func databaseError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return ErrConflict
		case "23503":
			return fmt.Errorf("%w: subscriber does not exist", ErrNotFound)
		}
	}
	return err
}

func page(db *gorm.DB, options types.ListOptions) (*gorm.DB, error) {
	if options.Limit == 0 {
		options.Limit = 100
	}
	if options.Limit < 1 || options.Limit > 1000 || options.Offset < 0 {
		return nil, fmt.Errorf("%w: limit must be between 1 and 1000 and offset must be nonnegative", ErrInvalid)
	}
	return db.Order("id ASC").Limit(options.Limit).Offset(options.Offset), nil
}

func (s *Service) CreateSubscriber(ctx context.Context, req types.SubscriberRequest) (types.Subscriber, error) {
	row, err := subscriberFromRequest(req)
	if err != nil {
		return row, err
	}
	err = s.db.WithContext(ctx).Table("subscribers").Create(&row).Error
	return row, databaseError(err)
}

func (s *Service) GetSubscriber(ctx context.Context, id int64) (types.Subscriber, error) {
	var row types.Subscriber
	err := s.db.WithContext(ctx).Table("subscribers").First(&row, id).Error
	return row, databaseError(err)
}

func (s *Service) ListSubscribers(ctx context.Context, options types.ListOptions) ([]types.Subscriber, error) {
	rows := make([]types.Subscriber, 0)
	query, err := page(s.db.WithContext(ctx).Table("subscribers"), options)
	if err != nil {
		return nil, err
	}
	err = query.Find(&rows).Error
	return rows, databaseError(err)
}

func (s *Service) UpdateSubscriber(ctx context.Context, id int64, req types.SubscriberRequest) (types.Subscriber, error) {
	row, err := subscriberFromRequest(req)
	if err != nil {
		return row, err
	}
	result := s.db.WithContext(ctx).Table("subscribers").Model(&row).Where("id = ?", id).
		Clauses(clause.Returning{}).Updates(map[string]any{"name": row.Name, "webhook_url": row.WebhookURL, "enabled": row.Enabled})
	if result.Error != nil {
		return row, databaseError(result.Error)
	}
	if result.RowsAffected == 0 {
		return row, ErrNotFound
	}
	return row, nil
}

func (s *Service) DeleteSubscriber(ctx context.Context, id int64) error {
	result := s.db.WithContext(ctx).Table("subscribers").Delete(&types.Subscriber{}, id)
	if result.Error != nil {
		return databaseError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Service) CreateSubscription(ctx context.Context, req types.SubscriptionRequest) (types.Subscription, error) {
	row, err := subscriptionFromRequest(req)
	if err != nil {
		return row, err
	}
	err = s.db.WithContext(ctx).Table("subscriptions").Create(&row).Error
	return row, databaseError(err)
}

func (s *Service) GetSubscription(ctx context.Context, id int64) (types.Subscription, error) {
	var row types.Subscription
	err := s.db.WithContext(ctx).Table("subscriptions").First(&row, id).Error
	return row, databaseError(err)
}

func (s *Service) ListSubscriptions(ctx context.Context, options types.ListOptions) ([]types.Subscription, error) {
	rows := make([]types.Subscription, 0)
	query, err := page(s.db.WithContext(ctx).Table("subscriptions"), options)
	if err != nil {
		return nil, err
	}
	err = query.Find(&rows).Error
	return rows, databaseError(err)
}

func (s *Service) UpdateSubscription(ctx context.Context, id int64, req types.SubscriptionRequest) (types.Subscription, error) {
	row, err := subscriptionFromRequest(req)
	if err != nil {
		return row, err
	}
	result := s.db.WithContext(ctx).Table("subscriptions").Model(&row).Where("id = ?", id).
		Clauses(clause.Returning{}).Updates(map[string]any{"subscriber_id": row.SubscriberID, "event_type": row.EventType, "enabled": row.Enabled})
	if result.Error != nil {
		return row, databaseError(result.Error)
	}
	if result.RowsAffected == 0 {
		return row, ErrNotFound
	}
	return row, nil
}

func (s *Service) DeleteSubscription(ctx context.Context, id int64) error {
	result := s.db.WithContext(ctx).Table("subscriptions").Delete(&types.Subscription{}, id)
	if result.Error != nil {
		return databaseError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Service) FindTargetsByEventType(ctx context.Context, eventType string) ([]types.DeliveryTarget, error) {
	targets := make([]types.DeliveryTarget, 0)
	err := s.db.WithContext(ctx).
		Table("subscriptions AS s").
		Select("s.id AS subscription_id, sub.webhook_url").
		Joins("JOIN subscribers AS sub on sub.id = s.subscriber_id").
		Where(
			"s.event_type = ? AND s.enabled = ? AND sub.enabled = ?",
			eventType, true, true,
		).Scan(&targets).Error
	return targets, err
}
