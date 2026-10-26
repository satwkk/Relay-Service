package consumer

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

const MaxDeliveryAttempts = 5

func RecordResult(ctx context.Context, db *gorm.DB, delivery Delivery, webhookErr error) error {
	if delivery.ClaimToken == nil {
		return ErrClaimLost
	}
	updates := map[string]any{
		"updated_at":       gorm.Expr("CURRENT_TIMESTAMP"),
		"claim_token":      nil,
		"lease_expires_at": nil,
	}
	if webhookErr == nil {
		updates["status"] = DeliveryStatusDelivered
		updates["delivered_at"] = gorm.Expr("CURRENT_TIMESTAMP")
		updates["last_error"] = nil
	} else {
		attempt := delivery.AttemptCount
		updates["last_error"] = webhookErr.Error()
		updates["delivered_at"] = nil
		if attempt >= MaxDeliveryAttempts {
			updates["status"] = DeliveryStatusFailed
		} else {
			updates["status"] = DeliveryStatusPending
			updates["next_attempt_at"] = gorm.Expr(
				"CURRENT_TIMESTAMP + (? * INTERVAL '1 second')", retryDelay(attempt).Seconds(),
			)
		}
	}
	result := db.WithContext(ctx).Model(&Delivery{}).
		Where("id = ? AND status = ? AND claim_token = ? AND lease_expires_at > CURRENT_TIMESTAMP",
			delivery.ID, DeliveryStatusProcessing, *delivery.ClaimToken).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("record webhook result for delivery %d: %w", delivery.ID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: delivery %d", ErrClaimLost, delivery.ID)
	}
	return nil
}

func retryDelay(attempt int) time.Duration {
	delay := 5 * time.Second
	const maximum = 5 * time.Minute
	for i := 1; i < attempt && delay < maximum; i++ {
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
