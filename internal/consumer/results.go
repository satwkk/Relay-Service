package consumer

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

const MaxDeliveryAttempts = 5

// RecordResult counts a completed attempt and persists either delivery success
// or a scheduled retry. A database error is returned to the caller for logging;
// the delivery remains eligible if the update was not committed.
func RecordResult(ctx context.Context, db *gorm.DB, delivery Delivery, webhookErr error) error {
	updates := map[string]any{
		"attempt_count":   gorm.Expr("attempt_count + 1"),
		"last_attempt_at": gorm.Expr("CURRENT_TIMESTAMP"),
		"updated_at":      gorm.Expr("CURRENT_TIMESTAMP"),
	}
	if webhookErr == nil {
		updates["status"] = DeliveryStatusDelivered
		updates["delivered_at"] = gorm.Expr("CURRENT_TIMESTAMP")
		updates["last_error"] = nil
	} else {
		attempt := delivery.AttemptCount + 1
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
		Where("id = ? AND status = ? AND attempt_count = ?", delivery.ID, DeliveryStatusPending, delivery.AttemptCount).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("record webhook result for delivery %d: %w", delivery.ID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("delivery %d was not found, changed, or is no longer pending", delivery.ID)
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
