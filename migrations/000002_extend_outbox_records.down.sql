BEGIN;

-- Resolve any failed (state = 3) records before rolling back this version.
-- The constraint rejects rollback if those records still exist.
ALTER TABLE outbox_record_models
    DROP CONSTRAINT outbox_record_models_state_check,
    ADD CONSTRAINT outbox_record_models_state_check CHECK (state IN (0, 1, 2));

DROP INDEX outbox_record_models_pending_idx;

CREATE INDEX outbox_record_models_pending_idx
    ON outbox_record_models (created_at, id)
    WHERE state = 1;

ALTER TABLE outbox_record_models
    DROP COLUMN next_attempt_at,
    DROP COLUMN last_error,
    DROP COLUMN last_attempt_at,
    DROP COLUMN attempt_count;

COMMIT;
