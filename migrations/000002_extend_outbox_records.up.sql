BEGIN;

ALTER TABLE outbox_record_models
    ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    ADD COLUMN last_attempt_at timestamptz,
    ADD COLUMN last_error text,
    ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE outbox_record_models
    DROP CONSTRAINT outbox_record_models_state_check,
    ADD CONSTRAINT outbox_record_models_state_check CHECK (state IN (0, 1, 2, 3));

DROP INDEX outbox_record_models_pending_idx;

CREATE INDEX outbox_record_models_pending_idx
    ON outbox_record_models (next_attempt_at, id)
    WHERE state = 1;

COMMIT;
