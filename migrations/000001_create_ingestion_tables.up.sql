BEGIN;

-- Names and embedded fields match GORM's current EventModel mapping.
CREATE TABLE event_models (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type text NOT NULL,
    idempotence_key text NOT NULL,
    payload text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- The current OutboxRecordModel embeds EventModel, so it stores a snapshot
-- of the same event fields. State values match ingestion.Status.
CREATE TABLE outbox_record_models (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type text NOT NULL,
    idempotence_key text NOT NULL,
    payload text NOT NULL,
    state smallint NOT NULL DEFAULT 1 CHECK (state IN (0, 1, 2)),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_record_models_pending_idx
    ON outbox_record_models (created_at, id)
    WHERE state = 1;

COMMIT;
