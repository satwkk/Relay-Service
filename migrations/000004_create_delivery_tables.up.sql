BEGIN;

CREATE TABLE IF NOT EXISTS deliveries (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    record_id bigint NOT NULL REFERENCES outbox_record_models(id),
    subscription_id bigint NOT NULL REFERENCES subscriptions(id),
    event_type text NOT NULL,
    webhook_url text NOT NULL,
    payload text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempt_count int NOT NULL DEFAULT 0,
    last_error text,
    last_attempt_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_event_subscription UNIQUE (record_id, subscription_id)
);

COMMIT;