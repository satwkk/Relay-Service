BEGIN;

CREATE TABLE subscribers (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (btrim(name) <> ''),
    webhook_url text NOT NULL CHECK (btrim(webhook_url) <> ''),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE subscriptions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id bigint NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    event_type text NOT NULL CHECK (btrim(event_type) <> ''),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (subscriber_id, event_type)
);

CREATE INDEX subscriptions_active_event_type_idx
    ON subscriptions (event_type, subscriber_id) WHERE enabled;

COMMIT;
