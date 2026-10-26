BEGIN;

ALTER TABLE deliveries
    ADD COLUMN claim_token uuid,
    ADD COLUMN lease_expires_at timestamptz,
    ADD CONSTRAINT deliveries_status_check CHECK (status IN ('pending', 'processing', 'delivered', 'failed')),
    ADD CONSTRAINT deliveries_attempt_count_check CHECK (attempt_count >= 0),
    ADD CONSTRAINT deliveries_claim_check CHECK (
        (status = 'processing' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'processing' AND claim_token IS NULL AND lease_expires_at IS NULL)
    );

CREATE INDEX deliveries_pending_idx ON deliveries (next_attempt_at, id) WHERE status = 'pending';
CREATE INDEX deliveries_processing_idx ON deliveries (lease_expires_at, id) WHERE status = 'processing';

CREATE TABLE consumer_fanouts (
    record_id bigint PRIMARY KEY REFERENCES outbox_record_models(id),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE consumer_rejections (
    topic text NOT NULL,
    partition integer NOT NULL,
    record_offset bigint NOT NULL,
    record_value bytea NOT NULL,
    last_error text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, record_offset)
);

COMMIT;
