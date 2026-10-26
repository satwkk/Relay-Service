BEGIN;

-- Do not remove ownership metadata from work that is still being processed.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM deliveries WHERE status = 'processing') THEN
        RAISE EXCEPTION 'stop consumers and resolve processing deliveries before rollback';
    END IF;
END $$;

DROP TABLE consumer_rejections;
DROP TABLE consumer_fanouts;
DROP INDEX deliveries_processing_idx;
DROP INDEX deliveries_pending_idx;
ALTER TABLE deliveries
    DROP CONSTRAINT deliveries_claim_check,
    DROP CONSTRAINT deliveries_attempt_count_check,
    DROP CONSTRAINT deliveries_status_check,
    DROP COLUMN lease_expires_at,
    DROP COLUMN claim_token;

COMMIT;
