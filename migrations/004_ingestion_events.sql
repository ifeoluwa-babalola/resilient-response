CREATE TABLE IF NOT EXISTS ingestion_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_ingestion_events_pending 
ON ingestion_events(status) WHERE status = 'PENDING';