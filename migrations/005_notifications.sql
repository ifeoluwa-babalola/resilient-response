CREATE TABLE IF NOT EXISTS notifications (
    notification_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id VARCHAR(64) NOT NULL REFERENCES incidents(incident_id),
    responder_id VARCHAR(64) NOT NULL REFERENCES responders(responder_id),
    recipient_phone VARCHAR(32) NOT NULL,
    message_body TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, SENT, FAILED
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_notifications_pending 
ON notifications(status, retry_count) WHERE status IN ('PENDING', 'FAILED');