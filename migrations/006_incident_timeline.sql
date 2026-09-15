CREATE TABLE IF NOT EXISTS incident_timeline (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id VARCHAR(64) NOT NULL REFERENCES incidents(incident_id) ON DELETE CASCADE,
    status VARCHAR(64) NOT NULL,
    actor VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_timeline_incident 
ON incident_timeline(incident_id, created_at ASC);