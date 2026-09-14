-- Enable PostGIS extension for geographic location tracking
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- Incidents table
CREATE TABLE IF NOT EXISTS incidents (
    incident_id VARCHAR(32) PRIMARY KEY,
    organization_id UUID NOT NULL,
    submission_id VARCHAR(64) NOT NULL,
    channel VARCHAR(32) NOT NULL,
    category VARCHAR(64) NOT NULL,
    urgency VARCHAR(16) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    location_text TEXT NOT NULL,
    description TEXT,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Guarantee idempotency: One submission_id per organization
    CONSTRAINT uq_organization_submission UNIQUE (organization_id, submission_id)
);