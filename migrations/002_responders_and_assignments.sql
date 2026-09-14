-- Responders table
CREATE TABLE IF NOT EXISTS responders (
    responder_id VARCHAR(32) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    is_eligible BOOLEAN NOT NULL DEFAULT TRUE,
    is_online BOOLEAN NOT NULL DEFAULT FALSE,
    active_capacity INT NOT NULL DEFAULT 3,
    current_active_incidents INT NOT NULL DEFAULT 0,
    location GEOGRAPHY(Point, 4326) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Add geographic location to incidents
ALTER TABLE incidents
ADD COLUMN IF NOT EXISTS location GEOGRAPHY(Point, 4326);

-- Assignments history
CREATE TABLE IF NOT EXISTS assignments (
    assignment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id VARCHAR(32) NOT NULL REFERENCES incidents(incident_id),
    responder_id VARCHAR(32) NOT NULL REFERENCES responders(responder_id),
    status VARCHAR(32) NOT NULL DEFAULT 'ASSIGNED',
    assigned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Spatial index
CREATE INDEX IF NOT EXISTS idx_responders_location
ON responders USING GIST(location);