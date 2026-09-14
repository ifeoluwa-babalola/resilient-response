-- Track assignment attempts on the incident
ALTER TABLE incidents 
ADD COLUMN IF NOT EXISTS assignment_count INT NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS max_assignments INT NOT NULL DEFAULT 2,
ADD COLUMN IF NOT EXISTS ack_deadline TIMESTAMP WITH TIME ZONE;

-- Add escalation status values to assignments
ALTER TABLE assignments 
ADD COLUMN IF NOT EXISTS ack_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS timed_out_at TIMESTAMP WITH TIME ZONE;