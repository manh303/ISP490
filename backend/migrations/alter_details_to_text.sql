-- Alter user_activity_logs.details column from JSONB to TEXT
-- This allows storing simple string messages instead of JSON objects

ALTER TABLE iam.user_activity_logs 
ALTER COLUMN details TYPE TEXT USING details::TEXT;

-- Update comment
COMMENT ON COLUMN iam.user_activity_logs.details IS 'Activity details as text string';
