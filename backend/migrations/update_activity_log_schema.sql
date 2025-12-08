-- Migration: Update Activity Log Schema
-- Description: Add missing columns to iam.user_activity_logs table to match todo.txt requirements
-- Version: 1.0
-- Date: 2025-12-01

-- Add missing columns to user_activity_logs table
ALTER TABLE iam.user_activity_logs
  ADD COLUMN IF NOT EXISTS module VARCHAR(50),
  ADD COLUMN IF NOT EXISTS role_at_time VARCHAR(255),
  ADD COLUMN IF NOT EXISTS resource_type VARCHAR(100),
  ADD COLUMN IF NOT EXISTS request_method VARCHAR(10),
  ADD COLUMN IF NOT EXISTS request_payload JSONB,
  ADD COLUMN IF NOT EXISTS before_data JSONB,
  ADD COLUMN IF NOT EXISTS after_data JSONB,
  ADD COLUMN IF NOT EXISTS message TEXT;

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_activity_logs_module ON iam.user_activity_logs(module);
CREATE INDEX IF NOT EXISTS idx_activity_logs_action ON iam.user_activity_logs(action);
CREATE INDEX IF NOT EXISTS idx_activity_logs_status ON iam.user_activity_logs(status);
CREATE INDEX IF NOT EXISTS idx_activity_logs_user_id ON iam.user_activity_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON iam.user_activity_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_logs_resource_type ON iam.user_activity_logs(resource_type);

-- Add comment to document the table purpose
COMMENT ON TABLE iam.user_activity_logs IS 'Activity audit log tracking all user actions in the system';
COMMENT ON COLUMN iam.user_activity_logs.module IS 'System module: IAM, Analytics, DSS, ML, DataPipeline';
COMMENT ON COLUMN iam.user_activity_logs.role_at_time IS 'User role at the time of action';
COMMENT ON COLUMN iam.user_activity_logs.resource_type IS 'Type of resource: iam_user, ml_model, report, dataset, etc.';
COMMENT ON COLUMN iam.user_activity_logs.request_method IS 'HTTP method: GET, POST, PUT, DELETE';
COMMENT ON COLUMN iam.user_activity_logs.request_payload IS 'Request body (sensitive fields masked)';
COMMENT ON COLUMN iam.user_activity_logs.before_data IS 'Data state before the action';
COMMENT ON COLUMN iam.user_activity_logs.after_data IS 'Data state after the action';
COMMENT ON COLUMN iam.user_activity_logs.message IS 'Detailed message, error description, or notes';
