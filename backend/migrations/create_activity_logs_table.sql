-- Create user activity logs table
CREATE TABLE IF NOT EXISTS user_activity_logs (
    log_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES iam_user(user_id) ON DELETE SET NULL,
    email VARCHAR(255),
    action VARCHAR(100) NOT NULL,
    resource VARCHAR(100),
    details JSONB,
    ip_address INET,
    user_agent TEXT,
    status VARCHAR(20) DEFAULT 'success',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_activity_logs_user_id ON user_activity_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON user_activity_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_activity_logs_action ON user_activity_logs(action);
CREATE INDEX IF NOT EXISTS idx_activity_logs_status ON user_activity_logs(status);
CREATE INDEX IF NOT EXISTS idx_activity_logs_email ON user_activity_logs(email);

-- Add comments
COMMENT ON TABLE user_activity_logs IS 'Stores user activity logs for admin monitoring';
COMMENT ON COLUMN user_activity_logs.user_id IS 'Reference to iam_user table, nullable for anonymous activities';
COMMENT ON COLUMN user_activity_logs.email IS 'User email for tracking, stored separately for deleted users';
COMMENT ON COLUMN user_activity_logs.action IS 'HTTP method and endpoint (e.g., POST /api/v1/auth/signin)';
COMMENT ON COLUMN user_activity_logs.resource IS 'Resource path accessed';
COMMENT ON COLUMN user_activity_logs.details IS 'JSON details including status_code, process_time, etc.';
COMMENT ON COLUMN user_activity_logs.status IS 'success or error based on HTTP status code';