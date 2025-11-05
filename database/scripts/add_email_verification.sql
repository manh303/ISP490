-- Add email verification table for signup process
-- Run this script to add email verification functionality

-- Create email verification token table
CREATE TABLE IF NOT EXISTS iam_email_verification_token (
  token_id    BIGSERIAL PRIMARY KEY,
  email       VARCHAR(255) NOT NULL,
  token_hash  TEXT NOT NULL,
  expires_at  TIMESTAMP NOT NULL,
  used_at     TIMESTAMP,
  created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Add index for faster lookups
CREATE INDEX IF NOT EXISTS idx_email_verification_email ON iam_email_verification_token(email);
CREATE INDEX IF NOT EXISTS idx_email_verification_expires ON iam_email_verification_token(expires_at);

-- Clean up expired tokens (optional, can be run as a cron job)
-- DELETE FROM iam_email_verification_token WHERE expires_at < NOW();
