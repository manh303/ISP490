-- ============================================
-- Migration: Add AI Generation Status Tracking
-- ============================================
-- Purpose: Track async AI generation status for DSS analysis sessions

-- Add new columns to dss_analysis_session for async AI tracking
ALTER TABLE dss.dss_analysis_session 
ADD COLUMN IF NOT EXISTS ai_generation_status VARCHAR(30) DEFAULT 'pending',
ADD COLUMN IF NOT EXISTS ai_generation_started_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS ai_generation_completed_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS ai_generation_error TEXT,
ADD COLUMN IF NOT EXISTS ai_model_used VARCHAR(100);

-- Add constraint for ai_generation_status
ALTER TABLE dss.dss_analysis_session 
DROP CONSTRAINT IF EXISTS chk_ai_generation_status;

ALTER TABLE dss.dss_analysis_session 
ADD CONSTRAINT chk_ai_generation_status 
CHECK (ai_generation_status IN ('pending', 'generating', 'completed', 'failed', 'skipped'));

-- Add index for querying by AI status
CREATE INDEX IF NOT EXISTS idx_session_ai_status 
ON dss.dss_analysis_session(ai_generation_status) 
WHERE ai_generation_status IN ('pending', 'generating');

-- Comments
COMMENT ON COLUMN dss.dss_analysis_session.ai_generation_status IS 'Status of async AI generation: pending, generating, completed, failed, skipped';
COMMENT ON COLUMN dss.dss_analysis_session.ai_generation_started_at IS 'Timestamp when AI generation started';
COMMENT ON COLUMN dss.dss_analysis_session.ai_generation_completed_at IS 'Timestamp when AI generation completed';
COMMENT ON COLUMN dss.dss_analysis_session.ai_generation_error IS 'Error message if AI generation failed';
COMMENT ON COLUMN dss.dss_analysis_session.ai_model_used IS 'AI model/provider used: OpenAI, Gemini, or rule-based-fallback';
