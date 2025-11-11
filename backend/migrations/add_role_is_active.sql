-- Add is_active column to iam_role table
-- Migration: Add role activation/deactivation support

-- Add is_active column with default value true
ALTER TABLE iam_role 
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true;

-- Update existing roles to be active
UPDATE iam_role 
SET is_active = true 
WHERE is_active IS NULL;

-- Add index for better performance on active role queries
CREATE INDEX IF NOT EXISTS idx_iam_role_is_active ON iam_role(is_active);

-- Add comment
COMMENT ON COLUMN iam_role.is_active IS 'Whether the role is active and can be assigned to users';