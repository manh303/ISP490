-- ===========================
-- IAM SEED DATA INITIALIZATION
-- ===========================

-- Insert Roles
INSERT INTO iam_role (role_code, role_name, description) VALUES
('SUPER_ADMIN', 'Super Administrator', 'Full system access and user management'),
('ADMIN', 'Administrator', 'System administration with limited user management'),
('MANAGER', 'Business Manager', 'Business operations and analytics access'),
('ANALYST', 'Data Analyst', 'Data analysis and reporting access'),
('CUSTOMER', 'Customer', 'Basic customer access to DSS features'),
('VIEWER', 'Viewer', 'Read-only access to reports and dashboards')
ON CONFLICT (role_code) DO NOTHING;

-- Insert Permissions
INSERT INTO iam_permission (perm_code, perm_name, module, action, description) VALUES
-- User Management
('user.create', 'Create Users', 'user', 'create', 'Create new user accounts'),
('user.read', 'Read Users', 'user', 'read', 'View user information'),
('user.update', 'Update Users', 'user', 'update', 'Update user profiles'),
('user.delete', 'Delete Users', 'user', 'delete', 'Delete user accounts'),
('user.manage_roles', 'Manage User Roles', 'user', 'manage', 'Assign/remove roles from users'),

-- System Administration
('system.admin', 'System Administration', 'system', 'admin', 'Full system administration access'),
('system.config', 'System Configuration', 'system', 'config', 'Configure system settings'),
('system.logs', 'View System Logs', 'system', 'read', 'Access system logs and audit trails'),

-- Data Management
('data.read', 'Read Data', 'data', 'read', 'View data and analytics'),
('data.write', 'Write Data', 'data', 'write', 'Create and modify data'),
('data.export', 'Export Data', 'data', 'export', 'Export data and reports'),
('data.import', 'Import Data', 'data', 'import', 'Import data into system'),

-- Analytics & Reports
('analytics.view', 'View Analytics', 'analytics', 'read', 'Access analytics dashboards'),
('analytics.create', 'Create Analytics', 'analytics', 'create', 'Create custom analytics'),
('analytics.advanced', 'Advanced Analytics', 'analytics', 'advanced', 'Access to advanced analytics features'),

-- DSS Features
('dss.dashboard', 'DSS Dashboard', 'dss', 'read', 'Access main DSS dashboard'),
('dss.customer_analytics', 'Customer Analytics', 'dss', 'read', 'Access customer analytics'),
('dss.sales_analytics', 'Sales Analytics', 'dss', 'read', 'Access sales analytics'),
('dss.inventory', 'Inventory Management', 'dss', 'read', 'Access inventory management'),
('dss.recommendations', 'AI Recommendations', 'dss', 'read', 'Access AI recommendations'),

-- Report Management
('report.view', 'View Reports', 'report', 'read', 'View existing reports'),
('report.create', 'Create Reports', 'report', 'create', 'Create new reports'),
('report.edit', 'Edit Reports', 'report', 'update', 'Modify existing reports'),
('report.delete', 'Delete Reports', 'report', 'delete', 'Delete reports'),
('report.schedule', 'Schedule Reports', 'report', 'schedule', 'Schedule automated reports'),

-- API Access
('api.read', 'API Read Access', 'api', 'read', 'Read access to API endpoints'),
('api.write', 'API Write Access', 'api', 'write', 'Write access to API endpoints'),
('api.admin', 'API Admin Access', 'api', 'admin', 'Administrative API access')

ON CONFLICT (perm_code) DO NOTHING;

-- Assign Permissions to Roles

-- SUPER_ADMIN: All permissions
INSERT INTO iam_role_permission (role_id, perm_id, granted_at)
SELECT r.role_id, p.perm_id, NOW()
FROM iam_role r, iam_permission p
WHERE r.role_code = 'SUPER_ADMIN'
ON CONFLICT (role_id, perm_id) DO NOTHING;

-- ADMIN: Most permissions except super admin features
INSERT INTO iam_role_permission (role_id, perm_id, granted_at)
SELECT r.role_id, p.perm_id, NOW()
FROM iam_role r, iam_permission p
WHERE r.role_code = 'ADMIN'
AND p.perm_code IN (
    'user.read', 'user.update', 'user.manage_roles',
    'system.config', 'system.logs',
    'data.read', 'data.write', 'data.export', 'data.import',
    'analytics.view', 'analytics.create', 'analytics.advanced',
    'dss.dashboard', 'dss.customer_analytics', 'dss.sales_analytics', 'dss.inventory', 'dss.recommendations',
    'report.view', 'report.create', 'report.edit', 'report.delete', 'report.schedule',
    'api.read', 'api.write'
)
ON CONFLICT (role_id, perm_id) DO NOTHING;

-- MANAGER: Business and analytics permissions
INSERT INTO iam_role_permission (role_id, perm_id, granted_at)
SELECT r.role_id, p.perm_id, NOW()
FROM iam_role r, iam_permission p
WHERE r.role_code = 'MANAGER'
AND p.perm_code IN (
    'user.read',
    'data.read', 'data.write', 'data.export',
    'analytics.view', 'analytics.create', 'analytics.advanced',
    'dss.dashboard', 'dss.customer_analytics', 'dss.sales_analytics', 'dss.inventory', 'dss.recommendations',
    'report.view', 'report.create', 'report.edit', 'report.schedule',
    'api.read', 'api.write'
)
ON CONFLICT (role_id, perm_id) DO NOTHING;

-- ANALYST: Analytics and reporting permissions
INSERT INTO iam_role_permission (role_id, perm_id, granted_at)
SELECT r.role_id, p.perm_id, NOW()
FROM iam_role r, iam_permission p
WHERE r.role_code = 'ANALYST'
AND p.perm_code IN (
    'data.read', 'data.export',
    'analytics.view', 'analytics.create',
    'dss.dashboard', 'dss.customer_analytics', 'dss.sales_analytics', 'dss.inventory',
    'report.view', 'report.create', 'report.edit',
    'api.read'
)
ON CONFLICT (role_id, perm_id) DO NOTHING;

-- CUSTOMER: Basic DSS access
INSERT INTO iam_role_permission (role_id, perm_id, granted_at)
SELECT r.role_id, p.perm_id, NOW()
FROM iam_role r, iam_permission p
WHERE r.role_code = 'CUSTOMER'
AND p.perm_code IN (
    'data.read',
    'analytics.view',
    'dss.dashboard', 'dss.customer_analytics',
    'report.view',
    'api.read'
)
ON CONFLICT (role_id, perm_id) DO NOTHING;

-- VIEWER: Read-only access
INSERT INTO iam_role_permission (role_id, perm_id, granted_at)
SELECT r.role_id, p.perm_id, NOW()
FROM iam_role r, iam_permission p
WHERE r.role_code = 'VIEWER'
AND p.perm_code IN (
    'data.read',
    'analytics.view',
    'dss.dashboard',
    'report.view',
    'api.read'
)
ON CONFLICT (role_id, perm_id) DO NOTHING;

-- Create Default Test Users
-- Note: Passwords are hashed using bcrypt for security

-- Super Admin User (password: admin123)
INSERT INTO iam_user (email, password_hash, full_name, phone, status, created_at, updated_at) VALUES
('admin@dss.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LQv3c1yqBWVHxkd0LH', 'System Administrator', '+84901234567', 'active', NOW(), NOW())
ON CONFLICT (email) DO NOTHING;

-- Business Manager (password: manager123)
INSERT INTO iam_user (email, password_hash, full_name, phone, status, created_at, updated_at) VALUES
('manager@dss.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LQv3c1yqBWVHxkd0LM', 'Business Manager', '+84901234568', 'active', NOW(), NOW())
ON CONFLICT (email) DO NOTHING;

-- Data Analyst (password: analyst123)
INSERT INTO iam_user (email, password_hash, full_name, phone, status, created_at, updated_at) VALUES
('analyst@dss.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LQv3c1yqBWVHxkd0LA', 'Data Analyst', '+84901234569', 'active', NOW(), NOW())
ON CONFLICT (email) DO NOTHING;

-- Customer User (password: customer123)
INSERT INTO iam_user (email, password_hash, full_name, phone, status, created_at, updated_at) VALUES
('customer@dss.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LQv3c1yqBWVHxkd0LC', 'Customer User', '+84901234570', 'active', NOW(), NOW())
ON CONFLICT (email) DO NOTHING;

-- Assign Roles to Users

-- Admin user gets SUPER_ADMIN role
INSERT INTO iam_user_role (user_id, role_id, assigned_at)
SELECT u.user_id, r.role_id, NOW()
FROM iam_user u, iam_role r
WHERE u.email = 'admin@dss.com' AND r.role_code = 'SUPER_ADMIN'
ON CONFLICT (user_id, role_id) DO NOTHING;

-- Manager user gets MANAGER role
INSERT INTO iam_user_role (user_id, role_id, assigned_at)
SELECT u.user_id, r.role_id, NOW()
FROM iam_user u, iam_role r
WHERE u.email = 'manager@dss.com' AND r.role_code = 'MANAGER'
ON CONFLICT (user_id, role_id) DO NOTHING;

-- Analyst user gets ANALYST role
INSERT INTO iam_user_role (user_id, role_id, assigned_at)
SELECT u.user_id, r.role_id, NOW()
FROM iam_user u, iam_role r
WHERE u.email = 'analyst@dss.com' AND r.role_code = 'ANALYST'
ON CONFLICT (user_id, role_id) DO NOTHING;

-- Customer user gets CUSTOMER role
INSERT INTO iam_user_role (user_id, role_id, assigned_at)
SELECT u.user_id, r.role_id, NOW()
FROM iam_user u, iam_role r
WHERE u.email = 'customer@dss.com' AND r.role_code = 'CUSTOMER'
ON CONFLICT (user_id, role_id) DO NOTHING;

-- Create Customer Profiles for business users
INSERT INTO customer_profile (user_id, company_name, address, contact_person)
SELECT u.user_id, 'DSS Demo Company', 'Ho Chi Minh City, Vietnam', u.full_name
FROM iam_user u
WHERE u.email IN ('manager@dss.com', 'customer@dss.com')
ON CONFLICT (user_id) DO NOTHING;

-- Create Analyst Profile
INSERT INTO analyst_profile (user_id, team, specialty, seniority)
SELECT u.user_id, 'Data Analytics Team', 'Business Intelligence', 'Senior'
FROM iam_user u
WHERE u.email = 'analyst@dss.com'
ON CONFLICT (user_id) DO NOTHING;

-- Create Admin Profile
INSERT INTO admin_profile (user_id, note)
SELECT u.user_id, 'System administrator with full access'
FROM iam_user u
WHERE u.email = 'admin@dss.com'
ON CONFLICT (user_id) DO NOTHING;

-- Log initial setup in audit
INSERT INTO iam_audit_log (user_id, action, target_type, details_text, created_at)
SELECT u.user_id, 'SYSTEM_INIT', 'SYSTEM', 'IAM system initialized with default users and permissions', NOW()
FROM iam_user u
WHERE u.email = 'admin@dss.com';

-- Display summary
SELECT 'IAM INITIALIZATION COMPLETE' as status;

SELECT 'ROLES CREATED:' as info, COUNT(*) as count FROM iam_role;
SELECT 'PERMISSIONS CREATED:' as info, COUNT(*) as count FROM iam_permission;
SELECT 'USERS CREATED:' as info, COUNT(*) as count FROM iam_user;
SELECT 'ROLE ASSIGNMENTS:' as info, COUNT(*) as count FROM iam_user_role;

-- Show test credentials
SELECT
    'TEST CREDENTIALS' as info,
    u.email,
    r.role_name,
    'Use password: ' ||
    CASE
        WHEN u.email = 'admin@dss.com' THEN 'admin123'
        WHEN u.email = 'manager@dss.com' THEN 'manager123'
        WHEN u.email = 'analyst@dss.com' THEN 'analyst123'
        WHEN u.email = 'customer@dss.com' THEN 'customer123'
        ELSE 'password123'
    END as credentials
FROM iam_user u
JOIN iam_user_role ur ON u.user_id = ur.user_id
JOIN iam_role r ON ur.role_id = r.role_id
ORDER BY u.email;