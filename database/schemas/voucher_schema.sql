-- ===================================
-- VOUCHER MANAGEMENT SCHEMA
-- ===================================
-- Database schema for comprehensive voucher/promotion system

-- Create vouchers table
CREATE TABLE IF NOT EXISTS vouchers (
    voucher_id VARCHAR(36) PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    voucher_type VARCHAR(20) NOT NULL CHECK (voucher_type IN ('percentage', 'fixed_amount', 'free_shipping', 'buy_x_get_y')),
    discount_value DECIMAL(10,2) NOT NULL,
    max_discount_amount DECIMAL(10,2),
    min_order_amount DECIMAL(10,2) DEFAULT 0,
    usage_limit INTEGER,
    usage_limit_per_customer INTEGER DEFAULT 1,
    target_type VARCHAR(20) NOT NULL DEFAULT 'all_customers' CHECK (target_type IN ('all_customers', 'customer_segment', 'specific_customers', 'new_customers')),
    target_segments JSONB DEFAULT '[]'::jsonb,
    target_customer_ids JSONB DEFAULT '[]'::jsonb,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    applicable_categories JSONB DEFAULT '[]'::jsonb,
    applicable_products JSONB DEFAULT '[]'::jsonb,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'expired', 'used_up')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(36),

    -- Constraints
    CONSTRAINT valid_date_range CHECK (end_date > start_date),
    CONSTRAINT valid_discount_value CHECK (discount_value > 0),
    CONSTRAINT valid_usage_limits CHECK (usage_limit IS NULL OR usage_limit > 0)
);

-- Create voucher usage tracking table
CREATE TABLE IF NOT EXISTS voucher_usage (
    usage_id VARCHAR(36) PRIMARY KEY,
    voucher_id VARCHAR(36) NOT NULL REFERENCES vouchers(voucher_id),
    customer_id VARCHAR(36) NOT NULL,
    order_id VARCHAR(36) NOT NULL,
    order_amount DECIMAL(10,2) NOT NULL,
    discount_applied DECIMAL(10,2) NOT NULL,
    used_at TIMESTAMP DEFAULT NOW(),

    -- Indexes for performance
    INDEX idx_voucher_usage_voucher_id (voucher_id),
    INDEX idx_voucher_usage_customer_id (customer_id),
    INDEX idx_voucher_usage_used_at (used_at)
);

-- Create voucher campaigns table for organized promotions
CREATE TABLE IF NOT EXISTS voucher_campaigns (
    campaign_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    campaign_type VARCHAR(20) DEFAULT 'general' CHECK (campaign_type IN ('general', 'seasonal', 'flash_sale', 'loyalty', 'welcome')),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    budget_limit DECIMAL(12,2),
    budget_used DECIMAL(12,2) DEFAULT 0,
    target_audience JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'paused', 'completed', 'cancelled')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(36)
);

-- Link vouchers to campaigns
CREATE TABLE IF NOT EXISTS campaign_vouchers (
    campaign_id VARCHAR(36) REFERENCES voucher_campaigns(campaign_id),
    voucher_id VARCHAR(36) REFERENCES vouchers(voucher_id),
    added_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (campaign_id, voucher_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_vouchers_code ON vouchers(code);
CREATE INDEX IF NOT EXISTS idx_vouchers_status ON vouchers(status);
CREATE INDEX IF NOT EXISTS idx_vouchers_dates ON vouchers(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_vouchers_type ON vouchers(voucher_type);
CREATE INDEX IF NOT EXISTS idx_vouchers_target_type ON vouchers(target_type);

-- Create indexes for voucher usage
CREATE INDEX IF NOT EXISTS idx_voucher_usage_voucher_customer ON voucher_usage(voucher_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_voucher_usage_order ON voucher_usage(order_id);
CREATE INDEX IF NOT EXISTS idx_voucher_usage_date_range ON voucher_usage(used_at);

-- Create indexes for campaigns
CREATE INDEX IF NOT EXISTS idx_campaigns_status ON voucher_campaigns(status);
CREATE INDEX IF NOT EXISTS idx_campaigns_dates ON voucher_campaigns(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_campaigns_type ON voucher_campaigns(campaign_type);

-- ===================================
-- SAMPLE DATA
-- ===================================

-- Insert sample vouchers
INSERT INTO vouchers (
    voucher_id, code, name, description, voucher_type, discount_value,
    max_discount_amount, min_order_amount, usage_limit, start_date, end_date
) VALUES
(
    'voucher-001', 'WELCOME10', 'Chào mừng khách hàng mới',
    'Giảm 10% cho đơn hàng đầu tiên', 'percentage', 10.00,
    100000, 200000, 1000, NOW(), NOW() + INTERVAL '30 days'
),
(
    'voucher-002', 'FREESHIP50', 'Miễn phí vận chuyển',
    'Miễn phí ship cho đơn từ 500k', 'free_shipping', 30000,
    NULL, 500000, 500, NOW(), NOW() + INTERVAL '60 days'
),
(
    'voucher-003', 'FLASH50K', 'Flash Sale 50K',
    'Giảm ngay 50K cho mọi đơn hàng', 'fixed_amount', 50000,
    NULL, 300000, 200, NOW(), NOW() + INTERVAL '7 days'
),
(
    'voucher-004', 'VIP20', 'Ưu đãi khách VIP',
    'Giảm 20% dành riêng cho khách VIP', 'percentage', 20.00,
    200000, 500000, 100, NOW(), NOW() + INTERVAL '90 days'
);

-- Update VIP voucher to target VIP customers only
UPDATE vouchers
SET target_type = 'customer_segment',
    target_segments = '["VIP", "Premium"]'::jsonb
WHERE code = 'VIP20';

-- Insert sample campaign
INSERT INTO voucher_campaigns (
    campaign_id, name, description, campaign_type,
    start_date, end_date, budget_limit, target_audience
) VALUES (
    'campaign-001', 'Tết 2025 Sale',
    'Chiến dịch khuyến mãi Tết Nguyên Đán 2025',
    'seasonal', NOW(), NOW() + INTERVAL '15 days',
    5000000, '{"segments": ["all"], "regions": ["vietnam"]}'::jsonb
);

-- Link vouchers to campaign
INSERT INTO campaign_vouchers (campaign_id, voucher_id) VALUES
('campaign-001', 'voucher-001'),
('campaign-001', 'voucher-002'),
('campaign-001', 'voucher-003');

-- ===================================
-- USEFUL VIEWS
-- ===================================

-- View for active vouchers with usage stats
CREATE OR REPLACE VIEW active_vouchers_stats AS
SELECT
    v.voucher_id,
    v.code,
    v.name,
    v.voucher_type,
    v.discount_value,
    v.usage_limit,
    v.start_date,
    v.end_date,
    COALESCE(usage_stats.usage_count, 0) as current_usage,
    COALESCE(usage_stats.total_discount_given, 0) as total_discount_given,
    CASE
        WHEN v.usage_limit IS NULL THEN 100.0
        ELSE (COALESCE(usage_stats.usage_count, 0)::float / v.usage_limit * 100)
    END as usage_percentage,
    CASE
        WHEN NOW() > v.end_date THEN 'expired'
        WHEN v.usage_limit IS NOT NULL AND COALESCE(usage_stats.usage_count, 0) >= v.usage_limit THEN 'used_up'
        WHEN NOW() < v.start_date THEN 'not_started'
        ELSE 'active'
    END as current_status
FROM vouchers v
LEFT JOIN (
    SELECT
        voucher_id,
        COUNT(*) as usage_count,
        SUM(discount_applied) as total_discount_given
    FROM voucher_usage
    GROUP BY voucher_id
) usage_stats ON v.voucher_id = usage_stats.voucher_id
WHERE v.status = 'active';

-- View for daily voucher usage analytics
CREATE OR REPLACE VIEW daily_voucher_analytics AS
SELECT
    DATE(vu.used_at) as usage_date,
    COUNT(*) as total_usage,
    COUNT(DISTINCT vu.customer_id) as unique_customers,
    SUM(vu.discount_applied) as total_discount_given,
    AVG(vu.order_amount) as avg_order_value,
    COUNT(DISTINCT vu.voucher_id) as vouchers_used
FROM voucher_usage vu
WHERE vu.used_at >= NOW() - INTERVAL '30 days'
GROUP BY DATE(vu.used_at)
ORDER BY usage_date DESC;

-- View for customer voucher usage patterns
CREATE OR REPLACE VIEW customer_voucher_patterns AS
SELECT
    vu.customer_id,
    COUNT(*) as total_vouchers_used,
    SUM(vu.discount_applied) as total_savings,
    AVG(vu.order_amount) as avg_order_value,
    MIN(vu.used_at) as first_voucher_usage,
    MAX(vu.used_at) as last_voucher_usage,
    COUNT(DISTINCT v.voucher_type) as voucher_types_used,
    ARRAY_AGG(DISTINCT v.voucher_type) as preferred_voucher_types
FROM voucher_usage vu
JOIN vouchers v ON vu.voucher_id = v.voucher_id
GROUP BY vu.customer_id
HAVING COUNT(*) >= 2;

-- ===================================
-- FUNCTIONS
-- ===================================

-- Function to auto-expire vouchers
CREATE OR REPLACE FUNCTION auto_expire_vouchers()
RETURNS INTEGER AS $$
DECLARE
    expired_count INTEGER;
BEGIN
    UPDATE vouchers
    SET status = 'expired', updated_at = NOW()
    WHERE status = 'active'
    AND end_date < NOW();

    GET DIAGNOSTICS expired_count = ROW_COUNT;
    RETURN expired_count;
END;
$$ LANGUAGE plpgsql;

-- Function to auto-mark used up vouchers
CREATE OR REPLACE FUNCTION auto_mark_used_up_vouchers()
RETURNS INTEGER AS $$
DECLARE
    used_up_count INTEGER;
BEGIN
    UPDATE vouchers
    SET status = 'used_up', updated_at = NOW()
    WHERE status = 'active'
    AND usage_limit IS NOT NULL
    AND voucher_id IN (
        SELECT voucher_id
        FROM voucher_usage
        GROUP BY voucher_id
        HAVING COUNT(*) >= vouchers.usage_limit
    );

    GET DIAGNOSTICS used_up_count = ROW_COUNT;
    RETURN used_up_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get voucher recommendations for customer
CREATE OR REPLACE FUNCTION get_customer_voucher_recommendations(
    p_customer_id VARCHAR(36),
    p_order_amount DECIMAL(10,2) DEFAULT 0
)
RETURNS TABLE (
    voucher_id VARCHAR(36),
    code VARCHAR(20),
    name VARCHAR(100),
    discount_value DECIMAL(10,2),
    estimated_discount DECIMAL(10,2),
    savings_percentage DECIMAL(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        v.voucher_id,
        v.code,
        v.name,
        v.discount_value,
        CASE
            WHEN v.voucher_type = 'percentage' THEN
                LEAST(p_order_amount * (v.discount_value / 100), COALESCE(v.max_discount_amount, p_order_amount))
            WHEN v.voucher_type = 'fixed_amount' THEN
                LEAST(v.discount_value, p_order_amount)
            WHEN v.voucher_type = 'free_shipping' THEN
                30000  -- Assumed shipping cost
            ELSE 0
        END as estimated_discount,
        CASE
            WHEN p_order_amount > 0 THEN
                (CASE
                    WHEN v.voucher_type = 'percentage' THEN
                        LEAST(p_order_amount * (v.discount_value / 100), COALESCE(v.max_discount_amount, p_order_amount))
                    WHEN v.voucher_type = 'fixed_amount' THEN
                        LEAST(v.discount_value, p_order_amount)
                    WHEN v.voucher_type = 'free_shipping' THEN
                        30000
                    ELSE 0
                END / p_order_amount * 100)
            ELSE 0
        END as savings_percentage
    FROM vouchers v
    WHERE v.status = 'active'
    AND NOW() BETWEEN v.start_date AND v.end_date
    AND (v.min_order_amount <= p_order_amount OR p_order_amount = 0)
    AND (
        v.target_type = 'all_customers' OR
        (v.target_type = 'customer_segment' AND EXISTS (
            SELECT 1 FROM customers c
            WHERE c.customer_id = p_customer_id
            AND c.customer_segment = ANY(SELECT jsonb_array_elements_text(v.target_segments))
        )) OR
        (v.target_type = 'specific_customers' AND p_customer_id = ANY(SELECT jsonb_array_elements_text(v.target_customer_ids)))
    )
    AND (
        v.usage_limit IS NULL OR
        (SELECT COUNT(*) FROM voucher_usage WHERE voucher_id = v.voucher_id) < v.usage_limit
    )
    AND (
        SELECT COUNT(*) FROM voucher_usage
        WHERE voucher_id = v.voucher_id AND customer_id = p_customer_id
    ) < v.usage_limit_per_customer
    ORDER BY estimated_discount DESC;
END;
$$ LANGUAGE plpgsql;