-- ===================================
-- CUSTOMER CLASSIFICATION SCHEMA
-- ===================================
-- Advanced customer segmentation and behavior analysis

-- Create customer classifications table
CREATE TABLE IF NOT EXISTS customer_classifications (
    classification_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(36) UNIQUE NOT NULL,
    rfm_segment VARCHAR(20) NOT NULL CHECK (rfm_segment IN (
        'champion', 'loyal', 'potential_loyalist', 'new_customer', 'promising',
        'need_attention', 'about_to_sleep', 'at_risk', 'cannot_lose', 'hibernating', 'lost'
    )),
    behavior_type VARCHAR(20) NOT NULL CHECK (behavior_type IN (
        'browser', 'converter', 'bargain_hunter', 'loyalist', 'occasional', 'impulse_buyer'
    )),
    recency_score INTEGER CHECK (recency_score BETWEEN 1 AND 5),
    frequency_score INTEGER CHECK (frequency_score BETWEEN 1 AND 5),
    monetary_score INTEGER CHECK (monetary_score BETWEEN 1 AND 5),
    clv_prediction DECIMAL(12,2) DEFAULT 0,
    churn_probability DECIMAL(3,2) CHECK (churn_probability BETWEEN 0 AND 1),
    analysis_date TIMESTAMP DEFAULT NOW(),
    rfm_data JSONB DEFAULT '{}'::jsonb,
    behavioral_data JSONB DEFAULT '{}'::jsonb,
    last_updated TIMESTAMP DEFAULT NOW(),

    -- Indexes
    INDEX idx_customer_classifications_segment (rfm_segment),
    INDEX idx_customer_classifications_behavior (behavior_type),
    INDEX idx_customer_classifications_churn (churn_probability),
    INDEX idx_customer_classifications_clv (clv_prediction),
    INDEX idx_customer_classifications_updated (last_updated)
);

-- Create customer behavior tracking table
CREATE TABLE IF NOT EXISTS customer_behavior_logs (
    behavior_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(36) NOT NULL,
    behavior_type VARCHAR(50) NOT NULL,
    behavior_data JSONB DEFAULT '{}'::jsonb,
    session_id VARCHAR(36),
    page_url TEXT,
    action_taken VARCHAR(100),
    timestamp TIMESTAMP DEFAULT NOW(),
    device_type VARCHAR(20),
    user_agent TEXT,

    -- Indexes
    INDEX idx_behavior_logs_customer (customer_id),
    INDEX idx_behavior_logs_type (behavior_type),
    INDEX idx_behavior_logs_timestamp (timestamp),
    INDEX idx_behavior_logs_session (session_id)
);

-- Create customer journey stages table
CREATE TABLE IF NOT EXISTS customer_journey_stages (
    journey_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(36) NOT NULL,
    stage_name VARCHAR(50) NOT NULL CHECK (stage_name IN (
        'awareness', 'consideration', 'purchase', 'retention', 'advocacy', 'churn'
    )),
    stage_entered_at TIMESTAMP DEFAULT NOW(),
    stage_completed_at TIMESTAMP,
    stage_data JSONB DEFAULT '{}'::jsonb,
    next_stage VARCHAR(50),
    conversion_probability DECIMAL(3,2),

    -- Indexes
    INDEX idx_journey_stages_customer (customer_id),
    INDEX idx_journey_stages_stage (stage_name),
    INDEX idx_journey_stages_entered (stage_entered_at)
);

-- Create customer engagement scores table
CREATE TABLE IF NOT EXISTS customer_engagement_scores (
    engagement_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(36) UNIQUE NOT NULL,
    email_engagement_score DECIMAL(3,2) DEFAULT 0,
    website_engagement_score DECIMAL(3,2) DEFAULT 0,
    purchase_engagement_score DECIMAL(3,2) DEFAULT 0,
    social_engagement_score DECIMAL(3,2) DEFAULT 0,
    overall_engagement_score DECIMAL(3,2) DEFAULT 0,
    last_email_open TIMESTAMP,
    last_website_visit TIMESTAMP,
    last_purchase TIMESTAMP,
    engagement_trend VARCHAR(20) CHECK (engagement_trend IN ('increasing', 'stable', 'decreasing', 'inactive')),
    calculated_at TIMESTAMP DEFAULT NOW(),

    -- Indexes
    INDEX idx_engagement_customer (customer_id),
    INDEX idx_engagement_overall (overall_engagement_score),
    INDEX idx_engagement_trend (engagement_trend),
    INDEX idx_engagement_calculated (calculated_at)
);

-- Create customer value tiers table
CREATE TABLE IF NOT EXISTS customer_value_tiers (
    tier_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(36) UNIQUE NOT NULL,
    current_tier VARCHAR(20) NOT NULL CHECK (current_tier IN (
        'diamond', 'platinum', 'gold', 'silver', 'bronze', 'new'
    )),
    previous_tier VARCHAR(20),
    tier_changed_at TIMESTAMP DEFAULT NOW(),
    tier_points INTEGER DEFAULT 0,
    tier_benefits JSONB DEFAULT '[]'::jsonb,
    next_tier_requirements JSONB DEFAULT '{}'::jsonb,
    tier_expiry_date TIMESTAMP,

    -- Indexes
    INDEX idx_value_tiers_customer (customer_id),
    INDEX idx_value_tiers_current (current_tier),
    INDEX idx_value_tiers_points (tier_points)
);

-- Create predictive model results table
CREATE TABLE IF NOT EXISTS customer_predictions (
    prediction_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(36) NOT NULL,
    model_name VARCHAR(50) NOT NULL,
    model_version VARCHAR(20) DEFAULT '1.0',
    prediction_type VARCHAR(30) NOT NULL CHECK (prediction_type IN (
        'churn_probability', 'clv_prediction', 'next_purchase_date', 'category_preference',
        'price_sensitivity', 'promotion_response', 'upsell_probability'
    )),
    prediction_value DECIMAL(10,2),
    prediction_confidence DECIMAL(3,2),
    prediction_data JSONB DEFAULT '{}'::jsonb,
    predicted_at TIMESTAMP DEFAULT NOW(),
    valid_until TIMESTAMP,

    -- Indexes
    INDEX idx_predictions_customer (customer_id),
    INDEX idx_predictions_type (prediction_type),
    INDEX idx_predictions_model (model_name),
    INDEX idx_predictions_predicted (predicted_at)
);

-- ===================================
-- SAMPLE DATA
-- ===================================

-- Insert sample customer classifications
INSERT INTO customer_classifications (
    customer_id, rfm_segment, behavior_type, recency_score, frequency_score,
    monetary_score, clv_prediction, churn_probability
) VALUES
('customer-001', 'champion', 'loyalist', 5, 5, 5, 15000000, 0.05),
('customer-002', 'loyal', 'converter', 4, 4, 4, 8000000, 0.15),
('customer-003', 'at_risk', 'bargain_hunter', 2, 3, 3, 3000000, 0.75),
('customer-004', 'new_customer', 'browser', 5, 1, 2, 2000000, 0.45),
('customer-005', 'potential_loyalist', 'impulse_buyer', 4, 2, 4, 6000000, 0.25);

-- Insert sample behavior logs
INSERT INTO customer_behavior_logs (
    customer_id, behavior_type, action_taken, session_id, device_type
) VALUES
('customer-001', 'product_view', 'viewed_product_details', 'session-001', 'desktop'),
('customer-001', 'add_to_cart', 'added_item_to_cart', 'session-001', 'desktop'),
('customer-002', 'price_comparison', 'compared_similar_products', 'session-002', 'mobile'),
('customer-003', 'coupon_search', 'searched_for_discounts', 'session-003', 'desktop'),
('customer-004', 'wishlist_add', 'added_to_wishlist', 'session-004', 'mobile');

-- Insert sample journey stages
INSERT INTO customer_journey_stages (
    customer_id, stage_name, stage_completed_at, next_stage, conversion_probability
) VALUES
('customer-001', 'purchase', NOW() - INTERVAL '30 days', 'retention', 0.85),
('customer-002', 'retention', NOW() - INTERVAL '15 days', 'advocacy', 0.60),
('customer-003', 'consideration', NOW() - INTERVAL '7 days', 'purchase', 0.35),
('customer-004', 'awareness', NOW() - INTERVAL '3 days', 'consideration', 0.40),
('customer-005', 'purchase', NOW() - INTERVAL '45 days', 'retention', 0.70);

-- Insert sample engagement scores
INSERT INTO customer_engagement_scores (
    customer_id, email_engagement_score, website_engagement_score,
    purchase_engagement_score, overall_engagement_score, engagement_trend
) VALUES
('customer-001', 0.85, 0.90, 0.95, 0.90, 'stable'),
('customer-002', 0.70, 0.80, 0.85, 0.78, 'increasing'),
('customer-003', 0.30, 0.40, 0.20, 0.30, 'decreasing'),
('customer-004', 0.60, 0.75, 0.10, 0.48, 'stable'),
('customer-005', 0.80, 0.70, 0.60, 0.70, 'stable');

-- Insert sample value tiers
INSERT INTO customer_value_tiers (
    customer_id, current_tier, tier_points, tier_benefits
) VALUES
('customer-001', 'platinum', 15000, '["free_shipping", "priority_support", "exclusive_deals"]'::jsonb),
('customer-002', 'gold', 8000, '["free_shipping", "birthday_discount"]'::jsonb),
('customer-003', 'silver', 3000, '["loyalty_points", "basic_support"]'::jsonb),
('customer-004', 'bronze', 500, '["welcome_bonus"]'::jsonb),
('customer-005', 'gold', 6000, '["free_shipping", "birthday_discount"]'::jsonb);

-- ===================================
-- VIEWS
-- ===================================

-- View for comprehensive customer profiles
CREATE OR REPLACE VIEW customer_profiles_comprehensive AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.registration_date,
    cc.rfm_segment,
    cc.behavior_type,
    cc.clv_prediction,
    cc.churn_probability,
    es.overall_engagement_score,
    es.engagement_trend,
    vt.current_tier,
    vt.tier_points,
    js.stage_name as current_journey_stage,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount_vnd) as total_spent,
    MAX(o.order_date) as last_order_date,
    DATE_PART('day', NOW() - MAX(o.order_date)) as days_since_last_order
FROM customers c
LEFT JOIN customer_classifications cc ON c.customer_id = cc.customer_id
LEFT JOIN customer_engagement_scores es ON c.customer_id = es.customer_id
LEFT JOIN customer_value_tiers vt ON c.customer_id = vt.customer_id
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN (
    SELECT DISTINCT ON (customer_id) customer_id, stage_name
    FROM customer_journey_stages
    ORDER BY customer_id, stage_entered_at DESC
) js ON c.customer_id = js.customer_id
GROUP BY c.customer_id, c.customer_name, c.email, c.registration_date,
         cc.rfm_segment, cc.behavior_type, cc.clv_prediction, cc.churn_probability,
         es.overall_engagement_score, es.engagement_trend, vt.current_tier,
         vt.tier_points, js.stage_name;

-- View for segment performance metrics
CREATE OR REPLACE VIEW segment_performance_metrics AS
SELECT
    cc.rfm_segment,
    COUNT(*) as customer_count,
    AVG(cc.clv_prediction) as avg_clv,
    AVG(cc.churn_probability) as avg_churn_risk,
    AVG(es.overall_engagement_score) as avg_engagement,
    COUNT(CASE WHEN cc.churn_probability > 0.7 THEN 1 END) as high_risk_customers,
    COUNT(CASE WHEN cc.churn_probability < 0.3 THEN 1 END) as low_risk_customers,
    SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days' THEN o.total_amount_vnd ELSE 0 END) as monthly_revenue,
    AVG(CASE WHEN o.order_date >= NOW() - INTERVAL '30 days' THEN o.total_amount_vnd END) as avg_monthly_order_value
FROM customer_classifications cc
LEFT JOIN customer_engagement_scores es ON cc.customer_id = es.customer_id
LEFT JOIN orders o ON cc.customer_id = o.customer_id
GROUP BY cc.rfm_segment
ORDER BY avg_clv DESC;

-- View for behavior analysis
CREATE OR REPLACE VIEW customer_behavior_analysis AS
SELECT
    cc.behavior_type,
    COUNT(*) as customer_count,
    AVG(cc.clv_prediction) as avg_clv,
    AVG(es.overall_engagement_score) as avg_engagement,
    COUNT(CASE WHEN cc.churn_probability > 0.5 THEN 1 END) as churn_risk_count,
    AVG(order_stats.avg_order_value) as avg_order_value,
    AVG(order_stats.order_frequency) as avg_order_frequency
FROM customer_classifications cc
LEFT JOIN customer_engagement_scores es ON cc.customer_id = es.customer_id
LEFT JOIN (
    SELECT
        customer_id,
        AVG(total_amount_vnd) as avg_order_value,
        COUNT(*) as order_frequency
    FROM orders
    WHERE order_date >= NOW() - INTERVAL '365 days'
    GROUP BY customer_id
) order_stats ON cc.customer_id = order_stats.customer_id
GROUP BY cc.behavior_type
ORDER BY avg_clv DESC;

-- ===================================
-- FUNCTIONS
-- ===================================

-- Function to calculate engagement score
CREATE OR REPLACE FUNCTION calculate_engagement_score(p_customer_id VARCHAR(36))
RETURNS DECIMAL(3,2) AS $$
DECLARE
    email_score DECIMAL(3,2) := 0;
    website_score DECIMAL(3,2) := 0;
    purchase_score DECIMAL(3,2) := 0;
    overall_score DECIMAL(3,2);
BEGIN
    -- Calculate email engagement (based on opens, clicks in last 30 days)
    SELECT LEAST(COUNT(*) / 10.0, 1.0) INTO email_score
    FROM customer_behavior_logs
    WHERE customer_id = p_customer_id
    AND behavior_type IN ('email_open', 'email_click')
    AND timestamp >= NOW() - INTERVAL '30 days';

    -- Calculate website engagement (based on sessions, page views)
    SELECT LEAST(COUNT(DISTINCT session_id) / 20.0, 1.0) INTO website_score
    FROM customer_behavior_logs
    WHERE customer_id = p_customer_id
    AND behavior_type IN ('page_view', 'product_view', 'search')
    AND timestamp >= NOW() - INTERVAL '30 days';

    -- Calculate purchase engagement (based on recent purchases)
    SELECT LEAST(COUNT(*) / 5.0, 1.0) INTO purchase_score
    FROM orders
    WHERE customer_id = p_customer_id
    AND order_date >= NOW() - INTERVAL '90 days';

    -- Calculate overall score
    overall_score := (email_score * 0.3 + website_score * 0.3 + purchase_score * 0.4);

    -- Update engagement scores table
    INSERT INTO customer_engagement_scores (
        customer_id, email_engagement_score, website_engagement_score,
        purchase_engagement_score, overall_engagement_score, calculated_at
    ) VALUES (
        p_customer_id, email_score, website_score, purchase_score, overall_score, NOW()
    )
    ON CONFLICT (customer_id)
    DO UPDATE SET
        email_engagement_score = EXCLUDED.email_engagement_score,
        website_engagement_score = EXCLUDED.website_engagement_score,
        purchase_engagement_score = EXCLUDED.purchase_engagement_score,
        overall_engagement_score = EXCLUDED.overall_engagement_score,
        calculated_at = EXCLUDED.calculated_at;

    RETURN overall_score;
END;
$$ LANGUAGE plpgsql;

-- Function to update customer tier
CREATE OR REPLACE FUNCTION update_customer_tier(p_customer_id VARCHAR(36))
RETURNS VARCHAR(20) AS $$
DECLARE
    total_spent DECIMAL(12,2);
    order_count INTEGER;
    new_tier VARCHAR(20);
    current_tier VARCHAR(20);
BEGIN
    -- Get customer spending data
    SELECT
        COALESCE(SUM(total_amount_vnd), 0),
        COUNT(*)
    INTO total_spent, order_count
    FROM orders
    WHERE customer_id = p_customer_id
    AND order_date >= NOW() - INTERVAL '365 days';

    -- Determine new tier
    IF total_spent >= 50000000 AND order_count >= 20 THEN
        new_tier := 'diamond';
    ELSIF total_spent >= 20000000 AND order_count >= 10 THEN
        new_tier := 'platinum';
    ELSIF total_spent >= 10000000 AND order_count >= 5 THEN
        new_tier := 'gold';
    ELSIF total_spent >= 5000000 AND order_count >= 3 THEN
        new_tier := 'silver';
    ELSIF total_spent >= 1000000 OR order_count >= 1 THEN
        new_tier := 'bronze';
    ELSE
        new_tier := 'new';
    END IF;

    -- Get current tier
    SELECT current_tier INTO current_tier
    FROM customer_value_tiers
    WHERE customer_id = p_customer_id;

    -- Update tier if changed
    IF current_tier IS NULL OR current_tier != new_tier THEN
        INSERT INTO customer_value_tiers (
            customer_id, current_tier, previous_tier, tier_changed_at, tier_points
        ) VALUES (
            p_customer_id, new_tier, current_tier, NOW(), total_spent / 10000
        )
        ON CONFLICT (customer_id)
        DO UPDATE SET
            previous_tier = customer_value_tiers.current_tier,
            current_tier = EXCLUDED.current_tier,
            tier_changed_at = EXCLUDED.tier_changed_at,
            tier_points = EXCLUDED.tier_points;
    END IF;

    RETURN new_tier;
END;
$$ LANGUAGE plpgsql;

-- Function for automated customer classification
CREATE OR REPLACE FUNCTION auto_classify_customer(p_customer_id VARCHAR(36))
RETURNS JSONB AS $$
DECLARE
    rfm_data JSONB;
    classification_result JSONB;
BEGIN
    -- This is a simplified version - in practice, you'd call your Python ML service
    -- For now, return basic classification based on order history

    WITH customer_metrics AS (
        SELECT
            customer_id,
            DATE_PART('day', NOW() - MAX(order_date)) as recency_days,
            COUNT(*) as order_frequency,
            SUM(total_amount_vnd) as total_monetary,
            NTILE(5) OVER (ORDER BY DATE_PART('day', NOW() - MAX(order_date)) ASC) as recency_score,
            NTILE(5) OVER (ORDER BY COUNT(*) DESC) as frequency_score,
            NTILE(5) OVER (ORDER BY SUM(total_amount_vnd) DESC) as monetary_score
        FROM orders
        WHERE customer_id = p_customer_id
        AND order_date >= NOW() - INTERVAL '365 days'
        GROUP BY customer_id
    )
    SELECT jsonb_build_object(
        'customer_id', customer_id,
        'recency_days', recency_days,
        'order_frequency', order_frequency,
        'total_monetary', total_monetary,
        'recency_score', recency_score,
        'frequency_score', frequency_score,
        'monetary_score', monetary_score,
        'rfm_total', recency_score + frequency_score + monetary_score
    ) INTO rfm_data
    FROM customer_metrics;

    RETURN rfm_data;
END;
$$ LANGUAGE plpgsql;