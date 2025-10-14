-- ===================================
-- FEEDBACK ANALYSIS SCHEMA
-- ===================================
-- Database schema for comprehensive feedback analysis and sentiment processing

-- Create feedback analysis table
CREATE TABLE IF NOT EXISTS feedback_analysis (
    feedback_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(36),
    product_id VARCHAR(36),
    order_id VARCHAR(36),
    source VARCHAR(20) NOT NULL CHECK (source IN (
        'product_review', 'order_feedback', 'customer_support',
        'social_media', 'survey', 'email'
    )),
    original_text TEXT NOT NULL,
    cleaned_text TEXT,
    sentiment_score DECIMAL(4,3) CHECK (sentiment_score BETWEEN -1 AND 1),
    sentiment_label VARCHAR(20) NOT NULL CHECK (sentiment_label IN (
        'very_positive', 'positive', 'neutral', 'negative', 'very_negative'
    )),
    confidence DECIMAL(3,2) CHECK (confidence BETWEEN 0 AND 1),
    categories JSONB DEFAULT '[]'::jsonb,
    priority VARCHAR(10) NOT NULL CHECK (priority IN ('critical', 'high', 'medium', 'low')),
    key_phrases JSONB DEFAULT '[]'::jsonb,
    emotions JSONB DEFAULT '{}'::jsonb,
    action_required BOOLEAN DEFAULT FALSE,
    recommended_response TEXT,
    response_sent BOOLEAN DEFAULT FALSE,
    response_sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP DEFAULT NOW(),

    -- Foreign key constraints
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,

    -- Indexes for performance
    INDEX idx_feedback_customer (customer_id),
    INDEX idx_feedback_product (product_id),
    INDEX idx_feedback_order (order_id),
    INDEX idx_feedback_sentiment (sentiment_label),
    INDEX idx_feedback_priority (priority),
    INDEX idx_feedback_action_required (action_required),
    INDEX idx_feedback_created (created_at),
    INDEX idx_feedback_source (source)
);

-- Create sentiment metrics table for aggregated data
CREATE TABLE IF NOT EXISTS sentiment_metrics (
    metric_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    source VARCHAR(20),
    category VARCHAR(20),
    total_feedback INTEGER DEFAULT 0,
    positive_count INTEGER DEFAULT 0,
    negative_count INTEGER DEFAULT 0,
    neutral_count INTEGER DEFAULT 0,
    avg_sentiment_score DECIMAL(4,3),
    top_keywords JSONB DEFAULT '[]'::jsonb,
    calculated_at TIMESTAMP DEFAULT NOW(),

    -- Unique constraint for daily metrics
    UNIQUE(date, source, category),

    -- Indexes
    INDEX idx_sentiment_metrics_date (date),
    INDEX idx_sentiment_metrics_source (source),
    INDEX idx_sentiment_metrics_category (category)
);

-- Create feedback topics table for trending analysis
CREATE TABLE IF NOT EXISTS feedback_topics (
    topic_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_name VARCHAR(100) NOT NULL,
    keywords JSONB DEFAULT '[]'::jsonb,
    category VARCHAR(20),
    sentiment_trend VARCHAR(20) CHECK (sentiment_trend IN ('improving', 'stable', 'declining')),
    frequency_last_7d INTEGER DEFAULT 0,
    frequency_last_30d INTEGER DEFAULT 0,
    avg_sentiment_score DECIMAL(4,3),
    first_mentioned TIMESTAMP,
    last_mentioned TIMESTAMP,
    is_trending BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Indexes
    INDEX idx_topics_name (topic_name),
    INDEX idx_topics_category (category),
    INDEX idx_topics_trending (is_trending),
    INDEX idx_topics_frequency (frequency_last_30d)
);

-- Create feedback responses table for tracking responses
CREATE TABLE IF NOT EXISTS feedback_responses (
    response_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    feedback_id VARCHAR(36) NOT NULL REFERENCES feedback_analysis(feedback_id),
    response_type VARCHAR(20) CHECK (response_type IN ('automated', 'manual', 'template')),
    response_text TEXT NOT NULL,
    sent_by VARCHAR(36), -- staff member ID
    sent_via VARCHAR(20) CHECK (sent_via IN ('email', 'sms', 'phone', 'chat', 'in_app')),
    sent_at TIMESTAMP DEFAULT NOW(),
    customer_replied BOOLEAN DEFAULT FALSE,
    customer_reply_at TIMESTAMP,
    resolution_status VARCHAR(20) DEFAULT 'pending' CHECK (resolution_status IN (
        'pending', 'in_progress', 'resolved', 'escalated', 'closed'
    )),

    -- Indexes
    INDEX idx_responses_feedback (feedback_id),
    INDEX idx_responses_sent_by (sent_by),
    INDEX idx_responses_status (resolution_status),
    INDEX idx_responses_sent_at (sent_at)
);

-- Create automated alerts table
CREATE TABLE IF NOT EXISTS sentiment_alerts (
    alert_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_type VARCHAR(30) NOT NULL CHECK (alert_type IN (
        'negative_spike', 'critical_feedback', 'trending_issue', 'competitor_mention',
        'product_quality_decline', 'service_issue'
    )),
    severity VARCHAR(10) NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    title VARCHAR(200) NOT NULL,
    description TEXT,
    related_feedback_ids JSONB DEFAULT '[]'::jsonb,
    threshold_value DECIMAL(10,2),
    actual_value DECIMAL(10,2),
    affected_products JSONB DEFAULT '[]'::jsonb,
    affected_categories JSONB DEFAULT '[]'::jsonb,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'acknowledged', 'resolved', 'dismissed')),
    acknowledged_by VARCHAR(36),
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Indexes
    INDEX idx_alerts_type (alert_type),
    INDEX idx_alerts_severity (severity),
    INDEX idx_alerts_status (status),
    INDEX idx_alerts_created (created_at)
);

-- ===================================
-- SAMPLE DATA
-- ===================================

-- Insert sample feedback analysis
INSERT INTO feedback_analysis (
    feedback_id, customer_id, product_id, source, original_text,
    sentiment_score, sentiment_label, confidence, categories, priority,
    key_phrases, emotions, action_required, recommended_response
) VALUES
(
    'feedback-001', 'customer-001', 'product-001', 'product_review',
    'Sản phẩm rất tuyệt vời, chất lượng tốt, giao hàng nhanh. Tôi rất hài lòng!',
    0.85, 'very_positive', 0.92, '["product_quality", "shipping"]'::jsonb, 'low',
    '["sản phẩm tuyệt vời", "chất lượng tốt", "giao hàng nhanh"]'::jsonb,
    '{"joy": 0.9, "satisfaction": 0.8}'::jsonb, FALSE,
    'Cảm ơn bạn đã chia sẻ trải nghiệm tuyệt vời! Chúng tôi rất vui khi bạn hài lòng với dịch vụ.'
),
(
    'feedback-002', 'customer-002', 'product-002', 'order_feedback',
    'Sản phẩm ok nhưng giao hàng hơi chậm, nhân viên tư vấn nhiệt tình.',
    0.2, 'neutral', 0.65, '["shipping", "customer_service"]'::jsonb, 'medium',
    '["giao hàng chậm", "nhân viên nhiệt tình"]'::jsonb,
    '{"neutral": 0.6, "satisfaction": 0.4}'::jsonb, FALSE,
    'Cảm ơn bạn đã phản hồi. Chúng tôi sẽ cải thiện quy trình giao hàng.'
),
(
    'feedback-003', 'customer-003', 'product-003', 'customer_support',
    'Sản phẩm không đúng mô tả, chất lượng kém, rất thất vọng. Muốn trả hàng!',
    -0.75, 'very_negative', 0.88, '["product_quality"]'::jsonb, 'critical',
    '["không đúng mô tả", "chất lượng kém", "thất vọng"]'::jsonb,
    '{"anger": 0.7, "sadness": 0.6}'::jsonb, TRUE,
    'Chúng tôi rất xin lỗi về vấn đề này. Chúng tôi sẽ liên hệ trực tiếp để khắc phục ngay lập tức.'
),
(
    'feedback-004', 'customer-004', 'product-001', 'social_media',
    'Giá hơi đắt nhưng chất lượng ổn, dịch vụ tốt.',
    0.4, 'positive', 0.70, '["pricing", "product_quality", "customer_service"]'::jsonb, 'low',
    '["giá đắt", "chất lượng ổn", "dịch vụ tốt"]'::jsonb,
    '{"satisfaction": 0.6, "concern": 0.3}'::jsonb, FALSE,
    'Cảm ơn bạn đã đánh giá tích cực! Chúng tôi sẽ tiếp tục cải thiện để phục vụ bạn tốt hơn.'
),
(
    'feedback-005', 'customer-005', 'product-004', 'survey',
    'Website dễ sử dụng, thanh toán thuận tiện, sẽ quay lại mua.',
    0.6, 'positive', 0.75, '["website_ux"]'::jsonb, 'low',
    '["website dễ sử dụng", "thanh toán thuận tiện"]'::jsonb,
    '{"satisfaction": 0.7, "joy": 0.5}'::jsonb, FALSE,
    'Cảm ơn bạn đã đánh giá tích cực! Chúng tôi sẽ tiếp tục cải thiện để phục vụ bạn tốt hơn.'
);

-- Insert sample sentiment metrics
INSERT INTO sentiment_metrics (
    date, source, category, total_feedback, positive_count, negative_count,
    neutral_count, avg_sentiment_score, top_keywords
) VALUES
(CURRENT_DATE - INTERVAL '1 day', 'product_review', 'product_quality', 45, 32, 8, 5, 0.42,
 '["chất lượng tốt", "sản phẩm tuyệt vời", "hài lòng"]'::jsonb),
(CURRENT_DATE - INTERVAL '1 day', 'order_feedback', 'shipping', 28, 18, 7, 3, 0.15,
 '["giao hàng nhanh", "đúng hẹn", "chậm trễ"]'::jsonb),
(CURRENT_DATE - INTERVAL '1 day', 'customer_support', 'customer_service', 22, 15, 4, 3, 0.38,
 '["nhân viên nhiệt tình", "hỗ trợ tốt", "thái độ"]'::jsonb);

-- Insert sample topics
INSERT INTO feedback_topics (
    topic_name, keywords, category, sentiment_trend, frequency_last_7d,
    frequency_last_30d, avg_sentiment_score, is_trending
) VALUES
('Chất lượng sản phẩm', '["chất lượng", "sản phẩm", "tốt", "kém"]'::jsonb,
 'product_quality', 'stable', 35, 142, 0.45, TRUE),
('Tốc độ giao hàng', '["giao hàng", "nhanh", "chậm", "đúng hẹn"]'::jsonb,
 'shipping', 'improving', 28, 118, 0.22, TRUE),
('Dịch vụ khách hàng', '["nhân viên", "hỗ trợ", "tư vấn", "thái độ"]'::jsonb,
 'customer_service', 'stable', 22, 95, 0.38, FALSE),
('Giá cả sản phẩm', '["giá", "đắt", "rẻ", "phù hợp"]'::jsonb,
 'pricing', 'declining', 18, 78, 0.12, FALSE);

-- Insert sample alerts
INSERT INTO sentiment_alerts (
    alert_type, severity, title, description, threshold_value, actual_value,
    affected_products, status
) VALUES
('negative_spike', 'high', 'Tăng đột biến phản hồi tiêu cực',
 'Phản hồi tiêu cực tăng 45% trong 24h qua', 0.15, 0.22,
 '["product-003", "product-007"]'::jsonb, 'active'),
('critical_feedback', 'critical', 'Phản hồi nghiêm trọng từ khách VIP',
 'Khách hàng VIP phản ánh vấn đề chất lượng sản phẩm', 1, 1,
 '["product-003"]'::jsonb, 'active'),
('trending_issue', 'medium', 'Vấn đề giao hàng chậm đang gia tăng',
 'Phàn nàn về giao hàng chậm tăng 30% trong tuần', 0.20, 0.26,
 '[]'::jsonb, 'acknowledged');

-- ===================================
-- VIEWS
-- ===================================

-- View for daily sentiment dashboard
CREATE OR REPLACE VIEW daily_sentiment_dashboard AS
SELECT
    DATE(created_at) as date,
    COUNT(*) as total_feedback,
    COUNT(CASE WHEN sentiment_label IN ('positive', 'very_positive') THEN 1 END) as positive_count,
    COUNT(CASE WHEN sentiment_label IN ('negative', 'very_negative') THEN 1 END) as negative_count,
    COUNT(CASE WHEN sentiment_label = 'neutral' THEN 1 END) as neutral_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment_score,
    COUNT(CASE WHEN action_required = TRUE THEN 1 END) as action_required_count,
    COUNT(CASE WHEN priority = 'critical' THEN 1 END) as critical_count
FROM feedback_analysis
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- View for product sentiment summary
CREATE OR REPLACE VIEW product_sentiment_summary AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(fa.feedback_id) as total_feedback,
    ROUND(AVG(fa.sentiment_score), 3) as avg_sentiment_score,
    COUNT(CASE WHEN fa.sentiment_label IN ('positive', 'very_positive') THEN 1 END) as positive_count,
    COUNT(CASE WHEN fa.sentiment_label IN ('negative', 'very_negative') THEN 1 END) as negative_count,
    COUNT(CASE WHEN fa.action_required = TRUE THEN 1 END) as issues_count,
    MAX(fa.created_at) as last_feedback_date
FROM products p
LEFT JOIN feedback_analysis fa ON p.product_id = fa.product_id
WHERE fa.created_at >= CURRENT_DATE - INTERVAL '90 days' OR fa.created_at IS NULL
GROUP BY p.product_id, p.product_name, p.category
ORDER BY avg_sentiment_score ASC, issues_count DESC;

-- View for category sentiment trends
CREATE OR REPLACE VIEW category_sentiment_trends AS
SELECT
    category,
    DATE_TRUNC('week', created_at) as week,
    COUNT(*) as feedback_count,
    AVG(sentiment_score) as avg_sentiment,
    COUNT(CASE WHEN sentiment_label IN ('positive', 'very_positive') THEN 1 END) as positive_count,
    COUNT(CASE WHEN sentiment_label IN ('negative', 'very_negative') THEN 1 END) as negative_count
FROM feedback_analysis fa,
     jsonb_array_elements_text(fa.categories) as category
WHERE created_at >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY category, DATE_TRUNC('week', created_at)
ORDER BY week DESC, avg_sentiment ASC;

-- View for customer feedback patterns
CREATE OR REPLACE VIEW customer_feedback_patterns AS
SELECT
    fa.customer_id,
    c.customer_name,
    c.customer_segment,
    COUNT(*) as total_feedback,
    AVG(fa.sentiment_score) as avg_sentiment,
    COUNT(CASE WHEN fa.sentiment_label IN ('negative', 'very_negative') THEN 1 END) as negative_feedback_count,
    COUNT(CASE WHEN fa.action_required = TRUE THEN 1 END) as action_required_count,
    MAX(fa.created_at) as last_feedback_date,
    MIN(fa.created_at) as first_feedback_date
FROM feedback_analysis fa
LEFT JOIN customers c ON fa.customer_id = c.customer_id
WHERE fa.created_at >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY fa.customer_id, c.customer_name, c.customer_segment
HAVING COUNT(*) >= 2
ORDER BY negative_feedback_count DESC, avg_sentiment ASC;

-- ===================================
-- FUNCTIONS
-- ===================================

-- Function to calculate daily sentiment metrics
CREATE OR REPLACE FUNCTION calculate_daily_sentiment_metrics(target_date DATE DEFAULT CURRENT_DATE)
RETURNS VOID AS $$
BEGIN
    -- Delete existing metrics for the date
    DELETE FROM sentiment_metrics WHERE date = target_date;

    -- Insert new metrics by source and category
    INSERT INTO sentiment_metrics (
        date, source, category, total_feedback, positive_count, negative_count,
        neutral_count, avg_sentiment_score, top_keywords
    )
    SELECT
        target_date,
        fa.source,
        category,
        COUNT(*) as total_feedback,
        COUNT(CASE WHEN fa.sentiment_label IN ('positive', 'very_positive') THEN 1 END) as positive_count,
        COUNT(CASE WHEN fa.sentiment_label IN ('negative', 'very_negative') THEN 1 END) as negative_count,
        COUNT(CASE WHEN fa.sentiment_label = 'neutral' THEN 1 END) as neutral_count,
        AVG(fa.sentiment_score) as avg_sentiment_score,
        jsonb_agg(DISTINCT phrase) FILTER (WHERE phrase IS NOT NULL) as top_keywords
    FROM feedback_analysis fa,
         jsonb_array_elements_text(fa.categories) as category,
         jsonb_array_elements_text(fa.key_phrases) as phrase
    WHERE DATE(fa.created_at) = target_date
    GROUP BY fa.source, category;
END;
$$ LANGUAGE plpgsql;

-- Function to detect sentiment anomalies
CREATE OR REPLACE FUNCTION detect_sentiment_anomalies()
RETURNS TABLE (
    alert_type VARCHAR(30),
    severity VARCHAR(10),
    description TEXT,
    affected_items JSONB
) AS $$
DECLARE
    avg_negative_rate DECIMAL(3,2);
    current_negative_rate DECIMAL(3,2);
    threshold_multiplier DECIMAL(3,2) := 1.5;
BEGIN
    -- Calculate baseline negative feedback rate (last 30 days)
    SELECT
        COUNT(CASE WHEN sentiment_label IN ('negative', 'very_negative') THEN 1 END)::DECIMAL / COUNT(*)
    INTO avg_negative_rate
    FROM feedback_analysis
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    AND created_at < CURRENT_DATE - INTERVAL '1 day';

    -- Calculate current negative feedback rate (last 24 hours)
    SELECT
        COUNT(CASE WHEN sentiment_label IN ('negative', 'very_negative') THEN 1 END)::DECIMAL / COUNT(*)
    INTO current_negative_rate
    FROM feedback_analysis
    WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';

    -- Check for negative spike
    IF current_negative_rate > (avg_negative_rate * threshold_multiplier) THEN
        RETURN QUERY SELECT
            'negative_spike'::VARCHAR(30),
            CASE
                WHEN current_negative_rate > (avg_negative_rate * 2) THEN 'critical'::VARCHAR(10)
                ELSE 'high'::VARCHAR(10)
            END,
            format('Negative feedback rate increased to %s%% (baseline: %s%%)',
                   ROUND(current_negative_rate * 100, 1),
                   ROUND(avg_negative_rate * 100, 1)),
            '[]'::JSONB;
    END IF;

    -- Check for product-specific issues
    FOR alert_type, severity, description, affected_items IN
        SELECT
            'product_quality_decline'::VARCHAR(30),
            'high'::VARCHAR(10),
            format('Product %s has %s negative reviews in last 24h', product_id, negative_count),
            jsonb_build_array(product_id)
        FROM (
            SELECT
                product_id,
                COUNT(CASE WHEN sentiment_label IN ('negative', 'very_negative') THEN 1 END) as negative_count
            FROM feedback_analysis
            WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
            AND product_id IS NOT NULL
            GROUP BY product_id
            HAVING COUNT(CASE WHEN sentiment_label IN ('negative', 'very_negative') THEN 1 END) >= 3
        ) product_issues
    LOOP
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to update topic trends
CREATE OR REPLACE FUNCTION update_topic_trends()
RETURNS VOID AS $$
BEGIN
    -- Update trending topics based on frequency and sentiment changes
    UPDATE feedback_topics
    SET
        frequency_last_7d = (
            SELECT COUNT(*)
            FROM feedback_analysis fa,
                 jsonb_array_elements_text(fa.key_phrases) as phrase
            WHERE phrase ILIKE '%' || topic_name || '%'
            AND fa.created_at >= CURRENT_DATE - INTERVAL '7 days'
        ),
        frequency_last_30d = (
            SELECT COUNT(*)
            FROM feedback_analysis fa,
                 jsonb_array_elements_text(fa.key_phrases) as phrase
            WHERE phrase ILIKE '%' || topic_name || '%'
            AND fa.created_at >= CURRENT_DATE - INTERVAL '30 days'
        ),
        is_trending = (
            frequency_last_7d > (frequency_last_30d / 4.0) -- More than weekly average
        ),
        last_mentioned = (
            SELECT MAX(fa.created_at)
            FROM feedback_analysis fa,
                 jsonb_array_elements_text(fa.key_phrases) as phrase
            WHERE phrase ILIKE '%' || topic_name || '%'
        );
END;
$$ LANGUAGE plpgsql;