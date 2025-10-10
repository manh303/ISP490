-- ===================================
-- BEST SELLER TRACKING SCHEMA
-- ===================================
-- Database schema for tracking and analyzing best-selling products

-- Create bestseller rankings table for historical tracking
CREATE TABLE IF NOT EXISTS bestseller_rankings (
    ranking_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id VARCHAR(36) NOT NULL,
    category VARCHAR(50),
    rank_position INTEGER NOT NULL,
    sales_volume INTEGER NOT NULL DEFAULT 0,
    revenue DECIMAL(12,2) NOT NULL DEFAULT 0,
    timeframe VARCHAR(20) NOT NULL CHECK (timeframe IN ('hour', 'day', 'week', 'month', 'quarter', 'year')),
    date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Unique constraint for product rankings per timeframe/date
    UNIQUE(product_id, timeframe, date),

    -- Indexes
    INDEX idx_bestseller_rankings_product (product_id),
    INDEX idx_bestseller_rankings_category (category),
    INDEX idx_bestseller_rankings_timeframe (timeframe),
    INDEX idx_bestseller_rankings_date (date),
    INDEX idx_bestseller_rankings_rank (rank_position),
    INDEX idx_bestseller_rankings_sales (sales_volume)
);

-- Create trending products table for velocity tracking
CREATE TABLE IF NOT EXISTS trending_products (
    trending_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id VARCHAR(36) NOT NULL,
    velocity_score DECIMAL(8,2) NOT NULL DEFAULT 0,
    growth_rate DECIMAL(5,2) NOT NULL DEFAULT 0,
    confidence_score DECIMAL(3,2) NOT NULL DEFAULT 0,
    trend_start_date TIMESTAMP NOT NULL,
    trend_peak_date TIMESTAMP,
    trend_end_date TIMESTAMP,
    peak_sales_volume INTEGER DEFAULT 0,
    total_trend_revenue DECIMAL(12,2) DEFAULT 0,
    trend_status VARCHAR(20) DEFAULT 'active' CHECK (trend_status IN ('active', 'peaked', 'declining', 'ended')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    -- Indexes
    INDEX idx_trending_products_product (product_id),
    INDEX idx_trending_products_velocity (velocity_score),
    INDEX idx_trending_products_growth (growth_rate),
    INDEX idx_trending_products_status (trend_status),
    INDEX idx_trending_products_start_date (trend_start_date)
);

-- Create category performance metrics table
CREATE TABLE IF NOT EXISTS category_performance_daily (
    performance_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    category VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    total_products INTEGER DEFAULT 0,
    active_products INTEGER DEFAULT 0,
    total_sales INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0,
    unique_customers INTEGER DEFAULT 0,
    avg_order_value DECIMAL(10,2) DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0,
    market_share DECIMAL(5,2) DEFAULT 0,
    growth_rate DECIMAL(5,2) DEFAULT 0,
    top_product_id VARCHAR(36),
    calculated_at TIMESTAMP DEFAULT NOW(),

    -- Unique constraint for daily category metrics
    UNIQUE(category, date),

    -- Indexes
    INDEX idx_category_performance_category (category),
    INDEX idx_category_performance_date (date),
    INDEX idx_category_performance_revenue (total_revenue),
    INDEX idx_category_performance_growth (growth_rate)
);

-- Create product velocity tracking table
CREATE TABLE IF NOT EXISTS product_velocity_tracking (
    velocity_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id VARCHAR(36) NOT NULL,
    tracking_date DATE NOT NULL,
    hour_of_day INTEGER CHECK (hour_of_day BETWEEN 0 AND 23),
    sales_volume INTEGER DEFAULT 0,
    revenue DECIMAL(10,2) DEFAULT 0,
    views INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    velocity_score DECIMAL(8,2) DEFAULT 0,
    momentum_direction VARCHAR(10) CHECK (momentum_direction IN ('up', 'down', 'stable')),
    created_at TIMESTAMP DEFAULT NOW(),

    -- Unique constraint for hourly tracking
    UNIQUE(product_id, tracking_date, hour_of_day),

    -- Indexes
    INDEX idx_velocity_tracking_product (product_id),
    INDEX idx_velocity_tracking_date (tracking_date),
    INDEX idx_velocity_tracking_hour (hour_of_day),
    INDEX idx_velocity_tracking_velocity (velocity_score)
);

-- Create real-time metrics cache table
CREATE TABLE IF NOT EXISTS real_time_metrics_cache (
    metric_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    metric_type VARCHAR(30) NOT NULL,
    metric_value JSONB NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Index for cleanup
    INDEX idx_real_time_metrics_expires (expires_at),
    INDEX idx_real_time_metrics_type (metric_type)
);

-- Create product alerts table
CREATE TABLE IF NOT EXISTS product_alerts (
    alert_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id VARCHAR(36) NOT NULL,
    alert_type VARCHAR(30) NOT NULL CHECK (alert_type IN (
        'stock_low', 'stock_out', 'trending_up', 'trending_down',
        'velocity_spike', 'conversion_drop', 'ranking_change'
    )),
    severity VARCHAR(10) NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    title VARCHAR(200) NOT NULL,
    description TEXT,
    threshold_value DECIMAL(10,2),
    actual_value DECIMAL(10,2),
    alert_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'acknowledged', 'resolved', 'dismissed')),
    acknowledged_by VARCHAR(36),
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Indexes
    INDEX idx_product_alerts_product (product_id),
    INDEX idx_product_alerts_type (alert_type),
    INDEX idx_product_alerts_severity (severity),
    INDEX idx_product_alerts_status (status),
    INDEX idx_product_alerts_created (created_at)
);

-- Create sales prediction table
CREATE TABLE IF NOT EXISTS sales_predictions (
    prediction_id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id VARCHAR(36) NOT NULL,
    prediction_date DATE NOT NULL,
    prediction_horizon INTEGER NOT NULL, -- Days ahead
    predicted_sales INTEGER NOT NULL,
    predicted_revenue DECIMAL(12,2) NOT NULL,
    confidence_interval_lower INTEGER,
    confidence_interval_upper INTEGER,
    model_used VARCHAR(50),
    model_accuracy DECIMAL(5,2),
    input_features JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    actual_sales INTEGER, -- Filled in later for accuracy tracking
    actual_revenue DECIMAL(12,2),

    -- Indexes
    INDEX idx_sales_predictions_product (product_id),
    INDEX idx_sales_predictions_date (prediction_date),
    INDEX idx_sales_predictions_horizon (prediction_horizon),
    INDEX idx_sales_predictions_model (model_used)
);

-- ===================================
-- SAMPLE DATA
-- ===================================

-- Insert sample bestseller rankings
INSERT INTO bestseller_rankings (
    product_id, category, rank_position, sales_volume, revenue, timeframe, date
) VALUES
('product-001', 'Electronics', 1, 150, 45000000, 'day', CURRENT_DATE),
('product-002', 'Electronics', 2, 132, 39600000, 'day', CURRENT_DATE),
('product-003', 'Fashion', 3, 128, 25600000, 'day', CURRENT_DATE),
('product-004', 'Home & Garden', 4, 95, 28500000, 'day', CURRENT_DATE),
('product-005', 'Electronics', 5, 87, 26100000, 'day', CURRENT_DATE),
('product-006', 'Fashion', 6, 82, 16400000, 'day', CURRENT_DATE),
('product-007', 'Books', 7, 78, 11700000, 'day', CURRENT_DATE),
('product-008', 'Sports', 8, 75, 22500000, 'day', CURRENT_DATE),
('product-009', 'Beauty', 9, 71, 14200000, 'day', CURRENT_DATE),
('product-010', 'Electronics', 10, 68, 20400000, 'day', CURRENT_DATE);

-- Insert previous day rankings for comparison
INSERT INTO bestseller_rankings (
    product_id, category, rank_position, sales_volume, revenue, timeframe, date
) VALUES
('product-002', 'Electronics', 1, 145, 43500000, 'day', CURRENT_DATE - 1),
('product-001', 'Electronics', 2, 140, 42000000, 'day', CURRENT_DATE - 1),
('product-004', 'Home & Garden', 3, 125, 37500000, 'day', CURRENT_DATE - 1),
('product-003', 'Fashion', 4, 115, 23000000, 'day', CURRENT_DATE - 1),
('product-005', 'Electronics', 5, 85, 25500000, 'day', CURRENT_DATE - 1);

-- Insert sample trending products
INSERT INTO trending_products (
    product_id, velocity_score, growth_rate, confidence_score, trend_start_date,
    peak_sales_volume, total_trend_revenue, trend_status
) VALUES
('product-011', 125.5, 245.8, 0.87, NOW() - INTERVAL '3 days', 95, 19000000, 'active'),
('product-012', 98.2, 189.3, 0.72, NOW() - INTERVAL '2 days', 67, 13400000, 'active'),
('product-013', 87.9, 156.7, 0.65, NOW() - INTERVAL '5 days', 120, 24000000, 'peaked'),
('product-014', 76.3, 134.2, 0.58, NOW() - INTERVAL '4 days', 89, 17800000, 'active'),
('product-015', 45.1, 98.4, 0.43, NOW() - INTERVAL '6 days', 156, 31200000, 'declining');

-- Insert sample category performance
INSERT INTO category_performance_daily (
    category, date, total_products, active_products, total_sales, total_revenue,
    unique_customers, avg_order_value, conversion_rate, market_share, growth_rate, top_product_id
) VALUES
('Electronics', CURRENT_DATE, 250, 89, 1250, 125000000, 456, 274193, 3.2, 35.5, 12.8, 'product-001'),
('Fashion', CURRENT_DATE, 180, 67, 890, 67200000, 312, 214423, 2.8, 19.1, 8.4, 'product-003'),
('Home & Garden', CURRENT_DATE, 95, 34, 456, 45600000, 198, 230303, 2.1, 13.0, 15.6, 'product-004'),
('Books', CURRENT_DATE, 320, 45, 234, 11700000, 156, 75000, 1.8, 3.3, -2.1, 'product-007'),
('Sports', CURRENT_DATE, 78, 28, 298, 29800000, 123, 242276, 2.9, 8.5, 22.3, 'product-008'),
('Beauty', CURRENT_DATE, 156, 52, 387, 38700000, 245, 158367, 2.4, 11.0, 6.7, 'product-009');

-- Insert sample velocity tracking (last 24 hours)
INSERT INTO product_velocity_tracking (
    product_id, tracking_date, hour_of_day, sales_volume, revenue, views, conversions, velocity_score, momentum_direction
) VALUES
-- Product 001 - Strong morning performance
('product-001', CURRENT_DATE, 8, 12, 3600000, 245, 12, 8.5, 'up'),
('product-001', CURRENT_DATE, 9, 18, 5400000, 312, 18, 12.8, 'up'),
('product-001', CURRENT_DATE, 10, 22, 6600000, 398, 22, 15.2, 'up'),
('product-001', CURRENT_DATE, 11, 15, 4500000, 289, 15, 9.8, 'stable'),
('product-001', CURRENT_DATE, 12, 8, 2400000, 156, 8, 5.2, 'down'),

-- Product 011 - Trending pattern
('product-011', CURRENT_DATE, 14, 25, 5000000, 456, 25, 18.7, 'up'),
('product-011', CURRENT_DATE, 15, 32, 6400000, 578, 32, 24.1, 'up'),
('product-011', CURRENT_DATE, 16, 28, 5600000, 523, 28, 20.9, 'stable'),
('product-011', CURRENT_DATE, 17, 19, 3800000, 387, 19, 14.2, 'down');

-- Insert sample alerts
INSERT INTO product_alerts (
    product_id, alert_type, severity, title, description, threshold_value, actual_value, alert_data
) VALUES
('product-001', 'stock_low', 'high', 'Sản phẩm bán chạy sắp hết hàng',
 'Sản phẩm #1 chỉ còn 15 sản phẩm trong kho', 20, 15,
 '{"current_rank": 1, "daily_sales": 150, "estimated_days": 0.1}'::jsonb),

('product-011', 'trending_up', 'medium', 'Sản phẩm đang viral',
 'Sản phẩm này tăng trưởng 245% trong 3 ngày qua', 100, 245.8,
 '{"velocity_score": 125.5, "confidence": 0.87}'::jsonb),

('product-013', 'trending_down', 'low', 'Xu hướng sản phẩm đang giảm',
 'Sản phẩm từng trending giờ đang giảm momentum', -10, -15.2,
 '{"peak_date": "2025-01-05", "current_velocity": 45.1}'::jsonb),

('product-004', 'velocity_spike', 'medium', 'Tăng đột biến trong giờ cao điểm',
 'Sales tăng 300% trong giờ vàng', 50, 150,
 '{"hour": 15, "normal_sales": 5, "spike_sales": 15}'::jsonb);

-- Insert sample predictions
INSERT INTO sales_predictions (
    product_id, prediction_date, prediction_horizon, predicted_sales, predicted_revenue,
    confidence_interval_lower, confidence_interval_upper, model_used, model_accuracy
) VALUES
('product-001', CURRENT_DATE + 1, 1, 145, 43500000, 130, 160, 'ARIMA', 0.85),
('product-001', CURRENT_DATE + 7, 7, 950, 285000000, 850, 1050, 'ARIMA', 0.78),
('product-002', CURRENT_DATE + 1, 1, 128, 38400000, 115, 140, 'Linear_Regression', 0.82),
('product-003', CURRENT_DATE + 1, 1, 135, 27000000, 120, 150, 'Random_Forest', 0.88),
('product-011', CURRENT_DATE + 1, 1, 105, 21000000, 85, 125, 'Trending_Model', 0.65);

-- ===================================
-- VIEWS
-- ===================================

-- View for current bestseller dashboard
CREATE OR REPLACE VIEW current_bestsellers_dashboard AS
SELECT
    br.rank_position,
    br.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.stock_quantity,
    br.sales_volume,
    br.revenue,
    br.date as ranking_date,
    -- Previous rank comparison
    prev_br.rank_position as previous_rank,
    COALESCE(br.rank_position - prev_br.rank_position, 0) as rank_change,
    -- Growth calculations
    CASE
        WHEN COALESCE(prev_br.sales_volume, 0) > 0 THEN
            ROUND(((br.sales_volume - prev_br.sales_volume)::DECIMAL / prev_br.sales_volume * 100), 2)
        ELSE 100.0
    END as growth_percentage,
    -- Stock status
    CASE
        WHEN p.stock_quantity <= 10 THEN 'critical'
        WHEN p.stock_quantity <= 50 THEN 'low'
        WHEN p.stock_quantity <= 100 THEN 'medium'
        ELSE 'good'
    END as stock_status,
    -- Estimated days to stockout
    CASE
        WHEN br.sales_volume > 0 THEN
            ROUND(p.stock_quantity::DECIMAL / br.sales_volume, 1)
        ELSE NULL
    END as days_to_stockout
FROM bestseller_rankings br
JOIN products p ON br.product_id = p.product_id
LEFT JOIN bestseller_rankings prev_br ON br.product_id = prev_br.product_id
    AND prev_br.date = br.date - INTERVAL '1 day'
    AND prev_br.timeframe = br.timeframe
WHERE br.date = CURRENT_DATE
AND br.timeframe = 'day'
ORDER BY br.rank_position;

-- View for trending products analysis
CREATE OR REPLACE VIEW trending_products_analysis AS
SELECT
    tp.product_id,
    p.product_name,
    p.category,
    p.price,
    tp.velocity_score,
    tp.growth_rate,
    tp.confidence_score,
    tp.trend_start_date,
    DATE_PART('day', NOW() - tp.trend_start_date) as trend_duration_days,
    tp.peak_sales_volume,
    tp.total_trend_revenue,
    tp.trend_status,
    -- Current performance
    recent_sales.daily_sales,
    recent_sales.daily_revenue,
    -- Momentum indicator
    CASE
        WHEN tp.velocity_score > 100 AND tp.confidence_score > 0.7 THEN 'explosive'
        WHEN tp.velocity_score > 50 AND tp.confidence_score > 0.5 THEN 'strong'
        WHEN tp.velocity_score > 20 THEN 'moderate'
        ELSE 'weak'
    END as momentum_strength
FROM trending_products tp
JOIN products p ON tp.product_id = p.product_id
LEFT JOIN (
    SELECT
        br.product_id,
        br.sales_volume as daily_sales,
        br.revenue as daily_revenue
    FROM bestseller_rankings br
    WHERE br.date = CURRENT_DATE AND br.timeframe = 'day'
) recent_sales ON tp.product_id = recent_sales.product_id
WHERE tp.trend_status = 'active'
ORDER BY tp.velocity_score DESC;

-- View for category performance comparison
CREATE OR REPLACE VIEW category_performance_comparison AS
SELECT
    cpd.category,
    cpd.date,
    cpd.total_sales,
    cpd.total_revenue,
    cpd.conversion_rate,
    cpd.market_share,
    cpd.growth_rate,
    -- Previous day comparison
    prev_cpd.total_sales as prev_total_sales,
    prev_cpd.total_revenue as prev_total_revenue,
    -- Performance indicators
    CASE
        WHEN cpd.growth_rate > 20 THEN 'excellent'
        WHEN cpd.growth_rate > 10 THEN 'good'
        WHEN cpd.growth_rate > 0 THEN 'positive'
        WHEN cpd.growth_rate > -10 THEN 'declining'
        ELSE 'poor'
    END as performance_rating,
    -- Market position
    RANK() OVER (ORDER BY cpd.total_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY cpd.growth_rate DESC) as growth_rank,
    RANK() OVER (ORDER BY cpd.conversion_rate DESC) as conversion_rank
FROM category_performance_daily cpd
LEFT JOIN category_performance_daily prev_cpd ON cpd.category = prev_cpd.category
    AND prev_cpd.date = cpd.date - INTERVAL '1 day'
WHERE cpd.date = CURRENT_DATE
ORDER BY cpd.total_revenue DESC;

-- ===================================
-- FUNCTIONS
-- ===================================

-- Function to calculate velocity score
CREATE OR REPLACE FUNCTION calculate_velocity_score(
    p_product_id VARCHAR(36),
    p_hours_back INTEGER DEFAULT 24
)
RETURNS DECIMAL(8,2) AS $$
DECLARE
    velocity_score DECIMAL(8,2) := 0;
    total_sales INTEGER := 0;
    total_views INTEGER := 0;
    time_factor DECIMAL(3,2);
BEGIN
    -- Get sales and views data
    SELECT
        COALESCE(SUM(sales_volume), 0),
        COALESCE(SUM(views), 0)
    INTO total_sales, total_views
    FROM product_velocity_tracking
    WHERE product_id = p_product_id
    AND tracking_date >= CURRENT_DATE - INTERVAL '1 day' * (p_hours_back / 24.0);

    -- Calculate base velocity (sales per hour)
    IF p_hours_back > 0 THEN
        velocity_score := total_sales::DECIMAL / p_hours_back;
    END IF;

    -- Apply conversion factor if we have views
    IF total_views > 0 THEN
        velocity_score := velocity_score * (total_sales::DECIMAL / total_views) * 100;
    END IF;

    -- Apply time decay factor (recent activity weighs more)
    time_factor := 1.0 + (24.0 - p_hours_back) / 24.0 * 0.5;
    velocity_score := velocity_score * time_factor;

    -- Apply logarithmic scaling for large numbers
    IF velocity_score > 10 THEN
        velocity_score := 10 + LOG(velocity_score - 9);
    END IF;

    RETURN ROUND(velocity_score, 2);
END;
$$ LANGUAGE plpgsql;

-- Function to detect trending products
CREATE OR REPLACE FUNCTION detect_trending_products()
RETURNS TABLE (
    product_id VARCHAR(36),
    velocity_score DECIMAL(8,2),
    confidence_score DECIMAL(3,2),
    trend_strength VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    WITH recent_performance AS (
        SELECT
            br.product_id,
            br.sales_volume as current_sales,
            COALESCE(prev_br.sales_volume, 0) as previous_sales,
            br.revenue as current_revenue
        FROM bestseller_rankings br
        LEFT JOIN bestseller_rankings prev_br ON br.product_id = prev_br.product_id
            AND prev_br.date = br.date - INTERVAL '1 day'
            AND prev_br.timeframe = br.timeframe
        WHERE br.date = CURRENT_DATE AND br.timeframe = 'day'
    ),
    velocity_calc AS (
        SELECT
            rp.product_id,
            calculate_velocity_score(rp.product_id, 24) as velocity,
            CASE
                WHEN rp.previous_sales > 0 THEN
                    (rp.current_sales - rp.previous_sales)::DECIMAL / rp.previous_sales
                ELSE 1.0
            END as growth_factor,
            rp.current_sales,
            rp.current_revenue
        FROM recent_performance rp
        WHERE rp.current_sales >= 5  -- Minimum threshold
    )
    SELECT
        vc.product_id,
        vc.velocity,
        -- Confidence based on sales volume and consistency
        LEAST(
            (vc.current_sales / 20.0)::DECIMAL(3,2),
            1.0
        ) * LEAST(
            (vc.growth_factor + 0.5)::DECIMAL(3,2),
            1.0
        ) as confidence,
        CASE
            WHEN vc.velocity > 50 AND vc.growth_factor > 1.5 THEN 'explosive'
            WHEN vc.velocity > 25 AND vc.growth_factor > 1.0 THEN 'strong'
            WHEN vc.velocity > 10 THEN 'moderate'
            ELSE 'weak'
        END::VARCHAR(20)
    FROM velocity_calc vc
    WHERE vc.velocity > 5  -- Only return products with meaningful velocity
    ORDER BY vc.velocity DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to update daily bestseller rankings
CREATE OR REPLACE FUNCTION update_daily_bestseller_rankings()
RETURNS INTEGER AS $$
DECLARE
    products_updated INTEGER := 0;
BEGIN
    -- Clear existing rankings for today
    DELETE FROM bestseller_rankings
    WHERE date = CURRENT_DATE AND timeframe = 'day';

    -- Insert new rankings based on today's sales
    INSERT INTO bestseller_rankings (
        product_id, category, rank_position, sales_volume, revenue, timeframe, date
    )
    SELECT
        p.product_id,
        p.category,
        ROW_NUMBER() OVER (ORDER BY SUM(oi.quantity) DESC) as rank_position,
        SUM(oi.quantity) as sales_volume,
        SUM(oi.quantity * oi.unit_price) as revenue,
        'day',
        CURRENT_DATE
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE DATE(o.order_date) = CURRENT_DATE
    GROUP BY p.product_id, p.category
    ORDER BY sales_volume DESC
    LIMIT 100;

    GET DIAGNOSTICS products_updated = ROW_COUNT;
    RETURN products_updated;
END;
$$ LANGUAGE plpgsql;

-- Function to generate stock alerts
CREATE OR REPLACE FUNCTION generate_stock_alerts()
RETURNS INTEGER AS $$
DECLARE
    alerts_created INTEGER := 0;
    product_record RECORD;
BEGIN
    -- Clear existing stock alerts
    UPDATE product_alerts
    SET status = 'resolved'
    WHERE alert_type IN ('stock_low', 'stock_out')
    AND status = 'active';

    -- Generate new stock alerts for bestsellers
    FOR product_record IN
        SELECT
            cb.product_id,
            cb.product_name,
            cb.stock_quantity,
            cb.sales_volume,
            cb.days_to_stockout,
            cb.rank_position
        FROM current_bestsellers_dashboard cb
        WHERE cb.stock_quantity <= 50 OR cb.days_to_stockout <= 7
    LOOP
        INSERT INTO product_alerts (
            product_id, alert_type, severity, title, description,
            threshold_value, actual_value, alert_data
        ) VALUES (
            product_record.product_id,
            CASE
                WHEN product_record.stock_quantity <= 0 THEN 'stock_out'
                ELSE 'stock_low'
            END,
            CASE
                WHEN product_record.days_to_stockout <= 1 THEN 'critical'
                WHEN product_record.days_to_stockout <= 3 THEN 'high'
                ELSE 'medium'
            END,
            CASE
                WHEN product_record.stock_quantity <= 0 THEN 'Sản phẩm bán chạy đã hết hàng'
                ELSE 'Sản phẩm bán chạy sắp hết hàng'
            END,
            format('Sản phẩm #%s (%s) chỉ còn %s sản phẩm, ước tính %s ngày hết hàng',
                   product_record.rank_position,
                   product_record.product_name,
                   product_record.stock_quantity,
                   COALESCE(product_record.days_to_stockout::TEXT, 'N/A')),
            CASE
                WHEN product_record.stock_quantity <= 0 THEN 1
                ELSE 20
            END,
            product_record.stock_quantity,
            json_build_object(
                'rank_position', product_record.rank_position,
                'daily_sales', product_record.sales_volume,
                'days_to_stockout', product_record.days_to_stockout
            )::jsonb
        );

        alerts_created := alerts_created + 1;
    END LOOP;

    RETURN alerts_created;
END;
$$ LANGUAGE plpgsql;