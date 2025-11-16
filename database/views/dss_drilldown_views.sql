-- DSS Drill-Down Analytics Views
-- Support Interactive Analysis: Overall → Platform → Category → Product

-- ========================================
-- 1. REVENUE ANALYSIS VIEWS
-- ========================================

-- View: Daily revenue by platform
CREATE OR REPLACE VIEW v_daily_revenue_by_platform AS
SELECT 
    d.date_sk,
    d.date_value,
    d.year,
    d.month,
    d.week_of_year,
    pl.platform_sk,
    pl.platform_code,
    pl.platform_name,
    COUNT(DISTINCT f.product_sk) as products_count,
    SUM(CASE WHEN f.is_available THEN 1 ELSE 0 END) as available_products,
    SUM(CAST(f.price_current AS DECIMAL)) as total_price_sum,
    AVG(f.price_current) as avg_price,
    SUM(f.sold_count) as total_sold,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as total_revenue,
    AVG(f.rating_avg) as avg_rating,
    SUM(f.rating_count) as total_ratings
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
GROUP BY d.date_sk, d.date_value, d.year, d.month, d.week_of_year,
         pl.platform_sk, pl.platform_code, pl.platform_name;

CREATE INDEX idx_daily_revenue_platform_date ON v_daily_revenue_by_platform(platform_code, date_value DESC);


-- View: Daily revenue by category
CREATE OR REPLACE VIEW v_daily_revenue_by_category AS
SELECT 
    d.date_sk,
    d.date_value,
    d.year,
    d.month,
    cat.category_sk,
    cat.category_code,
    cat.category_name,
    COUNT(DISTINCT f.product_sk) as products_count,
    SUM(CASE WHEN f.is_available THEN 1 ELSE 0 END) as available_products,
    SUM(CASE WHEN NOT f.is_available THEN 1 ELSE 0 END) as out_of_stock_count,
    SUM(f.sold_count) as total_sold,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as total_revenue,
    AVG(f.rating_avg) as avg_rating
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
GROUP BY d.date_sk, d.date_value, d.year, d.month,
         cat.category_sk, cat.category_code, cat.category_name;

CREATE INDEX idx_daily_revenue_category_date ON v_daily_revenue_by_category(category_code, date_value DESC);


-- View: Daily revenue by platform + category (cross dimension)
CREATE OR REPLACE VIEW v_daily_revenue_platform_category AS
SELECT 
    d.date_sk,
    d.date_value,
    d.year,
    d.month,
    pl.platform_sk,
    pl.platform_code,
    pl.platform_name,
    cat.category_sk,
    cat.category_code,
    cat.category_name,
    COUNT(DISTINCT f.product_sk) as products_count,
    SUM(CASE WHEN f.is_available THEN 1 ELSE 0 END) as available_products,
    SUM(CASE WHEN NOT f.is_available THEN 1 ELSE 0 END) as out_of_stock_count,
    SUM(f.sold_count) as total_sold,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as total_revenue,
    AVG(f.rating_avg) as avg_rating
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
GROUP BY d.date_sk, d.date_value, d.year, d.month,
         pl.platform_sk, pl.platform_code, pl.platform_name,
         cat.category_sk, cat.category_code, cat.category_name;

CREATE INDEX idx_daily_revenue_plat_cat ON v_daily_revenue_platform_category(platform_code, category_code, date_value DESC);


-- ========================================
-- 2. PRODUCT-LEVEL ANALYSIS VIEWS
-- ========================================

-- View: Product daily metrics (price, availability, sales)
CREATE OR REPLACE VIEW v_product_daily_metrics AS
SELECT 
    d.date_sk,
    d.date_value,
    p.global_product_id,
    p.product_sk,
    p.product_name,
    b.brand_sk,
    b.brand_name,
    cat.category_sk,
    cat.category_name,
    pl.platform_sk,
    pl.platform_code,
    pl.platform_name,
    f.price_current,
    f.price_original,
    f.discount_pct,
    f.is_available,
    f.sold_count,
    f.rating_avg,
    f.rating_count,
    f.review_count,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) OVER (
        PARTITION BY p.product_sk ORDER BY d.date_value
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
LEFT JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
LEFT JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk;

CREATE INDEX idx_product_daily_date ON v_product_daily_metrics(product_sk, date_value DESC);
CREATE INDEX idx_product_daily_platform ON v_product_daily_metrics(platform_code, date_value DESC);
CREATE INDEX idx_product_daily_category ON v_product_daily_metrics(category_name, date_value DESC);


-- View: Price changes detection (compare current vs previous period)
CREATE OR REPLACE VIEW v_price_changes AS
WITH daily_prices AS (
    SELECT 
        p.global_product_id,
        p.product_sk,
        p.product_name,
        b.brand_name,
        pl.platform_code,
        pl.platform_name,
        d.date_value,
        f.price_current,
        f.price_original,
        f.discount_pct,
        LAG(f.price_current, 7) OVER (
            PARTITION BY p.product_sk, pl.platform_sk 
            ORDER BY d.date_value
        ) as price_7days_ago,
        LAG(f.price_current, 30) OVER (
            PARTITION BY p.product_sk, pl.platform_sk 
            ORDER BY d.date_value
        ) as price_30days_ago
    FROM dwh_fact_product_daily f
    JOIN dwh_dim_date d ON f.date_sk = d.date_sk
    JOIN dwh_dim_product p ON f.product_sk = p.product_sk
    LEFT JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
    JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
)
SELECT 
    global_product_id,
    product_sk,
    product_name,
    brand_name,
    platform_code,
    platform_name,
    date_value,
    price_current,
    price_original,
    discount_pct,
    price_7days_ago,
    price_30days_ago,
    CASE 
        WHEN price_7days_ago IS NOT NULL THEN 
            ROUND(((price_current - price_7days_ago) / price_7days_ago * 100)::NUMERIC, 2)
        ELSE NULL
    END as price_change_7days_pct,
    CASE 
        WHEN price_30days_ago IS NOT NULL THEN 
            ROUND(((price_current - price_30days_ago) / price_30days_ago * 100)::NUMERIC, 2)
        ELSE NULL
    END as price_change_30days_pct,
    CASE 
        WHEN price_7days_ago IS NOT NULL AND price_current > price_7days_ago THEN 'INCREASED'
        WHEN price_7days_ago IS NOT NULL AND price_current < price_7days_ago THEN 'DECREASED'
        WHEN price_7days_ago IS NOT NULL THEN 'STABLE'
        ELSE 'NO_DATA'
    END as price_trend_7days
FROM daily_prices
WHERE price_7days_ago IS NOT NULL OR price_30days_ago IS NOT NULL;


-- View: Stock/Availability changes
CREATE OR REPLACE VIEW v_availability_changes AS
WITH daily_availability AS (
    SELECT 
        p.global_product_id,
        p.product_sk,
        p.product_name,
        b.brand_name,
        pl.platform_code,
        cat.category_name,
        d.date_value,
        f.is_available,
        LAG(f.is_available) OVER (
            PARTITION BY p.product_sk, pl.platform_sk 
            ORDER BY d.date_value
        ) as prev_available,
        LAG(f.is_available) OVER (
            PARTITION BY p.product_sk, pl.platform_sk 
            ORDER BY d.date_value
        ) IS DISTINCT FROM f.is_available as availability_changed
    FROM dwh_fact_product_daily f
    JOIN dwh_dim_date d ON f.date_sk = d.date_sk
    JOIN dwh_dim_product p ON f.product_sk = p.product_sk
    LEFT JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
    JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
    LEFT JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
)
SELECT 
    global_product_id,
    product_sk,
    product_name,
    brand_name,
    platform_code,
    category_name,
    date_value,
    is_available,
    prev_available,
    availability_changed,
    CASE 
        WHEN NOT is_available AND prev_available THEN 'OUT_OF_STOCK'
        WHEN is_available AND NOT prev_available THEN 'BACK_IN_STOCK'
        WHEN is_available THEN 'AVAILABLE'
        ELSE 'OUT_OF_STOCK'
    END as availability_status
FROM daily_availability
WHERE availability_changed OR NOT is_available;

CREATE INDEX idx_availability_changes_date ON v_availability_changes(platform_code, date_value DESC);


-- ========================================
-- 3. BRAND-LEVEL ANALYSIS VIEWS
-- ========================================

-- View: Daily revenue by brand
CREATE OR REPLACE VIEW v_daily_revenue_by_brand AS
SELECT 
    d.date_sk,
    d.date_value,
    d.year,
    d.month,
    b.brand_sk,
    b.brand_code,
    b.brand_name,
    COUNT(DISTINCT f.product_sk) as products_count,
    SUM(CASE WHEN f.is_available THEN 1 ELSE 0 END) as available_products,
    SUM(f.sold_count) as total_sold,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as total_revenue,
    AVG(f.rating_avg) as avg_rating
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY d.date_sk, d.date_value, d.year, d.month,
         b.brand_sk, b.brand_code, b.brand_name;

CREATE INDEX idx_daily_revenue_brand ON v_daily_revenue_by_brand(brand_name, date_value DESC);


-- View: Brand performance by platform
CREATE OR REPLACE VIEW v_brand_platform_performance AS
SELECT 
    d.date_sk,
    d.date_value,
    d.year,
    d.month,
    pl.platform_code,
    pl.platform_name,
    b.brand_sk,
    b.brand_name,
    COUNT(DISTINCT f.product_sk) as products_count,
    SUM(CASE WHEN f.is_available THEN 1 ELSE 0 END) as available_products,
    SUM(f.sold_count) as total_sold,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as total_revenue,
    AVG(f.price_current) as avg_price,
    AVG(f.rating_avg) as avg_rating
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY d.date_sk, d.date_value, d.year, d.month,
         pl.platform_code, pl.platform_name,
         b.brand_sk, b.brand_name;

CREATE INDEX idx_brand_platform_perf ON v_brand_platform_performance(platform_code, brand_name, date_value DESC);


-- ========================================
-- 4. ALERT & ANOMALY VIEWS
-- ========================================

-- View: Products with price increases >10%
CREATE OR REPLACE VIEW v_alert_price_increase AS
SELECT 
    pc.global_product_id,
    pc.product_name,
    pc.brand_name,
    pc.platform_code,
    pc.platform_name,
    pc.date_value,
    pc.price_current,
    pc.price_7days_ago,
    pc.price_30days_ago,
    pc.price_change_7days_pct,
    CASE 
        WHEN pc.price_change_7days_pct >= 10 THEN 'SIGNIFICANT_INCREASE'
        WHEN pc.price_change_7days_pct >= 5 THEN 'MODERATE_INCREASE'
        ELSE 'MINOR_INCREASE'
    END as alert_level,
    'Price Increase' as alert_type
FROM v_price_changes pc
WHERE pc.price_change_7days_pct > 0
  AND pc.price_current > pc.price_7days_ago
ORDER BY pc.price_change_7days_pct DESC;

CREATE INDEX idx_alert_price_increase ON v_alert_price_increase(platform_code, date_value DESC);


-- View: Products out of stock
CREATE OR REPLACE VIEW v_alert_out_of_stock AS
SELECT 
    ac.global_product_id,
    ac.product_name,
    ac.brand_name,
    ac.platform_code,
    ac.category_name,
    ac.date_value,
    ac.availability_status,
    'Stock Alert' as alert_type,
    'OUT_OF_STOCK' as alert_level,
    ROW_NUMBER() OVER (
        PARTITION BY ac.global_product_id, ac.platform_code 
        ORDER BY ac.date_value DESC
    ) as days_out_of_stock
FROM v_availability_changes ac
WHERE ac.is_available = FALSE
ORDER BY ac.date_value DESC;

CREATE INDEX idx_alert_oos ON v_alert_out_of_stock(platform_code, category_name, date_value DESC);


-- View: Categories with revenue decline >10%
CREATE OR REPLACE VIEW v_alert_category_decline AS
WITH category_comparison AS (
    SELECT 
        cat.category_code,
        cat.category_name,
        pl.platform_code,
        pl.platform_name,
        EXTRACT(YEAR FROM d.date_value) as year,
        EXTRACT(MONTH FROM d.date_value) as month,
        SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as monthly_revenue,
        LAG(SUM(CAST(f.price_current * f.sold_count AS DECIMAL))) OVER (
            PARTITION BY cat.category_code, pl.platform_code 
            ORDER BY EXTRACT(YEAR FROM d.date_value), EXTRACT(MONTH FROM d.date_value)
        ) as prev_month_revenue
    FROM dwh_fact_product_daily f
    JOIN dwh_dim_date d ON f.date_sk = d.date_sk
    JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
    JOIN dwh_dim_product p ON f.product_sk = p.product_sk
    JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
    GROUP BY cat.category_code, cat.category_name, pl.platform_code, pl.platform_name,
             EXTRACT(YEAR FROM d.date_value), EXTRACT(MONTH FROM d.date_value)
)
SELECT 
    category_code,
    category_name,
    platform_code,
    platform_name,
    year,
    month,
    monthly_revenue,
    prev_month_revenue,
    ROUND(((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100)::NUMERIC, 2) as revenue_change_pct,
    CASE 
        WHEN ((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100) < -20 THEN 'CRITICAL'
        WHEN ((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100) < -10 THEN 'WARNING'
        ELSE 'WATCH'
    END as alert_level,
    'Category Decline' as alert_type
FROM category_comparison
WHERE prev_month_revenue IS NOT NULL 
  AND monthly_revenue < prev_month_revenue
ORDER BY revenue_change_pct ASC;

CREATE INDEX idx_alert_category_decline ON v_alert_category_decline(platform_code, category_name, year DESC, month DESC);


-- ========================================
-- 5. SUMMARY VIEWS FOR DASHBOARDS
-- ========================================

-- View: Monthly revenue summary by platform
CREATE OR REPLACE VIEW v_monthly_revenue_platform AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    pl.platform_code,
    pl.platform_name,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as monthly_revenue,
    COUNT(DISTINCT f.product_sk) as products_count,
    SUM(f.sold_count) as total_sold,
    AVG(f.rating_avg) as avg_rating
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
GROUP BY d.year, d.month, d.month_name, pl.platform_code, pl.platform_name
ORDER BY d.year DESC, d.month DESC;


-- View: Top 100 products by revenue
CREATE OR REPLACE VIEW v_top_products_revenue AS
SELECT 
    p.global_product_id,
    p.product_name,
    b.brand_name,
    cat.category_name,
    pl.platform_name,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as total_revenue,
    SUM(f.sold_count) as total_sold,
    AVG(f.price_current) as avg_price,
    AVG(f.rating_avg) as avg_rating,
    COUNT(DISTINCT f.date_sk) as days_tracked
FROM dwh_fact_product_daily f
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
LEFT JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
LEFT JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
GROUP BY p.global_product_id, p.product_name, b.brand_name, cat.category_name, pl.platform_name
ORDER BY total_revenue DESC
LIMIT 100;


-- ========================================
-- GRANT PERMISSIONS
-- ========================================

GRANT SELECT ON v_daily_revenue_by_platform TO dss_user;
GRANT SELECT ON v_daily_revenue_by_category TO dss_user;
GRANT SELECT ON v_daily_revenue_platform_category TO dss_user;
GRANT SELECT ON v_product_daily_metrics TO dss_user;
GRANT SELECT ON v_price_changes TO dss_user;
GRANT SELECT ON v_availability_changes TO dss_user;
GRANT SELECT ON v_daily_revenue_by_brand TO dss_user;
GRANT SELECT ON v_brand_platform_performance TO dss_user;
GRANT SELECT ON v_alert_price_increase TO dss_user;
GRANT SELECT ON v_alert_out_of_stock TO dss_user;
GRANT SELECT ON v_alert_category_decline TO dss_user;
GRANT SELECT ON v_monthly_revenue_platform TO dss_user;
GRANT SELECT ON v_top_products_revenue TO dss_user;
