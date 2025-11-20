-- =====================================================
-- Review Details Table Queries
-- =====================================================

-- 1. Check if table exists and count records
SELECT 
    EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'dwh' 
        AND table_name = 'fact_reviews_detail'
    ) as table_exists,
    (SELECT COUNT(*) FROM dwh.fact_reviews_detail) as total_records;

-- 2. View sample reviews with text content
SELECT 
    review_id,
    global_product_id,
    source_platform_std,
    reviewer_name,
    rating,
    review_text,
    review_date,
    sentiment_score,
    sentiment_label,
    helpful_count
FROM dwh.fact_reviews_detail
ORDER BY created_at DESC
LIMIT 10;

-- 3. Reviews by sentiment
SELECT 
    sentiment_label,
    COUNT(*) as count,
    AVG(rating) as avg_rating,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM dwh.fact_reviews_detail
GROUP BY sentiment_label
ORDER BY count DESC;

-- 4. Top reviewed products by review count
SELECT 
    global_product_id,
    source_platform_std,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    AVG(sentiment_score) as avg_sentiment
FROM dwh.fact_reviews_detail
GROUP BY global_product_id, source_platform_std
ORDER BY review_count DESC
LIMIT 10;

-- 5. Top reviewers
SELECT 
    reviewer_name,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    COUNT(DISTINCT global_product_id) as products_reviewed
FROM dwh.fact_reviews_detail
GROUP BY reviewer_name
HAVING COUNT(*) > 1
ORDER BY review_count DESC
LIMIT 10;

-- 6. Reviews with specific keywords
SELECT 
    review_id,
    reviewer_name,
    rating,
    review_text,
    sentiment_label
FROM dwh.fact_reviews_detail
WHERE LOWER(review_text) LIKE '%quality%'
   OR LOWER(review_text) LIKE '%price%'
   OR LOWER(review_text) LIKE '%delivery%'
LIMIT 10;

-- 7. Negative reviews (for analysis)
SELECT 
    review_id,
    global_product_id,
    reviewer_name,
    rating,
    review_text,
    sentiment_score,
    helpful_count
FROM dwh.fact_reviews_detail
WHERE sentiment_label = 'negative'
ORDER BY sentiment_score ASC
LIMIT 20;

-- 8. Reviews by platform
SELECT 
    source_platform_std,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    AVG(sentiment_score) as avg_sentiment,
    COUNT(DISTINCT global_product_id) as products,
    COUNT(DISTINCT reviewer_name) as reviewers
FROM dwh.fact_reviews_detail
GROUP BY source_platform_std
ORDER BY review_count DESC;

-- 9. Reviews trend over time
SELECT 
    DATE_TRUNC('month', review_date)::DATE as month,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    AVG(sentiment_score) as avg_sentiment
FROM dwh.fact_reviews_detail
WHERE review_date IS NOT NULL
GROUP BY DATE_TRUNC('month', review_date)
ORDER BY month DESC;

-- 10. Compare fact_review_daily_agg with fact_reviews_detail
SELECT 
    'fact_review_daily_agg' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT global_product_id) as distinct_products
FROM dwh.fact_review_daily_agg
UNION ALL
SELECT 
    'fact_reviews_detail' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT global_product_id) as distinct_products
FROM dwh.fact_reviews_detail;
