-- Seed ML Predictions with sample data

-- Insert sample product recommendations
-- Assuming product_sk values exist in your data warehouse
INSERT INTO ml_product_recommendations (
    product_sk,
    recommended_product_sk,
    similarity_score,
    recommendation_type,
    created_at
) VALUES
-- Product 1 recommendations
(1, 2, 0.9200, 'content_based', NOW()),
(1, 3, 0.8850, 'content_based', NOW()),
(1, 5, 0.8650, 'collaborative', NOW()),
(1, 7, 0.8200, 'hybrid', NOW()),
(1, 10, 0.7950, 'collaborative', NOW()),
-- Product 2 recommendations
(2, 1, 0.9200, 'content_based', NOW()),
(2, 4, 0.8750, 'content_based', NOW()),
(2, 6, 0.8450, 'collaborative', NOW()),
(2, 8, 0.8100, 'hybrid', NOW()),
(2, 11, 0.7850, 'collaborative', NOW()),
-- Product 3 recommendations
(3, 1, 0.8850, 'content_based', NOW()),
(3, 5, 0.8650, 'collaborative', NOW()),
(3, 9, 0.8300, 'content_based', NOW()),
(3, 12, 0.8050, 'hybrid', NOW()),
(3, 15, 0.7650, 'collaborative', NOW()),
-- Product 4 recommendations
(4, 2, 0.8750, 'content_based', NOW()),
(4, 6, 0.8450, 'collaborative', NOW()),
(4, 10, 0.8150, 'content_based', NOW()),
(4, 14, 0.7900, 'hybrid', NOW()),
(4, 18, 0.7550, 'collaborative', NOW()),
-- Product 5 recommendations
(5, 1, 0.8650, 'collaborative', NOW()),
(5, 3, 0.8650, 'content_based', NOW()),
(5, 7, 0.8300, 'hybrid', NOW()),
(5, 11, 0.8000, 'content_based', NOW()),
(5, 16, 0.7600, 'collaborative', NOW())
ON CONFLICT DO NOTHING;

-- Insert sample price predictions
INSERT INTO ml_price_predictions (
    product_sk,
    platform_sk,
    prediction_date,
    predicted_price,
    confidence_interval_lower,
    confidence_interval_upper,
    model_version,
    created_at
) VALUES
-- Product 1 price predictions (Tiki & Lazada)
(1, 1, CURRENT_DATE + INTERVAL '1 day', 250000.00, 245000.00, 255000.00, '1.0.0', NOW()),
(1, 1, CURRENT_DATE + INTERVAL '2 days', 252500.00, 247000.00, 258000.00, '1.0.0', NOW()),
(1, 1, CURRENT_DATE + INTERVAL '3 days', 255000.00, 249000.00, 261000.00, '1.0.0', NOW()),
(1, 2, CURRENT_DATE + INTERVAL '1 day', 248000.00, 243000.00, 253000.00, '1.0.0', NOW()),
(1, 2, CURRENT_DATE + INTERVAL '2 days', 250500.00, 245000.00, 256000.00, '1.0.0', NOW()),
(1, 2, CURRENT_DATE + INTERVAL '3 days', 253000.00, 247000.00, 259000.00, '1.0.0', NOW()),
-- Product 2 price predictions
(2, 1, CURRENT_DATE + INTERVAL '1 day', 350000.00, 342500.00, 357500.00, '1.0.0', NOW()),
(2, 1, CURRENT_DATE + INTERVAL '2 days', 352500.00, 345000.00, 360000.00, '1.0.0', NOW()),
(2, 1, CURRENT_DATE + INTERVAL '3 days', 355000.00, 347500.00, 362500.00, '1.0.0', NOW()),
(2, 2, CURRENT_DATE + INTERVAL '1 day', 348000.00, 340000.00, 356000.00, '1.0.0', NOW()),
(2, 2, CURRENT_DATE + INTERVAL '2 days', 350500.00, 342500.00, 358500.00, '1.0.0', NOW()),
(2, 2, CURRENT_DATE + INTERVAL '3 days', 353000.00, 345000.00, 361000.00, '1.0.0', NOW()),
-- Product 3 price predictions
(3, 1, CURRENT_DATE + INTERVAL '1 day', 150000.00, 147500.00, 152500.00, '1.0.0', NOW()),
(3, 1, CURRENT_DATE + INTERVAL '2 days', 151500.00, 149000.00, 154000.00, '1.0.0', NOW()),
(3, 1, CURRENT_DATE + INTERVAL '3 days', 153000.00, 150500.00, 155500.00, '1.0.0', NOW()),
(3, 2, CURRENT_DATE + INTERVAL '1 day', 149000.00, 146000.00, 152000.00, '1.0.0', NOW()),
(3, 2, CURRENT_DATE + INTERVAL '2 days', 150500.00, 147500.00, 153500.00, '1.0.0', NOW()),
(3, 2, CURRENT_DATE + INTERVAL '3 days', 152000.00, 149000.00, 155000.00, '1.0.0', NOW()),
-- Product 4 price predictions
(4, 1, CURRENT_DATE + INTERVAL '1 day', 450000.00, 441000.00, 459000.00, '1.0.0', NOW()),
(4, 1, CURRENT_DATE + INTERVAL '2 days', 452500.00, 443500.00, 461500.00, '1.0.0', NOW()),
(4, 1, CURRENT_DATE + INTERVAL '3 days', 455000.00, 446000.00, 464000.00, '1.0.0', NOW()),
(4, 2, CURRENT_DATE + INTERVAL '1 day', 448000.00, 439000.00, 457000.00, '1.0.0', NOW()),
(4, 2, CURRENT_DATE + INTERVAL '2 days', 450500.00, 441500.00, 459500.00, '1.0.0', NOW()),
(4, 2, CURRENT_DATE + INTERVAL '3 days', 453000.00, 444000.00, 462000.00, '1.0.0', NOW()),
-- Product 5 price predictions
(5, 1, CURRENT_DATE + INTERVAL '1 day', 75000.00, 73500.00, 76500.00, '1.0.0', NOW()),
(5, 1, CURRENT_DATE + INTERVAL '2 days', 76000.00, 74500.00, 77500.00, '1.0.0', NOW()),
(5, 1, CURRENT_DATE + INTERVAL '3 days', 77000.00, 75500.00, 78500.00, '1.0.0', NOW()),
(5, 2, CURRENT_DATE + INTERVAL '1 day', 74000.00, 72500.00, 75500.00, '1.0.0', NOW()),
(5, 2, CURRENT_DATE + INTERVAL '2 days', 75000.00, 73500.00, 76500.00, '1.0.0', NOW()),
(5, 2, CURRENT_DATE + INTERVAL '3 days', 76000.00, 74500.00, 77500.00, '1.0.0', NOW())
ON CONFLICT DO NOTHING;

-- Verify inserted data
SELECT COUNT(*) as recommendation_count FROM ml_product_recommendations;
SELECT COUNT(*) as price_prediction_count FROM ml_price_predictions;
