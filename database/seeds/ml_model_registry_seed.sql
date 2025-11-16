-- Seed ML Model Registry with sample models

INSERT INTO ml_model_registry (
    model_name, 
    model_type, 
    version, 
    status, 
    description, 
    model_path,
    accuracy,
    precision,
    recall,
    f1_score,
    trained_at,
    metrics
) VALUES 
(
    'demand_linear_v1.0',
    'demand_prediction',
    '1.0.0',
    'active',
    'Linear regression model for demand prediction',
    '/models/ml-models/demand_linear.pkl',
    0.8750,
    0.8620,
    0.8880,
    0.8750,
    NOW(),
    '{"rmse": 12.5, "mae": 8.3, "r2_score": 0.8750}'::jsonb
),
(
    'demand_linear_v0.9',
    'demand_prediction',
    '0.9.0',
    'inactive',
    'Previous version of demand prediction model',
    '/models/ml-models/demand_linear_old.pkl',
    0.8500,
    0.8400,
    0.8600,
    0.8500,
    NOW() - INTERVAL '30 days',
    '{"rmse": 14.2, "mae": 9.1, "r2_score": 0.8500}'::jsonb
),
(
    'recommendation_nn_v1.0',
    'product_recommendation',
    '1.0.0',
    'active',
    'Nearest neighbors model for product recommendations',
    '/models/ml-models/recommendation_nearest_neighbors.pkl',
    0.7920,
    0.7850,
    0.8050,
    0.7920,
    NOW(),
    '{"precision_at_5": 0.82, "recall_at_5": 0.75, "nDCG": 0.79}'::jsonb
),
(
    'recommendation_kmeans_v1.0',
    'product_recommendation',
    '1.0.0',
    'active',
    'KMeans clustering model for product recommendations',
    '/models/ml-models/recommendation_kmeans.pkl',
    0.7650,
    0.7500,
    0.7800,
    0.7650,
    NOW(),
    '{"silhouette_score": 0.45, "inertia": 2850.5}'::jsonb
),
(
    'price_prediction_v1.0',
    'price_prediction',
    '1.0.0',
    'inactive',
    'Gradient boosting model for price predictions',
    '/models/ml-models/price_prediction_gb.pkl',
    0.8200,
    0.8100,
    0.8300,
    0.8200,
    NOW() - INTERVAL '7 days',
    '{"rmse": 150.25, "mae": 95.50, "r2_score": 0.8200}'::jsonb
),
(
    'customer_segmentation_v1.0',
    'customer_segmentation',
    '1.0.0',
    'active',
    'RFM-based customer segmentation model',
    '/models/ml-models/customer_segmentation.pkl',
    NULL,
    NULL,
    NULL,
    NULL,
    NOW() - INTERVAL '5 days',
    '{"num_clusters": 4, "silhouette_score": 0.52}'::jsonb
);

-- Verify inserted records
SELECT 
    model_id,
    model_name,
    model_type,
    version,
    status,
    accuracy,
    trained_at
FROM ml_model_registry
ORDER BY created_at DESC;
