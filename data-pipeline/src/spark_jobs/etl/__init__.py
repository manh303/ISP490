# src/spark_jobs/etl/__init__.py

"""
ETL Pipeline Package

Cấu trúc module:
- config.py: Tất cả cấu hình ENV, constants, CATEGORY_MAPPINGS
- spark_session.py: Tạo SparkSession với config MinIO/S3
- extract.py: Load raw products/reviews từ MinIO/local
- product_transforms.py: Clean, map, standardize, sync ID, dedup, validate products
- product_aggregation.py: Aggregate products daily & save cleaned data
- dwh_loader.py: Load dim_* & fact_product_daily vào DWH
- review_transforms.py: Transform pipeline cho reviews + sentiment analysis
- review_aggregation.py: Aggregate reviews daily & save results
- metadata_utils.py: Load fact_review & fact_review_daily
- pipeline_main.py: Main orchestrator (thay thế main() cũ)

Usage:
    python -m spark_jobs.etl.pipeline_main
    
    hoặc:
    
    from spark_jobs.etl.pipeline_main import run_etl
    run_etl()
"""

from .config import (
    MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY,
    CRAWLER_OUTPUT_DIR, MINIO_CLEANED_BUCKET, MINIO_PROCESSED_REVIEWS_BUCKET,
    SAVE_TO_MINIO, PROCESS_REVIEWS,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, JDBC_URL,
    DWH_SCHEMA, ML_SCHEMA,
    CATEGORY_MAPPINGS, STAR_SCHEMA_SQL_TEMPLATE
)

from .spark_session import create_spark_session

from .extract import load_raw_products, load_raw_reviews

from .product_transforms import (
    clean_products, map_product_categories, standardize_products,
    sync_product_identifiers, deduplicate_products, validate_products
)

from .product_aggregation import aggregate_products_daily, save_cleaned_products

from .dwh_loader import (
    get_db_connection, ensure_star_schema, make_slug, truncate_str,
    load_dimensions, load_fact_product_daily
)

from .review_transforms import (
    clean_review_data, standardize_review_data, synchronize_review_identifiers,
    deduplicate_review_data, validate_review_data, analyze_sentiment,
    add_review_time_features, run_review_transform_pipeline
)

from .review_aggregation import aggregate_reviews_daily, save_review_results

from .metadata_utils import (
    load_review_dimensions_to_dwh, load_fact_review_star, load_fact_review_daily_star
)

from .pipeline_main import run_etl

__all__ = [
    # Config
    'MINIO_HOST', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'MINIO_SECURE',
    'S3_ENDPOINT', 'S3_ACCESS_KEY', 'S3_SECRET_KEY',
    'CRAWLER_OUTPUT_DIR', 'MINIO_CLEANED_BUCKET', 'MINIO_PROCESSED_REVIEWS_BUCKET',
    'SAVE_TO_MINIO', 'PROCESS_REVIEWS',
    'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'JDBC_URL',
    'DWH_SCHEMA', 'ML_SCHEMA',
    'CATEGORY_MAPPINGS', 'STAR_SCHEMA_SQL_TEMPLATE',
    
    # Spark
    'create_spark_session',
    
    # Extract
    'load_raw_products', 'load_raw_reviews',
    
    # Product Transforms
    'clean_products', 'map_product_categories', 'standardize_products',
    'sync_product_identifiers', 'deduplicate_products', 'validate_products',
    
    # Product Aggregation
    'aggregate_products_daily', 'save_cleaned_products',
    
    # DWH Loader
    'get_db_connection', 'ensure_star_schema', 'make_slug', 'truncate_str',
    'load_dimensions', 'load_fact_product_daily',
    
    # Review Transforms
    'clean_review_data', 'standardize_review_data', 'synchronize_review_identifiers',
    'deduplicate_review_data', 'validate_review_data', 'analyze_sentiment',
    'add_review_time_features', 'run_review_transform_pipeline',
    
    # Review Aggregation
    'aggregate_reviews_daily', 'save_review_results',
    
    # Metadata Utils
    'load_review_dimensions_to_dwh', 'load_fact_review_star', 'load_fact_review_daily_star',
    
    # Main
    'run_etl',
]
