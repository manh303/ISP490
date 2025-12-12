#!/usr/bin/env python3
"""
Technical Metadata - Populate metadata tables (UPDATED FOR CURRENT SCHEMA)
Only includes tables that actually exist: dwh.* and ml.*
"""
import psycopg2
import os
from datetime import datetime

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'dss_password_123')
}

def create_metadata_tables(conn):
    """Create all metadata tables"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta_source_system (
                source_id BIGSERIAL PRIMARY KEY,
                code VARCHAR(32) UNIQUE NOT NULL,
                name TEXT,
                owner_contact TEXT
            );
            
            CREATE TABLE IF NOT EXISTS meta_dataset (
                dataset_id BIGSERIAL PRIMARY KEY,
                source_id BIGINT,
                layer VARCHAR(16) NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                dataset_type VARCHAR(24) NOT NULL,
                pii_class VARCHAR(24),
                retention_days INT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                UNIQUE (schema_name, table_name),
                FOREIGN KEY (source_id) REFERENCES meta_source_system(source_id)
            );
            
            CREATE TABLE IF NOT EXISTS meta_job (
                job_id BIGSERIAL PRIMARY KEY,
                job_name TEXT UNIQUE NOT NULL,
                owner TEXT,
                schedule TEXT,
                active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS meta_expectation (
                exp_id BIGSERIAL PRIMARY KEY,
                dataset_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                severity VARCHAR(8) NOT NULL,
                check_sql TEXT NOT NULL,
                owner TEXT,
                tags TEXT,
                FOREIGN KEY (dataset_id) REFERENCES meta_dataset(dataset_id)
            );
            
            CREATE TABLE IF NOT EXISTS meta_business_term (
                term_id BIGSERIAL PRIMARY KEY,
                term_name TEXT UNIQUE NOT NULL,
                definition TEXT,
                steward TEXT,
                status VARCHAR(12)
            );
        """)
        conn.commit()
        print("[OK] Metadata tables created")

def populate_source_systems(conn):
    """Populate source system metadata"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO meta_source_system (code, name, owner_contact)
            VALUES 
                ('LAZADA', 'Lazada Vietnam', 'data-team@company.com'),
                ('TIKI', 'Tiki Vietnam', 'data-team@company.com'),
                ('INTERNAL', 'Internal System', 'data-team@company.com')
            ON CONFLICT (code) DO NOTHING
        """)
        conn.commit()
        print("[OK] Source systems populated")

def populate_datasets(conn):
    """Populate dataset metadata - ONLY EXISTING TABLES"""
    datasets = [
        # DWH - Dimensions (correct table names without prefix)
        ('INTERNAL', 'dwh', 'dim_date', 'dimension', None, None),
        ('INTERNAL', 'dwh', 'dim_platform', 'dimension', None, None),
        ('INTERNAL', 'dwh', 'dim_brand', 'dimension', None, None),
        ('INTERNAL', 'dwh', 'dim_category', 'dimension', None, None),
        ('INTERNAL', 'dwh', 'dim_product', 'dimension', None, None),
        
        # DWH - Facts
        ('INTERNAL', 'dwh', 'fact_product_daily', 'fact', None, 730),  # 2 years retention
        ('INTERNAL', 'dwh', 'fact_review', 'fact', 'PII', 730),
        ('INTERNAL', 'dwh', 'fact_review_daily', 'fact', None, 730),
        
        # ML Tables
        ('INTERNAL', 'ml', 'dim_ml_model', 'dimension', None, None),
        ('INTERNAL', 'ml', 'fact_price_prediction', 'fact', None, 90),
        ('INTERNAL', 'ml', 'fact_product_recommendation', 'fact', None, 90),
    ]
    
    with conn.cursor() as cur:
        for source, schema, table, dtype, pii, retention in datasets:
            cur.execute("""
                INSERT INTO meta_dataset 
                (source_id, layer, schema_name, table_name, dataset_type, pii_class, retention_days, created_at, updated_at)
                SELECT 
                    s.source_id, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                FROM meta_source_system s
                WHERE s.code = %s
                ON CONFLICT (schema_name, table_name) DO UPDATE
                SET updated_at = NOW()
            """, (schema, schema, table, dtype, pii, retention, source))
        
        conn.commit()
        print(f"[OK] Populated {len(datasets)} datasets")

def populate_jobs(conn):
    """Populate job metadata"""
    jobs = [
        ('minio_ecommerce_dwh_pipeline', 'data-team', '0 0 * * *'),
        ('collect_metadata', 'data-team', '0 1 * * *'),
        ('ml_price_prediction', 'ml-team', '0 2 * * *'),
        ('ml_product_recommendation', 'ml-team', '0 3 * * *'),
    ]
    
    with conn.cursor() as cur:
        for job_name, owner, schedule in jobs:
            cur.execute("""
                INSERT INTO meta_job (job_name, owner, schedule, active)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (job_name) DO UPDATE
                SET owner = EXCLUDED.owner, schedule = EXCLUDED.schedule
            """, (job_name, owner, schedule))
        
        conn.commit()
        print(f"[OK] Populated {len(jobs)} jobs")

def populate_expectations(conn):
    """Populate data quality expectations"""
    expectations = [
        ('fact_product_daily', 'positive_prices', 'error', 
         'SELECT COUNT(*) FROM dwh.fact_product_daily WHERE avg_price <= 0'),
        ('fact_product_daily', 'no_future_dates', 'error',
         'SELECT COUNT(*) FROM dwh.fact_product_daily WHERE date_sk > TO_CHAR(NOW(), \'YYYYMMDD\')::INT'),
        ('fact_review', 'rating_valid', 'error',
         'SELECT COUNT(*) FROM dwh.fact_review WHERE rating NOT BETWEEN 1 AND 5'),
        ('dim_product', 'product_name_not_null', 'error',
         'SELECT COUNT(*) FROM dwh.dim_product WHERE product_name IS NULL OR product_name = \'\''),
    ]
    
    with conn.cursor() as cur:
        for table, name, severity, sql in expectations:
            cur.execute("""
                INSERT INTO meta_expectation (dataset_id, name, severity, check_sql, owner)
                SELECT d.dataset_id, %s, %s, %s, 'data-team'
                FROM meta_dataset d
                WHERE d.table_name = %s
                ON CONFLICT DO NOTHING
            """, (name, severity, sql, table))
        
        conn.commit()
        print(f"[OK] Populated {len(expectations)} expectations")

def populate_business_terms(conn):
    """Populate business glossary"""
    terms = [
        ('Product', 'A sellable item on e-commerce platform', 'product-owner'),
        ('Price Point', 'Daily aggregated price metrics for a product', 'pricing-team'),
        ('Review', 'Customer feedback and rating for product', 'customer-team'),
        ('Product Key', 'Unique identifier across all platforms (global_product_id)', 'data-team'),
        ('Platform', 'E-commerce marketplace (Tiki or Lazada)', 'data-team'),
        ('Brand', 'Product manufacturer or brand name', 'product-team'),
        ('Category', 'Standardized product classification', 'product-team'),
        ('ML Model', 'Machine learning prediction model', 'ml-team'),
        ('Recommendation', 'AI-generated product suggestion', 'ml-team'),
        ('Price Prediction', 'Forecasted future product price', 'ml-team'),
    ]
    
    with conn.cursor() as cur:
        for term, definition, steward in terms:
            cur.execute("""
                INSERT INTO meta_business_term (term_name, definition, steward, status)
                VALUES (%s, %s, %s, 'approved')
                ON CONFLICT (term_name) DO UPDATE
                SET definition = EXCLUDED.definition
            """, (term, definition, steward))
        
        conn.commit()
        print(f"[OK] Populated {len(terms)} business terms")

def main():
    print("TECHNICAL METADATA - UPDATED FOR CURRENT SCHEMA")
    print("=" * 60)
    print("Only DWH and ML schemas (no staging/ods)")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        create_metadata_tables(conn)
        populate_source_systems(conn)
        populate_datasets(conn)
        populate_jobs(conn)
        populate_expectations(conn)
        populate_business_terms(conn)
        print("\n[OK] COMPLETE!")
        print("✅ Updated metadata for current schema structure")
    except Exception as e:
        print(f"\n[ERROR] FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
