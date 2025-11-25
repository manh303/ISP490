#!/usr/bin/env python3
"""
Technical Metadata - Populate metadata tables
"""
import psycopg2
import os
from datetime import datetime

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss_1'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', '6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G')
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
    """Populate dataset metadata"""
    datasets = [
        ('LAZADA', 'stg', 'stg_raw_products', 'table', None, 90),
        ('LAZADA', 'stg', 'stg_raw_reviews', 'table', None, 90),
        ('TIKI', 'stg', 'stg_raw_products', 'table', None, 90),
        ('TIKI', 'stg', 'stg_raw_reviews', 'table', None, 90),
        ('INTERNAL', 'ods', 'ods_product_clean', 'table', None, 365),
        ('INTERNAL', 'ods', 'ods_price_point', 'table', None, 365),
        ('INTERNAL', 'ods', 'ods_review_clean', 'table', 'PII', 365),
        ('INTERNAL', 'dwh', 'dwh_dim_product', 'table', None, None),
        ('INTERNAL', 'dwh', 'dwh_fact_product_daily', 'table', None, None),
    ]
    
    with conn.cursor() as cur:
        for source, layer, table, dtype, pii, retention in datasets:
            cur.execute("""
                INSERT INTO meta_dataset 
                (source_id, layer, schema_name, table_name, dataset_type, pii_class, retention_days, created_at, updated_at)
                SELECT 
                    s.source_id, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                FROM meta_source_system s
                WHERE s.code = %s
                ON CONFLICT (schema_name, table_name) DO UPDATE
                SET updated_at = NOW()
            """, (layer, layer, table, dtype, pii, retention, source))
        
        conn.commit()
        print(f"[OK] Populated {len(datasets)} datasets")

def populate_jobs(conn):
    """Populate job metadata"""
    jobs = [
        ('crawl_lazada', 'data-team', '0 10 * * *'),
        ('crawl_tiki', 'data-team', '0 10 * * *'),
        ('load_raw_data', 'data-team', '0 11 * * *'),
        ('data_cleaning', 'data-team', '0 12 * * *'),
        ('data_quality', 'data-team', '0 13 * * *'),
        ('warehouse_build', 'data-team', '0 14 * * *'),
        ('datamart_build', 'data-team', '0 15 * * *'),
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
        ('ods_product_clean', 'product_name_not_null', 'error', 
         'SELECT COUNT(*) FROM ods_product_clean WHERE product_name IS NULL OR product_name = \'\''),
        ('ods_price_point', 'price_positive', 'error',
         'SELECT COUNT(*) FROM ods_price_point WHERE price_current <= 0'),
        ('ods_review_clean', 'rating_valid', 'error',
         'SELECT COUNT(*) FROM ods_review_clean WHERE rating NOT BETWEEN 1 AND 5'),
        ('dwh_fact_product_daily', 'no_future_dates', 'error',
         'SELECT COUNT(*) FROM dwh_fact_product_daily WHERE date_sk > TO_CHAR(NOW(), \'YYYYMMDD\')::INT'),
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
        ('Price Point', 'Historical price snapshot at specific time', 'pricing-team'),
        ('Review', 'Customer feedback and rating for product', 'customer-team'),
        ('Global Product ID', 'Unique identifier across all platforms', 'data-team'),
        ('Master Product ID', 'Canonical identifier for matched products', 'data-team'),
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
    print("TECHNICAL METADATA")
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
    except Exception as e:
        print(f"\n[ERROR] FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
