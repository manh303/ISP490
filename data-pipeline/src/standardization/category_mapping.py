#!/usr/bin/env python3
"""
Category Mapping - Standardize categories across platforms
"""
import psycopg2
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4')
}

# Standard category mapping
CATEGORY_MAP = {
    'điện thoại': 'Electronics > Mobile Phones',
    'laptop': 'Electronics > Computers > Laptops',
    'máy tính bảng': 'Electronics > Tablets',
    'đồng hồ thông minh': 'Electronics > Wearables > Smartwatches',
    'tai nghe': 'Electronics > Audio > Headphones',
    'máy ảnh': 'Electronics > Cameras',
    'loa bluetooth': 'Electronics > Audio > Speakers',
    'màn hình máy tính': 'Electronics > Computers > Monitors',
    'chuột máy tính': 'Electronics > Computers > Accessories > Mouse',
    'bàn phím': 'Electronics > Computers > Accessories > Keyboard',
    'tivi smart': 'Electronics > TVs',
    'máy in': 'Electronics > Computers > Printers'
}

def create_category_tables(conn):
    """Create category dimension and mapping tables"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dwh_dim_category (
                category_sk SERIAL PRIMARY KEY,
                category_code VARCHAR(100) UNIQUE NOT NULL,
                category_name TEXT NOT NULL,
                parent_category_sk INT,
                category_level INT DEFAULT 1,
                category_path TEXT
            );
            
            CREATE TABLE IF NOT EXISTS ods_category_mapping (
                id SERIAL PRIMARY KEY,
                source_category TEXT NOT NULL,
                standard_category TEXT NOT NULL,
                category_sk INT,
                UNIQUE(source_category)
            );
        """)
        conn.commit()
        print("[OK] Category tables created")

def load_standard_categories(conn):
    """Load standard category hierarchy"""
    categories = {}
    
    with conn.cursor() as cur:
        for source_cat, standard_cat in CATEGORY_MAP.items():
            parts = standard_cat.split(' > ')
            
            parent_sk = None
            for level, part in enumerate(parts, 1):
                code = part.lower().replace(' ', '_')
                
                if code not in categories:
                    cur.execute("""
                        INSERT INTO dwh_dim_category 
                        (category_code, category_name, parent_category_sk, category_level, category_path)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (category_code) DO UPDATE 
                        SET category_name = EXCLUDED.category_name
                        RETURNING category_sk
                    """, (code, part, parent_sk, level, standard_cat))
                    
                    result = cur.fetchone()
                    category_sk = result[0] if result else None
                    categories[code] = category_sk
                else:
                    category_sk = categories[code]
                
                parent_sk = category_sk
            
            # Map source to standard
            cur.execute("""
                INSERT INTO ods_category_mapping (source_category, standard_category, category_sk)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_category) DO UPDATE 
                SET standard_category = EXCLUDED.standard_category,
                    category_sk = EXCLUDED.category_sk
            """, (source_cat, standard_cat, category_sk))
        
        conn.commit()
        print(f"[OK] Loaded {len(CATEGORY_MAP)} category mappings")

def apply_category_mapping(conn):
    """Apply category mapping to products"""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ods_product_clean p
            SET category_sk = cm.category_sk
            FROM stg_raw_products srp
            JOIN ods_category_mapping cm ON LOWER(srp.raw_data->>'category') = cm.source_category
            WHERE p.global_product_id = srp.source_platform || '_' || srp.platform_product_id
        """)
        count = cur.rowcount
        conn.commit()
        print(f"[OK] Applied categories to {count} products")

def main():
    print("CATEGORY MAPPING")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        create_category_tables(conn)
        load_standard_categories(conn)
        apply_category_mapping(conn)
        print("\n[OK] COMPLETE!")
    except Exception as e:
        print(f"\n[ERROR] FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
