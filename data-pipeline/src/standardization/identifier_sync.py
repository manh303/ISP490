#!/usr/bin/env python3
"""
Identifier Synchronization - Match products across platforms
"""
import psycopg2
import os
from difflib import SequenceMatcher

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'dss_password_123')
}

def similarity(a, b):
    """Calculate string similarity"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def create_master_product_table(conn):
    """Create master product mapping table"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ods_product_master (
                master_product_id VARCHAR(36) PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                brand_name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS ods_product_mapping (
                global_product_id VARCHAR(36) PRIMARY KEY,
                master_product_id VARCHAR(36) NOT NULL,
                confidence_score DECIMAL(3,2),
                FOREIGN KEY (global_product_id) REFERENCES ods_product_clean(global_product_id),
                FOREIGN KEY (master_product_id) REFERENCES ods_product_master(master_product_id)
            );
        """)
        conn.commit()
        print("[OK] Master product tables created")

def synchronize_identifiers(conn):
    """Match products across platforms"""
    with conn.cursor() as cur:
        # Get all products
        cur.execute("""
            SELECT global_product_id, product_name, brand_name 
            FROM ods_product_clean
            ORDER BY brand_name, product_name
        """)
        products = cur.fetchall()
        
        matched = 0
        master_id = 1
        processed = set()
        
        for i, (pid1, name1, brand1) in enumerate(products):
            if pid1 in processed:
                continue
            
            # Create new master product
            master_pid = f"MASTER_{master_id:08d}"
            cur.execute("""
                INSERT INTO ods_product_master (master_product_id, canonical_name, brand_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (master_product_id) DO NOTHING
            """, (master_pid, name1, brand1))
            
            # Map current product
            cur.execute("""
                INSERT INTO ods_product_mapping (global_product_id, master_product_id, confidence_score)
                VALUES (%s, %s, 1.0)
                ON CONFLICT (global_product_id) DO UPDATE SET master_product_id = EXCLUDED.master_product_id
            """, (pid1, master_pid))
            processed.add(pid1)
            
            # Find similar products
            for pid2, name2, brand2 in products[i+1:]:
                if pid2 in processed:
                    continue
                
                # Match if same brand and similar name
                if brand1 and brand2 and brand1.lower() == brand2.lower():
                    sim = similarity(name1, name2)
                    if sim > 0.85:  # 85% similarity threshold
                        cur.execute("""
                            INSERT INTO ods_product_mapping (global_product_id, master_product_id, confidence_score)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (global_product_id) DO UPDATE SET master_product_id = EXCLUDED.master_product_id
                        """, (pid2, master_pid, sim))
                        processed.add(pid2)
                        matched += 1
            
            master_id += 1
        
        conn.commit()
        print(f"[OK] Synchronized {matched} product matches")
        print(f"[OK] Created {master_id-1} master products")

def main():
    print("IDENTIFIER SYNCHRONIZATION")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        create_master_product_table(conn)
        synchronize_identifiers(conn)
        print("\n[OK] COMPLETE!")
    except Exception as e:
        print(f"\n[ERROR] FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
