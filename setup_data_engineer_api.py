                                                                                                        #!/usr/bin/env python3
"""
Quick Setup Script for Data Engineer API
Run this to setup everything automatically
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

# Database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://dss_user:dss_password_123@localhost/ecommerce_dss"
)

def print_step(step_num, message):
    """Print formatted step"""
    print(f"\n{'='*60}")
    print(f"STEP {step_num}: {message}")
    print('='*60)

def print_success(message):
    """Print success message"""
    print(f"✅ {message}")

def print_error(message):
    """Print error message"""
    print(f"❌ {message}")

def print_info(message):
    """Print info message"""
    print(f"ℹ️  {message}")

# ============================================================
# STEP 1: Check Database Connection
# ============================================================
def step1_check_connection():
    print_step(1, "Checking Database Connection")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print_success(f"Connected to PostgreSQL")
        print_info(f"Version: {version[:50]}...")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print_error(f"Connection failed: {e}")
        return False

# ============================================================
# STEP 2: Check Existing Meta Schema
# ============================================================
def step2_check_meta_schema():
    print_step(2, "Checking Existing Meta Schema")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if meta schema exists
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'meta';
        """)
        
        if cur.fetchone():
            print_success("Meta schema exists")
            
            # Count tables
            cur.execute("""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_schema = 'meta';
            """)
            count = cur.fetchone()['count']
            print_info(f"Found {count} tables in meta schema")
            
            if count < 15:
                print_info("Need to apply extended schema (expecting 15 tables)")
                return False
            else:
                print_success("Extended schema already applied!")
                return True
        else:
            print_error("Meta schema does NOT exist")
            print_info("Please run meta_schema.sql first!")
            return False
            
    except Exception as e:
        print_error(f"Check failed: {e}")
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# ============================================================
# STEP 3: Apply Extended Schema
# ============================================================
def step3_apply_extended_schema():
    print_step(3, "Applying Extended Schema")
    
    schema_file = 'database/schema/meta_schema_extended.sql'
    
    if not os.path.exists(schema_file):
        print_error(f"Schema file not found: {schema_file}")
        return False
    
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        print_info(f"Reading {schema_file}...")
        
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()
        
        print_info("Executing SQL...")
        cur.execute(sql)
        
        print_success("Extended schema applied successfully!")
        
        # Verify
        cur.execute("""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = 'meta';
        """)
        count = cur.fetchone()[0]
        print_info(f"Total meta tables: {count}")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print_error(f"Failed to apply schema: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================
# STEP 4: Verify Tables
# ============================================================
def step4_verify_tables():
    print_step(4, "Verifying Tables")
    
    expected_tables = [
        'etl_job', 'etl_run', 'etl_log', 'table_stats',
        'data_quality_issue', 'data_quality_rule', 'data_quality_check_result',
        'db_connection_health', 'schema_version', 'data_lineage',
        'pipeline_dependency', 'alert_config', 'alert_history',
        'query_performance', 'storage_usage'
    ]
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'meta'
            ORDER BY table_name;
        """)
        
        actual_tables = [row['table_name'] for row in cur.fetchall()]
        
        print_info(f"Found {len(actual_tables)} tables:")
        for table in actual_tables:
            status = "✅" if table in expected_tables else "⚠️"
            print(f"  {status} {table}")
        
        missing = set(expected_tables) - set(actual_tables)
        if missing:
            print_error(f"Missing tables: {', '.join(missing)}")
            return False
        else:
            print_success("All expected tables present!")
            return True
            
    except Exception as e:
        print_error(f"Verification failed: {e}")
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# ============================================================
# STEP 5: Test API Endpoint (requires backend running)
# ============================================================
def step5_test_api():
    print_step(5, "Testing API Endpoint")
    
    try:
        import requests
        
        api_url = "http://localhost:8000/api/v1/data-engineer/health"
        
        print_info(f"Calling {api_url}...")
        response = requests.get(api_url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            print_success("API is responding!")
            print_info(f"Response: {data}")
            return True
        else:
            print_error(f"API returned status {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print_error("Cannot connect to API")
        print_info("Make sure backend is running: docker-compose up -d backend")
        return False
    except ImportError:
        print_info("requests library not available, skipping API test")
        return None
    except Exception as e:
        print_error(f"API test failed: {e}")
        return False

# ============================================================
# MAIN
# ============================================================
def main():
    print("\n" + "="*60)
    print("DATA ENGINEER API - QUICK SETUP")
    print("="*60)
    
    results = []
    
    # Step 1: Check connection
    if not step1_check_connection():
        print("\n❌ Setup failed at Step 1")
        return 1
    results.append(("Database Connection", True))
    
    # Step 2: Check existing schema
    schema_exists = step2_check_meta_schema()
    results.append(("Meta Schema Check", schema_exists))
    
    # Step 3: Apply extended schema (if needed)
    if not schema_exists:
        if not step3_apply_extended_schema():
            print("\n❌ Setup failed at Step 3")
            return 1
        results.append(("Apply Extended Schema", True))
    else:
        print_info("Skipping Step 3 (schema already complete)")
        results.append(("Apply Extended Schema", "Skipped"))
    
    # Step 4: Verify tables
    if not step4_verify_tables():
        print("\n❌ Setup failed at Step 4")
        return 1
    results.append(("Verify Tables", True))
    
    # Step 5: Test API (optional)
    api_result = step5_test_api()
    if api_result is not None:
        results.append(("API Test", api_result))
    else:
        results.append(("API Test", "Skipped"))
    
    # Summary
    print("\n" + "="*60)
    print("SETUP SUMMARY")
    print("="*60)
    
    for step, result in results:
        if result == True:
            status = "✅"
        elif result == False:
            status = "❌"
        else:
            status = "⚠️"
        print(f"{status} {step}: {result}")
    
    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    print("1. Restart backend:")
    print("   docker-compose restart backend")
    print("\n2. Visit API docs:")
    print("   http://localhost:8000/docs")
    print("\n3. Run metrics collector:")
    print("   python backend/scripts/collect_metadata_metrics.py")
    print("\n4. Read documentation:")
    print("   - DATA_ENGINEER_API_SETUP.md")
    print("   - backend/scripts/DATA_ENGINEER_QUICK_REFERENCE.md")
    print("="*60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())


