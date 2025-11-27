#!/usr/bin/env python3
"""
Script để kiểm tra schemas (META, DWH, ML) trên database Render
"""
import psycopg2
from pathlib import Path

# Database URL từ Render
DATABASE_URL = "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"

def check_schema_exists(cur, schema_name):
    """Kiểm tra schema tồn tại"""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.schemata 
            WHERE schema_name = %s
        );
    """, (schema_name,))
    return cur.fetchone()[0]

def check_table_exists(cur, schema_name, table_name):
    """Kiểm tra bảng tồn tại"""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        );
    """, (schema_name, table_name))
    return cur.fetchone()[0]

def get_table_row_count(cur, schema_name, table_name):
    """Lấy số dòng của bảng"""
    try:
        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
        return cur.fetchone()[0]
    except:
        return None

def main():
    print("=" * 70)
    print("KIỂM TRA SCHEMAS - RENDER DATABASE")
    print("=" * 70)
    
    # Kết nối database
    print(f"\n🔌 Kết nối database Render...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        print("✅ Kết nối thành công!")
        
        # ===================================================================
        # 1. KIỂM TRA SCHEMA META
        # ===================================================================
        print("\n" + "=" * 70)
        print("1. SCHEMA META")
        print("=" * 70)
        
        if check_schema_exists(cur, 'meta'):
            print("✅ Schema 'meta' tồn tại")
            
            meta_tables = [
                'etl_job',
                'etl_run',
                'etl_log',
                'table_stats',
                'data_quality_issue',
                'data_quality_rule',
                'data_quality_check_result'
            ]
            
            print("\nCác bảng trong schema meta:")
            for table in meta_tables:
                exists = check_table_exists(cur, 'meta', table)
                status = "✅" if exists else "❌ THIẾU"
                row_count = get_table_row_count(cur, 'meta', table) if exists else 0
                print(f"  {status} meta.{table:<30} ({row_count:>6} rows)")
        else:
            print("❌ Schema 'meta' CHƯA TỒN TẠI")
            print("   → Chạy: python database/scripts/setup_meta_schema_render.py")
        
        # ===================================================================
        # 2. KIỂM TRA SCHEMA DWH
        # ===================================================================
        print("\n" + "=" * 70)
        print("2. SCHEMA DWH")
        print("=" * 70)
        
        if check_schema_exists(cur, 'dwh'):
            print("✅ Schema 'dwh' tồn tại")
            
            # Dimensions
            print("\n📊 DIMENSION TABLES:")
            dim_tables = [
                'dim_date',
                'dim_platform',
                'dim_brand',
                'dim_category',
                'dim_product'
            ]
            
            for table in dim_tables:
                exists = check_table_exists(cur, 'dwh', table)
                status = "✅" if exists else "❌ THIẾU"
                row_count = get_table_row_count(cur, 'dwh', table) if exists else 0
                print(f"  {status} dwh.{table:<30} ({row_count:>6} rows)")
            
            # Facts
            print("\n📈 FACT TABLES:")
            fact_tables = [
                'fact_product_daily',
                'fact_review',
                'fact_review_daily'
            ]
            
            for table in fact_tables:
                exists = check_table_exists(cur, 'dwh', table)
                status = "✅" if exists else "❌ THIẾU"
                row_count = get_table_row_count(cur, 'dwh', table) if exists else 0
                print(f"  {status} dwh.{table:<30} ({row_count:>6} rows)")
        else:
            print("❌ Schema 'dwh' CHƯA TỒN TẠI")
            print("   → Chạy: python database/scripts/setup_dwh_schema_render.py")
        
        # ===================================================================
        # 3. KIỂM TRA SCHEMA ML
        # ===================================================================
        print("\n" + "=" * 70)
        print("3. SCHEMA ML")
        print("=" * 70)
        
        if check_schema_exists(cur, 'ml'):
            print("✅ Schema 'ml' tồn tại")
            
            ml_tables = [
                'dim_ml_model',
                'fact_price_prediction',
                'fact_product_recommendation'
            ]
            
            print("\nCác bảng trong schema ml:")
            for table in ml_tables:
                exists = check_table_exists(cur, 'ml', table)
                status = "✅" if exists else "❌ THIẾU"
                row_count = get_table_row_count(cur, 'ml', table) if exists else 0
                print(f"  {status} ml.{table:<30} ({row_count:>6} rows)")
        else:
            print("❌ Schema 'ml' CHƯA TỒN TẠI")
            print("   → Schema ml thường được tạo cùng với dwh")
        
        # ===================================================================
        # 4. TỔNG KẾT
        # ===================================================================
        print("\n" + "=" * 70)
        print("4. TỔNG KẾT")
        print("=" * 70)
        
        cur.execute("""
            SELECT 
                schemaname as schema_name,
                COUNT(*) as table_count,
                pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_size
            FROM pg_tables
            WHERE schemaname IN ('meta', 'dwh', 'ml')
            GROUP BY schemaname
            ORDER BY schemaname;
        """)
        
        summary = cur.fetchall()
        if summary:
            print("\n📊 Thống kê schemas:")
            print(f"{'Schema':<15} {'Số bảng':<15} {'Kích thước':<15}")
            print("-" * 45)
            for (schema, count, size) in summary:
                print(f"{schema:<15} {count:<15} {size:<15}")
        
        # Kiểm tra data freshness
        print("\n" + "=" * 70)
        print("5. DATA FRESHNESS")
        print("=" * 70)
        
        if check_table_exists(cur, 'dwh', 'fact_product_daily'):
            cur.execute("""
                SELECT 
                    MAX(dd.date_value) as latest_date,
                    CURRENT_DATE - MAX(dd.date_value) as days_behind
                FROM dwh.fact_product_daily fpd
                JOIN dwh.dim_date dd ON fpd.date_sk = dd.date_sk;
            """)
            result = cur.fetchone()
            if result and result[0]:
                latest_date, days_behind = result
                print(f"\n📅 Dữ liệu mới nhất: {latest_date}")
                if days_behind == 0:
                    print(f"✅ Dữ liệu cập nhật (hôm nay)")
                elif days_behind == 1:
                    print(f"⚠️  Dữ liệu chậm 1 ngày (hôm qua)")
                else:
                    print(f"❌ Dữ liệu chậm {days_behind} ngày")
            else:
                print("⚠️  Chưa có dữ liệu trong fact_product_daily")
        
        cur.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("✅ KIỂM TRA HOÀN TẤT!")
        print("=" * 70)
        return 0
        
    except psycopg2.Error as e:
        print(f"\n❌ Lỗi database: {e}")
        print(f"   Code: {e.pgcode}")
        return 1
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())

