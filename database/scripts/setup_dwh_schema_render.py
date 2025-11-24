#!/usr/bin/env python3
"""
Script để tạo schema DWH + ML trên database Render
"""
import psycopg2
from pathlib import Path

# Database URL từ Render
DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

def main():
    print("=" * 60)
    print("SETUP DWH + ML SCHEMA - RENDER DATABASE")
    print("=" * 60)
    
    # Đọc SQL file
    sql_file = Path(__file__).parent.parent / "schema" / "datawarehouse.sql"
    
    if not sql_file.exists():
        print(f"❌ File không tồn tại: {sql_file}")
        return 1
    
    print(f"\n📄 Đọc file: {sql_file}")
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Kết nối database
    print(f"\n🔌 Kết nối database Render...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        cur = conn.cursor()
        
        print("✅ Kết nối thành công!")
        
        # Thực thi SQL
        print(f"\n⚙️  Thực thi SQL script...")
        cur.execute(sql_content)
        conn.commit()
        
        print("✅ Schema DWH + ML đã được tạo thành công!")
        
        # Kiểm tra kết quả DWH
        print(f"\n🔍 Kiểm tra bảng DWH đã tạo...")
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'dwh'
            ORDER BY table_name;
        """)
        
        dwh_tables = cur.fetchall()
        if dwh_tables:
            print(f"\n✅ Đã tạo {len(dwh_tables)} bảng trong schema dwh:")
            for (table_name,) in dwh_tables:
                print(f"   - dwh.{table_name}")
        else:
            print("⚠️  Không tìm thấy bảng nào trong schema dwh")
        
        # Kiểm tra kết quả ML
        print(f"\n🔍 Kiểm tra bảng ML đã tạo...")
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ml'
            ORDER BY table_name;
        """)
        
        ml_tables = cur.fetchall()
        if ml_tables:
            print(f"\n✅ Đã tạo {len(ml_tables)} bảng trong schema ml:")
            for (table_name,) in ml_tables:
                print(f"   - ml.{table_name}")
        else:
            print("⚠️  Không tìm thấy bảng nào trong schema ml")
        
        cur.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ HOÀN THÀNH!")
        print("=" * 60)
        return 0
        
    except psycopg2.Error as e:
        print(f"\n❌ Lỗi database: {e}")
        print(f"   Code: {e.pgcode}")
        print(f"   Message: {e.pgerror}")
        return 1
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())

