#!/usr/bin/env python3
"""
Script tổng hợp để setup toàn bộ database Render
Chạy: python setup_render_database.py
"""
import sys
import subprocess
from pathlib import Path

def run_script(script_path):
    """Chạy một Python script"""
    print(f"\n{'='*70}")
    print(f"Chạy: {script_path}")
    print('='*70)
    
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=False
    )
    
    return result.returncode == 0

def main():
    print("=" * 70)
    print("SETUP TOÀN BỘ DATABASE RENDER")
    print("=" * 70)
    
    scripts_dir = Path(__file__).parent / "database" / "scripts"
    
    # Bước 1: Kiểm tra trạng thái hiện tại
    print("\n[Bước 1/4] Kiểm tra trạng thái hiện tại...")
    check_script = scripts_dir / "check_schemas_render.py"
    run_script(check_script)
    
    # Hỏi user có muốn tiếp tục không
    response = input("\n📋 Bạn có muốn setup schemas? (y/n): ")
    if response.lower() != 'y':
        print("Đã hủy.")
        return 0
    
    # Bước 2: Setup schema META
    print("\n[Bước 2/4] Setup schema META...")
    meta_script = scripts_dir / "setup_meta_schema_render.py"
    if not run_script(meta_script):
        print("\n❌ Setup META failed!")
        return 1
    
    # Bước 3: Setup schema DWH + ML
    print("\n[Bước 3/4] Setup schema DWH + ML...")
    dwh_script = scripts_dir / "setup_dwh_schema_render.py"
    if not run_script(dwh_script):
        print("\n❌ Setup DWH failed!")
        return 1
    
    # Bước 4: Kiểm tra lại
    print("\n[Bước 4/4] Kiểm tra lại...")
    run_script(check_script)
    
    print("\n" + "=" * 70)
    print("✅ SETUP HOÀN TẤT!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Chạy Airflow DAG: minio_ecommerce_dwh_pipeline")
    print("2. Monitor ETL runs: SELECT * FROM meta.etl_run;")
    print("3. Check data quality: SELECT * FROM meta.data_quality_issue;")
    
    return 0

if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Đã hủy bởi user")
        exit(1)
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

