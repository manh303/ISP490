#!/usr/bin/env python3
"""
Check and Validate Database Schema
"""
import psycopg2
import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss_1'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', '6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G')
}

def check_table_exists(conn, table_name):
    """Kiểm tra bảng có tồn tại"""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            )
        """)
        return cur.fetchone()[0]

def get_table_structure(conn, table_name):
    """Lấy cấu trúc bảng"""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        return cur.fetchall()

def get_table_row_count(conn, table_name):
    """Lấy số dòng"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchone()[0]
    except:
        return None

def main():
    try:
        logger.info("=" * 70)
        logger.info("DATABASE SCHEMA CHECKER")
        logger.info("=" * 70)
        
        # Kết nối
        logger.info(f"\n🔌 Kết nối database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✓ Kết nối thành công")
        
        # Kiểm tra bảng sản phẩm
        tables_to_check = ['ods_product_clean', 'stg_raw_products', 'dwh_products']
        
        logger.info("\n📊 KIỂM TRA BẢNG DỮ LIỆU")
        logger.info("-" * 70)
        
        for table_name in tables_to_check:
            if check_table_exists(conn, table_name):
                logger.info(f"\n✓ Bảng '{table_name}' tồn tại")
                
                # Lấy cấu trúc
                columns = get_table_structure(conn, table_name)
                logger.info(f"  📋 Columns ({len(columns)}):")
                
                for col_name, data_type, is_nullable in columns:
                    nullable = "NULLABLE" if is_nullable == 'YES' else "NOT NULL"
                    logger.info(f"      - {col_name}: {data_type:20} ({nullable})")
                
                # Lấy số dòng
                row_count = get_table_row_count(conn, table_name)
                if row_count is not None:
                    logger.info(f"  📈 Số dòng: {row_count}")
                
                # Kiểm tra columns quan trọng
                important_cols = {
                    'product_id': 'ID sản phẩm',
                    'category': 'Danh mục',
                    'category_sk': 'Category surrogate key',
                    'product_name': 'Tên sản phẩm',
                    'price': 'Giá',
                    'updated_at': 'Thời gian cập nhật'
                }
                
                col_names = [col[0] for col in columns]
                logger.info(f"\n  ✓ Kiểm tra columns quan trọng:")
                for col_name, description in important_cols.items():
                    if col_name in col_names:
                        logger.info(f"    ✓ {col_name}: {description}")
                    else:
                        logger.info(f"    ✗ {col_name}: {description} (THIẾU)")
            else:
                logger.warning(f"✗ Bảng '{table_name}' không tồn tại")
        
        # Kiểm tra tất cả bảng
        logger.info("\n📋 DANH SÁCH TẤT CẢ BẢNG TRONG DATABASE")
        logger.info("-" * 70)
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            all_tables = cur.fetchall()
            
            if all_tables:
                for table in all_tables:
                    count = get_table_row_count(conn, table[0])
                    if count is not None:
                        logger.info(f"  - {table[0]}: {count} dòng")
                    else:
                        logger.info(f"  - {table[0]}")
            else:
                logger.warning("  Không có bảng nào")
        
        # Lời khuyên
        logger.info("\n💡 LỜI KHUYÊN")
        logger.info("-" * 70)
        
        if check_table_exists(conn, 'ods_product_clean'):
            columns = get_table_structure(conn, 'ods_product_clean')
            col_names = [col[0] for col in columns]
            
            if 'category_sk' not in col_names:
                logger.warning("  - Bảng ods_product_clean chưa có column 'category_sk'")
                logger.info("    Script sẽ tự động thêm column này")
            
            if 'updated_at' not in col_names:
                logger.warning("  - Bảng ods_product_clean chưa có column 'updated_at'")
                logger.info("    Script sẽ bỏ qua cập nhật updated_at")
            
            if 'category' not in col_names:
                logger.error("  ✗ Bảng ods_product_clean THIẾU column 'category'")
                logger.error("    Không thể ánh xạ danh mục!")
        
        conn.close()
        logger.info("\n✓ Hoàn tất kiểm tra")
        
    except Exception as e:
        logger.error(f"✗ Lỗi: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
