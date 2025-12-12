#!/usr/bin/env python3
"""
Category Mapping - Standardize categories across platforms
Thích nghi với schema bảng ods_product_clean thực tế
"""
import psycopg2
from psycopg2.extras import execute_batch
import os
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'dss_password_123')
}

# Category mapping - linh hoạt, không phụ thuộc vào columns
CATEGORY_MAP = {
    # Điện thoại
    'điện thoại': 'Electronics|Mobile Phones|Smartphones',
    'mobile phone': 'Electronics|Mobile Phones|Smartphones',
    'smartphone': 'Electronics|Mobile Phones|Smartphones',
    'phone': 'Electronics|Mobile Phones|Smartphones',
    'điện thoại thông minh': 'Electronics|Mobile Phones|Smartphones',
    
    # Laptop và máy tính
    'laptop': 'Electronics|Computers|Laptops',
    'máy tính xách tay': 'Electronics|Computers|Laptops',
    'notebook': 'Electronics|Computers|Laptops',
    'máy tính để bàn': 'Electronics|Computers|Desktop',
    'desktop': 'Electronics|Computers|Desktop',
    'pc': 'Electronics|Computers|Desktop',
    
    # Máy tính bảng
    'máy tính bảng': 'Electronics|Tablets',
    'tablet': 'Electronics|Tablets',
    'ipad': 'Electronics|Tablets',
    
    # Đồng hồ thông minh
    'đồng hồ thông minh': 'Electronics|Wearables|Smartwatches',
    'smartwatch': 'Electronics|Wearables|Smartwatches',
    'smart watch': 'Electronics|Wearables|Smartwatches',
    
    # Tai nghe
    'tai nghe': 'Electronics|Audio|Headphones',
    'headphones': 'Electronics|Audio|Headphones',
    'earphone': 'Electronics|Audio|Earphones',
    'tai nghe không dây': 'Electronics|Audio|Headphones',
    'wireless earbuds': 'Electronics|Audio|Earphones',
    
    # Loa
    'loa bluetooth': 'Electronics|Audio|Speakers',
    'speaker': 'Electronics|Audio|Speakers',
    'loa': 'Electronics|Audio|Speakers',
    'bluetooth speaker': 'Electronics|Audio|Speakers',
    
    # Máy ảnh
    'máy ảnh': 'Electronics|Cameras',
    'camera': 'Electronics|Cameras',
    'máy ảnh kỹ thuật số': 'Electronics|Cameras',
    'digital camera': 'Electronics|Cameras',
    
    # Màn hình
    'màn hình máy tính': 'Electronics|Computers|Monitors',
    'monitor': 'Electronics|Computers|Monitors',
    'display': 'Electronics|Computers|Monitors',
    
    # Phụ kiện máy tính
    'chuột máy tính': 'Electronics|Computers|Accessories|Mouse',
    'mouse': 'Electronics|Computers|Accessories|Mouse',
    'bàn phím': 'Electronics|Computers|Accessories|Keyboard',
    'keyboard': 'Electronics|Computers|Accessories|Keyboard',
    'bàn phím cơ': 'Electronics|Computers|Accessories|Keyboard',
    'mechanical keyboard': 'Electronics|Computers|Accessories|Keyboard',
    
    # Tivi
    'tivi smart': 'Electronics|TVs|Smart TVs',
    'smart tv': 'Electronics|TVs|Smart TVs',
    'television': 'Electronics|TVs|Smart TVs',
    'tivi': 'Electronics|TVs|Smart TVs',
    
    # Máy in
    'máy in': 'Electronics|Computers|Printers',
    'printer': 'Electronics|Computers|Printers',
    
    # Router/Modem
    'router wifi': 'Electronics|Networking|Router',
    'modem': 'Electronics|Networking|Modem',
    'access point': 'Electronics|Networking|Access Points',
}

class CategoryMapper:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.id_column = None  # Sẽ detect từ schema
        
    def connect(self):
        """Kết nối tới database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("✓ Kết nối database thành công")
            return self.conn
        except Exception as e:
            logger.error(f"✗ Lỗi kết nối: {e}")
            raise
    
    def close(self):
        """Đóng kết nối"""
        if self.conn:
            self.conn.close()
            logger.info("✓ Đóng kết nối database")
    
    def check_table_structure(self):
        """Kiểm tra cấu trúc bảng ods_product_clean và detect columns"""
        try:
            with self.conn.cursor() as cur:
                # Kiểm tra bảng tồn tại
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'ods_product_clean'
                    )
                """)
                if not cur.fetchone()[0]:
                    logger.error("✗ Bảng ods_product_clean không tồn tại")
                    return False
                
                # Lấy danh sách columns
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = 'ods_product_clean'
                    ORDER BY ordinal_position
                """)
                columns = cur.fetchall()
                
                logger.info("📊 Cấu trúc bảng ods_product_clean:")
                col_dict = {}
                for col_name, data_type, is_nullable in columns:
                    nullable = "NULL" if is_nullable == 'YES' else "NOT NULL"
                    logger.info(f"  - {col_name}: {data_type} ({nullable})")
                    col_dict[col_name] = data_type
                
                # Detect ID column
                if 'global_product_id' in col_dict:
                    self.id_column = 'global_product_id'
                    logger.info("✓ Detect ID column: global_product_id")
                elif 'product_id' in col_dict:
                    self.id_column = 'product_id'
                    logger.info("✓ Detect ID column: product_id")
                else:
                    logger.error("✗ Không tìm thấy ID column (product_id hoặc global_product_id)")
                    return False
                
                # Kiểm tra category column
                if 'category' not in col_dict:
                    logger.error("✗ Thiếu column 'category'")
                    return False
                
                logger.info("✓ Detect category column: category")
                
                # Kiểm tra category_sk
                if 'category_sk' not in col_dict:
                    logger.info("⚠ Column 'category_sk' không tồn tại, sẽ thêm")
                else:
                    logger.info("✓ Column 'category_sk' đã tồn tại")
                
                logger.info("✓ Bảng có đầy đủ columns cần thiết")
                return True
        except Exception as e:
            logger.error(f"✗ Lỗi kiểm tra schema: {e}")
            return False
    
    def create_category_tables(self):
        """Tạo bảng category dimension và mapping"""
        try:
            with self.conn.cursor() as cur:
                # Bảng chiều danh mục
                cur.execute("""
                    DROP TABLE IF EXISTS ods_category_mapping CASCADE;
                    DROP TABLE IF EXISTS dwh_dim_category CASCADE;
                    
                    CREATE TABLE dwh_dim_category (
                        category_sk SERIAL PRIMARY KEY,
                        category_code VARCHAR(100) UNIQUE NOT NULL,
                        category_name TEXT NOT NULL,
                        parent_category_sk INT REFERENCES dwh_dim_category(category_sk),
                        category_level INT DEFAULT 1,
                        category_path TEXT NOT NULL,
                        full_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    -- Bảng mapping nguồn đến chuẩn
                    CREATE TABLE ods_category_mapping (
                        id SERIAL PRIMARY KEY,
                        source_category VARCHAR(255) NOT NULL UNIQUE,
                        standard_category VARCHAR(255) NOT NULL,
                        category_sk INT REFERENCES dwh_dim_category(category_sk),
                        confidence_score FLOAT DEFAULT 1.0,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    CREATE INDEX idx_source_category ON ods_category_mapping(source_category);
                    CREATE INDEX idx_category_path ON dwh_dim_category(category_path);
                """)
                self.conn.commit()
                logger.info("✓ Tạo bảng category thành công")
        except Exception as e:
            logger.error(f"✗ Lỗi tạo bảng: {e}")
            self.conn.rollback()
            raise
    
    def load_standard_categories(self):
        """Load danh mục tiêu chuẩn vào database"""
        try:
            with self.conn.cursor() as cur:
                category_hierarchy = {}
                
                # Xây dựng phân cấp danh mục
                for source_cat, standard_cat in CATEGORY_MAP.items():
                    parts = standard_cat.split('|')
                    parent_sk = None
                    path_parts = []
                    
                    for level, part in enumerate(parts, 1):
                        code = part.lower().replace(' ', '_').replace('|', '')
                        path_parts.append(code)
                        full_path = '|'.join(path_parts)
                        
                        if code not in category_hierarchy:
                            cur.execute("""
                                INSERT INTO dwh_dim_category 
                                (category_code, category_name, parent_category_sk, 
                                 category_level, category_path, full_path)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (category_code) DO UPDATE 
                                SET updated_at = CURRENT_TIMESTAMP
                                RETURNING category_sk
                            """, (code, part, parent_sk, level, standard_cat, full_path))
                            
                            result = cur.fetchone()
                            category_sk = result[0] if result else None
                            category_hierarchy[code] = category_sk
                        else:
                            category_sk = category_hierarchy[code]
                        
                        parent_sk = category_sk
                    
                    # Map nguồn tới chuẩn
                    cur.execute("""
                        INSERT INTO ods_category_mapping 
                        (source_category, standard_category, category_sk, confidence_score)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (source_category) DO UPDATE 
                        SET standard_category = EXCLUDED.standard_category,
                            category_sk = EXCLUDED.category_sk,
                            updated_at = CURRENT_TIMESTAMP
                    """, (source_cat, standard_cat, category_sk, 1.0))
                
                self.conn.commit()
                logger.info(f"✓ Load {len(CATEGORY_MAP)} danh mục ánh xạ thành công")
        except Exception as e:
            logger.error(f"✗ Lỗi load danh mục: {e}")
            self.conn.rollback()
            raise
    
    def apply_category_mapping_to_products(self):
        """Áp dụng ánh xạ danh mục vào sản phẩm"""
        try:
            with self.conn.cursor() as cur:
                # Kiểm tra category_sk column
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'ods_product_clean' 
                        AND column_name = 'category_sk'
                    )
                """)
                has_category_sk = cur.fetchone()[0]
                
                if not has_category_sk:
                    logger.info("➕ Thêm column category_sk vào bảng...")
                    cur.execute("""
                        ALTER TABLE ods_product_clean 
                        ADD COLUMN category_sk INT
                    """)
                    self.conn.commit()
                
                # Áp dụng ánh xạ danh mục
                logger.info(f"📝 Cập nhật category_sk sử dụng ID column: {self.id_column}")
                
                update_query = f"""
                    UPDATE ods_product_clean opc
                    SET category_sk = cm.category_sk
                    FROM ods_category_mapping cm
                    WHERE LOWER(TRIM(opc.category)) = LOWER(TRIM(cm.source_category))
                    AND cm.is_active = TRUE
                    AND (opc.category_sk IS NULL OR opc.category_sk = 0)
                """
                
                cur.execute(update_query)
                count = cur.rowcount
                self.conn.commit()
                logger.info(f"✓ Áp dụng danh mục cho {count} sản phẩm")
                
                # Kiểm tra sản phẩm chưa được ánh xạ
                cur.execute("""
                    SELECT COUNT(*) FROM ods_product_clean 
                    WHERE category_sk IS NULL AND category IS NOT NULL
                """)
                unmapped = cur.fetchone()[0]
                if unmapped > 0:
                    logger.warning(f"⚠ {unmapped} sản phẩm chưa được ánh xạ danh mục")
                    
                    # Log các danh mục không được ánh xạ
                    cur.execute("""
                        SELECT DISTINCT LOWER(TRIM(category)), COUNT(*) as count
                        FROM ods_product_clean 
                        WHERE category_sk IS NULL AND category IS NOT NULL
                        GROUP BY LOWER(TRIM(category))
                        ORDER BY count DESC
                        LIMIT 20
                    """)
                    logger.info("Danh mục chưa ánh xạ (Top 20):")
                    for row in cur.fetchall():
                        logger.info(f"  - {row[0]}: {row[1]} sản phẩm")
        except Exception as e:
            logger.error(f"✗ Lỗi áp dụng ánh xạ: {e}")
            self.conn.rollback()
            raise
    
    def validate_mapping(self):
        """Kiểm chứng kết quả ánh xạ"""
        try:
            with self.conn.cursor() as cur:
                # Thống kê tổng quát
                cur.execute("""
                    SELECT 
                        COUNT(DISTINCT category_sk) as total_categories,
                        COUNT(*) as total_mappings
                    FROM ods_category_mapping
                    WHERE is_active = TRUE
                """)
                total_cat, total_map = cur.fetchone()
                logger.info(f"✓ Tổng danh mục: {total_cat}, Tổng ánh xạ: {total_map}")
                
                # Kiểm tra danh mục tiêu
                cur.execute("""
                    SELECT COUNT(*) FROM dwh_dim_category
                """)
                total_dim = cur.fetchone()[0]
                logger.info(f"✓ Tổng danh mục chiều: {total_dim}")
                
                # Phân bố danh mục
                cur.execute("""
                    SELECT category_level, COUNT(*) as count
                    FROM dwh_dim_category
                    GROUP BY category_level
                    ORDER BY category_level
                """)
                logger.info("Phân bố theo cấp độ:")
                for level, count in cur.fetchall():
                    logger.info(f"  - Cấp {level}: {count} danh mục")
                
                # Thống kê ánh xạ sản phẩm
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_products,
                        SUM(CASE WHEN category_sk IS NOT NULL THEN 1 ELSE 0 END) as mapped_products,
                        SUM(CASE WHEN category_sk IS NULL THEN 1 ELSE 0 END) as unmapped_products
                    FROM ods_product_clean
                """)
                total_prod, mapped_prod, unmapped_prod = cur.fetchone()
                logger.info(f"\n✓ Thống kê ánh xạ sản phẩm:")
                logger.info(f"  - Tổng sản phẩm: {total_prod}")
                logger.info(f"  - Đã ánh xạ: {mapped_prod} ({100*mapped_prod//total_prod if total_prod else 0}%)")
                logger.info(f"  - Chưa ánh xạ: {unmapped_prod}")
        except Exception as e:
            logger.error(f"✗ Lỗi kiểm chứng: {e}")
            raise
    
    def export_mapping_report(self, output_path="category_mapping_report.json"):
        """Xuất báo cáo ánh xạ"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT 
                        cm.source_category,
                        cm.standard_category,
                        c.category_path,
                        cm.confidence_score,
                        COUNT(DISTINCT p.{self.id_column}) as product_count,
                        STRING_AGG(DISTINCT p.source_platform, ', ' ORDER BY p.source_platform) as platforms
                    FROM ods_category_mapping cm
                    LEFT JOIN dwh_dim_category c ON c.category_sk = cm.category_sk
                    LEFT JOIN ods_product_clean p ON p.category_sk = c.category_sk
                    WHERE cm.is_active = TRUE
                    GROUP BY cm.source_category, cm.standard_category, c.category_path, cm.confidence_score
                    ORDER BY product_count DESC
                """)
                
                results = cur.fetchall()
                report = {
                    'timestamp': datetime.now().isoformat(),
                    'database': self.db_config['database'],
                    'id_column': self.id_column,
                    'total_mappings': len(results),
                    'mappings': [
                        {
                            'source_category': row[0],
                            'standard_category': row[1],
                            'category_path': row[2],
                            'confidence_score': float(row[3]) if row[3] else 0,
                            'product_count': row[4] or 0,
                            'platforms': row[5].split(', ') if row[5] else []
                        }
                        for row in results
                    ]
                }
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                
                logger.info(f"✓ Xuất báo cáo tới {output_path}")
        except Exception as e:
            logger.error(f"✗ Lỗi xuất báo cáo: {e}")
            raise
    
    def run(self):
        """Chạy quy trình ánh xạ danh mục"""
        try:
            logger.info("=" * 70)
            logger.info("CATEGORY MAPPING - ADAPTIVE SCHEMA")
            logger.info("=" * 70)
            
            self.connect()
            
            # Kiểm tra schema trước tiên
            logger.info("\n📋 BƯỚC 1: Kiểm tra cấu trúc dữ liệu")
            logger.info("-" * 70)
            if not self.check_table_structure():
                logger.error("✗ Kiểm tra schema thất bại")
                return
            
            logger.info("\n📋 BƯỚC 2: Tạo bảng danh mục tiêu")
            logger.info("-" * 70)
            self.create_category_tables()
            
            logger.info("\n📋 BƯỚC 3: Load danh mục tiêu chuẩn")
            logger.info("-" * 70)
            self.load_standard_categories()
            
            logger.info("\n📋 BƯỚC 4: Áp dụng ánh xạ cho sản phẩm")
            logger.info("-" * 70)
            self.apply_category_mapping_to_products()
            
            logger.info("\n📋 BƯỚC 5: Kiểm chứng kết quả")
            logger.info("-" * 70)
            self.validate_mapping()
            
            logger.info("\n📋 BƯỚC 6: Xuất báo cáo")
            logger.info("-" * 70)
            self.export_mapping_report()
            
            logger.info("\n" + "=" * 70)
            logger.info("✓ HOÀN THÀNH THÀNH CÔNG!")
            logger.info("=" * 70)
        except Exception as e:
            logger.error(f"\n✗ THẤT BẠI: {e}")
            raise
        finally:
            self.close()

def main():
    mapper = CategoryMapper(DB_CONFIG)
    mapper.run()

if __name__ == "__main__":
    main()
