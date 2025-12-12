#!/usr/bin/env python3
"""
Test Category Mapping functionality
"""
import pytest
import psycopg2
from src.standardization.category_mapping import CategoryMapper, CATEGORY_MAP

@pytest.fixture
def db_config():
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'ecommerce_dss',
        'user': 'postgres',
        'password': 'postgres123'
    }

@pytest.fixture
def mapper(db_config):
    mapper = CategoryMapper(db_config)
    mapper.connect()
    yield mapper
    mapper.close()

class TestCategoryMapping:
    
    def test_connection(self, mapper):
        """Kiểm tra kết nối database"""
        assert mapper.conn is not None
    
    def test_create_tables(self, mapper):
        """Kiểm tra tạo bảng"""
        mapper.create_category_tables()
        
        with mapper.conn.cursor() as cur:
            # Kiểm tra bảng dimension
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'dwh_dim_category'
                )
            """)
            assert cur.fetchone()[0] == True
            
            # Kiểm tra bảng mapping
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'ods_category_mapping'
                )
            """)
            assert cur.fetchone()[0] == True
    
    def test_load_categories(self, mapper):
        """Kiểm tra load danh mục"""
        mapper.create_category_tables()
        mapper.load_standard_categories()
        
        with mapper.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ods_category_mapping WHERE is_active = TRUE")
            count = cur.fetchone()[0]
            assert count == len(CATEGORY_MAP)
    
    def test_category_hierarchy(self, mapper):
        """Kiểm tra phân cấp danh mục"""
        mapper.create_category_tables()
        mapper.load_standard_categories()
        
        with mapper.conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(category_level) FROM dwh_dim_category
            """)
            max_level = cur.fetchone()[0]
            assert max_level >= 3  # Ít nhất 3 cấp (Electronics > Type > Subtype)
    
    def test_category_mapping_structure(self, mapper):
        """Kiểm tra cấu trúc ánh xạ"""
        with mapper.conn.cursor() as cur:
            for source, target in CATEGORY_MAP.items():
                parts = target.split('|')
                assert len(parts) >= 2, f"Danh mục {source} thiếu cấp độ"
                for part in parts:
                    assert len(part.strip()) > 0, f"Phần danh mục trống: {target}"
    
    def test_export_report(self, mapper, tmp_path):
        """Kiểm tra xuất báo cáo"""
        mapper.create_category_tables()
        mapper.load_standard_categories()
        
        report_path = tmp_path / "test_report.json"
        mapper.export_mapping_report(str(report_path))
        
        assert report_path.exists()
        
        import json
        with open(report_path) as f:
            report = json.load(f)
        
        assert 'timestamp' in report
        assert 'total_mappings' in report
        assert 'mappings' in report
        assert len(report['mappings']) == len(CATEGORY_MAP)

class TestCategoryValidation:
    
    def test_category_map_keys(self):
        """Kiểm tra keys của category map"""
        for key in CATEGORY_MAP.keys():
            assert isinstance(key, str)
            assert len(key.strip()) > 0
    
    def test_category_map_values(self):
        """Kiểm tra values của category map"""
        for value in CATEGORY_MAP.values():
            assert isinstance(value, str)
            parts = value.split('|')
            assert len(parts) >= 2
            for part in parts:
                assert len(part.strip()) > 0
    
    def test_unique_categories(self):
        """Kiểm tra danh mục nguồn duy nhất"""
        keys = list(CATEGORY_MAP.keys())
        assert len(keys) == len(set(keys))

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
