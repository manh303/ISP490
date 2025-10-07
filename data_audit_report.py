#!/usr/bin/env python3
"""
Data Audit Report Generator
Tạo báo cáo tổng hợp tình trạng dữ liệu trong dự án
"""

import os
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataAuditor:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.data_dir = self.project_root / "data"
        self.backup_dir = self.project_root / "data_backup"
        self.report_file = self.project_root / f"DATA_AUDIT_REPORT_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    def check_data_warehouse(self):
        """Kiểm tra Data Warehouse"""
        logger.info("🏪 Kiểm tra Data Warehouse...")

        warehouse_info = {
            'status': 'EMPTY',
            'location': 'data_backup/warehouse/',
            'structure': {},
            'total_files': 0,
            'total_size_mb': 0
        }

        warehouse_path = self.backup_dir / "warehouse"
        if warehouse_path.exists():
            # Kiểm tra cấu trúc thư mục
            subdirs = [d.name for d in warehouse_path.iterdir() if d.is_dir()]
            warehouse_info['structure'] = {
                'subdirectories': subdirs,
                'details': {}
            }

            total_files = 0
            total_size = 0

            for subdir in subdirs:
                subdir_path = warehouse_path / subdir
                files = list(subdir_path.glob("*"))
                file_count = len(files)

                subdir_size = sum(f.stat().st_size for f in files if f.is_file())

                warehouse_info['structure']['details'][subdir] = {
                    'file_count': file_count,
                    'size_mb': round(subdir_size / (1024*1024), 2),
                    'files': [f.name for f in files if f.is_file()]
                }

                total_files += file_count
                total_size += subdir_size

            warehouse_info['total_files'] = total_files
            warehouse_info['total_size_mb'] = round(total_size / (1024*1024), 2)
            warehouse_info['status'] = 'EXISTS_BUT_EMPTY' if total_files == 0 else 'HAS_DATA'

        return warehouse_info

    def check_data_lake(self):
        """Kiểm tra Data Lake"""
        logger.info("🏞️ Kiểm tra Data Lake...")

        lake_info = {
            'status': 'EMPTY',
            'location': 'data_backup/lake/',
            'structure': {},
            'total_files': 0,
            'total_size_mb': 0
        }

        lake_path = self.backup_dir / "lake"
        if lake_path.exists():
            # Kiểm tra cấu trúc layers
            layers = [d.name for d in lake_path.iterdir() if d.is_dir()]
            lake_info['structure'] = {
                'layers': layers,
                'details': {}
            }

            total_files = 0
            total_size = 0

            for layer in layers:
                layer_path = lake_path / layer
                files = list(layer_path.glob("*"))
                file_count = len(files)

                layer_size = sum(f.stat().st_size for f in files if f.is_file())

                lake_info['structure']['details'][layer] = {
                    'file_count': file_count,
                    'size_mb': round(layer_size / (1024*1024), 2),
                    'files': [f.name for f in files if f.is_file()]
                }

                total_files += file_count
                total_size += layer_size

            lake_info['total_files'] = total_files
            lake_info['total_size_mb'] = round(total_size / (1024*1024), 2)
            lake_info['status'] = 'EXISTS_BUT_EMPTY' if total_files == 0 else 'HAS_DATA'

        return lake_info

    def check_current_data(self):
        """Kiểm tra dữ liệu hiện tại"""
        logger.info("📊 Kiểm tra dữ liệu hiện tại...")

        current_data = {
            'electronics_clean': {},
            'remaining_directories': {},
            'total_csv_files': 0,
            'total_data_size_mb': 0
        }

        # Kiểm tra vietnam_electronics_clean
        clean_path = self.data_dir / "vietnam_electronics_clean"
        if clean_path.exists():
            files = list(clean_path.glob("*.csv"))
            json_files = list(clean_path.glob("*.json"))

            # Phân tích file master
            master_files = [f for f in files if 'master' in f.name]
            master_info = {}

            if master_files:
                master_file = master_files[0]
                try:
                    df = pd.read_csv(master_file, nrows=1)  # Đọc header
                    file_size = master_file.stat().st_size

                    # Đếm tổng số dòng
                    with open(master_file, 'r', encoding='utf-8') as f:
                        total_lines = sum(1 for line in f) - 1  # Trừ header

                    master_info = {
                        'file_name': master_file.name,
                        'total_products': total_lines,
                        'columns': list(df.columns),
                        'column_count': len(df.columns),
                        'file_size_mb': round(file_size / (1024*1024), 2)
                    }
                except Exception as e:
                    master_info = {'error': str(e)}

            current_data['electronics_clean'] = {
                'status': 'EXISTS',
                'csv_files': len(files),
                'json_files': len(json_files),
                'master_file_info': master_info,
                'category_files': [f.name for f in files if 'master' not in f.name][:10]  # First 10
            }

        # Kiểm tra các thư mục còn lại
        remaining_dirs = ['electronics', 'consolidated', 'expanded_real', 'integrated', 'real_datasets', 'vietnamese_ecommerce']

        for dir_name in remaining_dirs:
            dir_path = self.data_dir / dir_name
            if dir_path.exists():
                csv_files = list(dir_path.glob("*.csv"))
                json_files = list(dir_path.glob("*.json"))

                dir_size = sum(f.stat().st_size for f in dir_path.glob("*") if f.is_file())

                current_data['remaining_directories'][dir_name] = {
                    'csv_files': len(csv_files),
                    'json_files': len(json_files),
                    'size_mb': round(dir_size / (1024*1024), 2),
                    'sample_files': [f.name for f in csv_files[:5]]  # First 5
                }

        # Tổng số file CSV
        total_csv = len(list(self.data_dir.rglob("*.csv")))

        # Tổng kích thước
        total_size = sum(f.stat().st_size for f in self.data_dir.rglob("*") if f.is_file())

        current_data['total_csv_files'] = total_csv
        current_data['total_data_size_mb'] = round(total_size / (1024*1024), 2)

        return current_data

    def check_collection_capability(self):
        """Kiểm tra khả năng thu thập dữ liệu"""
        logger.info("🔍 Kiểm tra khả năng thu thập...")

        collection_info = {
            'collector_script': {
                'exists': False,
                'location': 'vietnam_electronics_collector.py',
                'features': []
            },
            'fresh_data_directory': {
                'exists': False,
                'location': 'data/vietnam_electronics_fresh/',
                'files': []
            },
            'dependencies': {
                'required_packages': ['requests', 'pandas', 'fake-useragent'],
                'status': 'unknown'
            }
        }

        # Kiểm tra collector script
        collector_path = self.project_root / "vietnam_electronics_collector.py"
        if collector_path.exists():
            collection_info['collector_script']['exists'] = True
            collection_info['collector_script']['features'] = [
                'Tiki API integration',
                'DummyJSON API',
                'FakeStore API',
                'Synthetic Vietnam data generation',
                'Multiple platform support'
            ]

        # Kiểm tra fresh data directory
        fresh_path = self.data_dir / "vietnam_electronics_fresh"
        if fresh_path.exists():
            collection_info['fresh_data_directory']['exists'] = True
            files = list(fresh_path.glob("*"))
            collection_info['fresh_data_directory']['files'] = [f.name for f in files]

        return collection_info

    def generate_recommendations(self, warehouse_info, lake_info, current_data, collection_info):
        """Tạo khuyến nghị"""
        recommendations = []

        # Data Warehouse recommendations
        if warehouse_info['status'] == 'EXISTS_BUT_EMPTY':
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Data Warehouse',
                'issue': 'Data Warehouse rỗng',
                'recommendation': 'Thiết lập pipeline ETL để load dữ liệu điện tử VN vào warehouse',
                'action': 'Tạo schema và tables cho sản phẩm điện tử, sau đó load từ vietnam_electronics_clean'
            })

        # Data Lake recommendations
        if lake_info['status'] == 'EXISTS_BUT_EMPTY':
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Data Lake',
                'issue': 'Data Lake rỗng',
                'recommendation': 'Tổ chức dữ liệu theo layers: raw → processed → analytics',
                'action': 'Di chuyển dữ liệu điện tử vào lake structure với proper partitioning'
            })

        # Current data recommendations
        if current_data.get('electronics_clean', {}).get('status') == 'EXISTS':
            master_info = current_data['electronics_clean'].get('master_file_info', {})
            if master_info.get('total_products', 0) < 1000:
                recommendations.append({
                    'priority': 'HIGH',
                    'category': 'Data Collection',
                    'issue': f'Chỉ có {master_info.get("total_products", 0)} sản phẩm điện tử',
                    'recommendation': 'Thu thập thêm dữ liệu từ các platform Việt Nam',
                    'action': 'Chạy vietnam_electronics_collector.py để thu thập thêm dữ liệu'
                })

        # Collection capability recommendations
        if not collection_info['fresh_data_directory']['exists']:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Data Collection',
                'issue': 'Chưa có fresh data directory',
                'recommendation': 'Chạy collector để tạo dữ liệu mới',
                'action': 'python vietnam_electronics_collector.py'
            })

        return recommendations

    def generate_report(self):
        """Tạo báo cáo tổng hợp"""
        logger.info("📋 Tạo báo cáo tổng hợp...")

        # Thu thập thông tin
        warehouse_info = self.check_data_warehouse()
        lake_info = self.check_data_lake()
        current_data = self.check_current_data()
        collection_info = self.check_collection_capability()
        recommendations = self.generate_recommendations(warehouse_info, lake_info, current_data, collection_info)

        # Tạo báo cáo Markdown
        report_content = f"""# 📊 BÁO CÁO KIỂM TRA DỮ LIỆU DỰ ÁN

**Ngày tạo:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
**Dự án:** Vietnam Electronics E-commerce DSS
**Phạm vi:** Dữ liệu sản phẩm điện tử TMĐT Việt Nam

---

## 🏪 DATA WAREHOUSE

**Trạng thái:** `{warehouse_info['status']}`
**Vị trí:** `{warehouse_info['location']}`
**Tổng files:** {warehouse_info['total_files']}
**Kích thước:** {warehouse_info['total_size_mb']} MB

### Cấu trúc:
"""

        # Data Warehouse structure
        if warehouse_info['structure'].get('subdirectories'):
            for subdir, details in warehouse_info['structure']['details'].items():
                report_content += f"- **{subdir}:** {details['file_count']} files ({details['size_mb']} MB)\n"
        else:
            report_content += "- Không có cấu trúc dữ liệu\n"

        report_content += f"""
---

## 🏞️ DATA LAKE

**Trạng thái:** `{lake_info['status']}`
**Vị trí:** `{lake_info['location']}`
**Tổng files:** {lake_info['total_files']}
**Kích thước:** {lake_info['total_size_mb']} MB

### Layers:
"""

        # Data Lake layers
        if lake_info['structure'].get('layers'):
            for layer, details in lake_info['structure']['details'].items():
                report_content += f"- **{layer}:** {details['file_count']} files ({details['size_mb']} MB)\n"
        else:
            report_content += "- Không có layer structure\n"

        report_content += f"""
---

## 📱 DỮ LIỆU ĐIỆN TỬ HIỆN TẠI

### Vietnam Electronics Clean Data:
"""

        # Current electronics data
        if current_data.get('electronics_clean', {}).get('status') == 'EXISTS':
            electronics = current_data['electronics_clean']
            master = electronics.get('master_file_info', {})

            report_content += f"""
**Master File:** `{master.get('file_name', 'N/A')}`
**Tổng sản phẩm:** {master.get('total_products', 0):,}
**Số cột:** {master.get('column_count', 0)}
**Kích thước:** {master.get('file_size_mb', 0)} MB
**Category files:** {electronics.get('csv_files', 0) - 1}

#### Các cột dữ liệu:
"""
            if master.get('columns'):
                for i, col in enumerate(master['columns'][:10], 1):  # First 10 columns
                    report_content += f"{i}. `{col}`\n"
                if len(master['columns']) > 10:
                    report_content += f"... và {len(master['columns']) - 10} cột khác\n"
        else:
            report_content += "- Không có dữ liệu electronics clean\n"

        report_content += f"""
### Thư mục dữ liệu khác:
"""

        # Other directories
        for dir_name, info in current_data.get('remaining_directories', {}).items():
            report_content += f"- **{dir_name}:** {info['csv_files']} CSV files ({info['size_mb']} MB)\n"

        report_content += f"""
**Tổng CSV files:** {current_data.get('total_csv_files', 0)}
**Tổng kích thước dữ liệu:** {current_data.get('total_data_size_mb', 0)} MB

---

## 🔄 KHẢ NĂNG THU THẬP DỮ LIỆU

### Collector Script:
**Trạng thái:** {'✅ Có sẵn' if collection_info['collector_script']['exists'] else '❌ Chưa có'}
**File:** `{collection_info['collector_script']['location']}`

#### Tính năng:
"""

        for feature in collection_info['collector_script']['features']:
            report_content += f"- {feature}\n"

        report_content += f"""
### Fresh Data Directory:
**Trạng thái:** {'✅ Đã tạo' if collection_info['fresh_data_directory']['exists'] else '❌ Chưa tạo'}
**Vị trí:** `{collection_info['fresh_data_directory']['location']}`
**Files:** {len(collection_info['fresh_data_directory']['files'])}

---

## 🎯 KHUYẾN NGHỊ

"""

        # Recommendations
        for i, rec in enumerate(recommendations, 1):
            priority_emoji = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟢'}.get(rec['priority'], '⚪')
            report_content += f"""
### {i}. {priority_emoji} {rec['category']} - {rec['priority']} Priority

**Vấn đề:** {rec['issue']}
**Khuyến nghị:** {rec['recommendation']}
**Hành động:** `{rec['action']}`

"""

        report_content += f"""---

## 📈 TÓM TẮT TỔNG QUAN

| Thành phần | Trạng thái | Kích thước | Ghi chú |
|------------|------------|------------|---------|
| Data Warehouse | {warehouse_info['status']} | {warehouse_info['total_size_mb']} MB | {warehouse_info['total_files']} files |
| Data Lake | {lake_info['status']} | {lake_info['total_size_mb']} MB | {lake_info['total_files']} files |
| Electronics Data | {'✅ ACTIVE' if current_data.get('electronics_clean', {}).get('status') == 'EXISTS' else '❌ MISSING'} | {current_data.get('total_data_size_mb', 0)} MB | {current_data.get('total_csv_files', 0)} CSV files |
| Collector | {'✅ READY' if collection_info['collector_script']['exists'] else '❌ MISSING'} | - | Vietnam platforms support |

---

## 🚀 HÀNH ĐỘNG TIẾP THEO

1. **Nếu cần thu thập dữ liệu mới:**
   ```bash
   pip install requests pandas fake-useragent
   python vietnam_electronics_collector.py
   ```

2. **Nếu cần setup Data Warehouse:**
   - Tạo schema PostgreSQL cho electronics
   - Load dữ liệu từ vietnam_electronics_clean
   - Setup ETL pipeline

3. **Nếu cần setup Data Lake:**
   - Tổ chức dữ liệu theo layers (raw/processed/analytics)
   - Implement data partitioning
   - Setup metadata catalog

---

*Báo cáo được tạo tự động bởi Data Auditor*
*Dự án: Vietnam Electronics E-commerce DSS*
"""

        # Lưu báo cáo
        with open(self.report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)

        logger.info(f"📋 Báo cáo đã lưu: {self.report_file}")

        return {
            'report_file': str(self.report_file),
            'warehouse_info': warehouse_info,
            'lake_info': lake_info,
            'current_data': current_data,
            'collection_info': collection_info,
            'recommendations': recommendations
        }

if __name__ == "__main__":
    auditor = DataAuditor()

    print("🔍 VIETNAM ELECTRONICS DATA AUDIT")
    print("=" * 50)

    result = auditor.generate_report()

    print("✅ AUDIT HOÀN TẤT!")
    print(f"📋 Báo cáo: {result['report_file']}")
    print(f"📊 Data Warehouse: {result['warehouse_info']['status']}")
    print(f"🏞️ Data Lake: {result['lake_info']['status']}")
    print(f"📱 Electronics Data: {result['current_data'].get('electronics_clean', {}).get('status', 'MISSING')}")
    print(f"🎯 Khuyến nghị: {len(result['recommendations'])} items")
    print("=" * 50)