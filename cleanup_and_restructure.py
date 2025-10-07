#!/usr/bin/env python3
"""
Script dọn dẹp và tái cấu trúc dự án chỉ tập trung vào
DỮ LIỆU SẢN PHẨM ĐIỆN TỬ TMĐT VIỆT NAM
"""

import os
import shutil
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VietnamElectronicsDataCleaner:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.data_dir = self.project_root / "data"
        self.backup_dir = self.project_root / "data_backup"
        self.cleaned_dir = self.data_dir / "vietnam_electronics_clean"

        # Các file cần GIỮ LẠI (chỉ liên quan đến điện tử VN)
        self.files_to_keep = [
            # Dữ liệu điện tử
            "data/electronics/electronics_*.csv",
            "data/electronics/electronics_analysis_*.json",

            # Dữ liệu Vietnam consolidated (chỉ products)
            "data/consolidated/vietnam_products_consolidated_*.csv",

            # Dữ liệu Vietnam integrated (chỉ products)
            "data/integrated/vietnamese_products_integrated_*.csv",

            # Dữ liệu điện tử expanded
            "data/expanded_real/dummyjson_expanded_electronics.csv",
            "data/expanded_real/platzi_electronics.csv",

            # Dữ liệu real datasets
            "data/real_datasets/dummyjson_electronics.csv",
            "data/real_datasets/kaggle_electronics_filtered.csv",
        ]

        # Thư mục cần XÓA HOÀN TOÀN
        self.dirs_to_remove = [
            "data/raw",
            "data/clean",
            "data/backups",
            "data/lake",
            "data/logs",
            "data/models",
            "data/mongo_backups",
            "data/warehouse",
            "data/vietnam_only"
        ]

        # File đơn lẻ cần XÓA
        self.files_to_remove = [
            "data/customers.csv",
            "data/consolidated/vietnam_customers_consolidated_*.csv",
            "data/consolidated/vietnam_orders_consolidated_*.csv",
            "data/consolidated/vietnam_market_insights_*.json",
            "data/integrated/vietnamese_customers_integrated_*.csv",
            "data/integrated/vietnamese_orders_integrated_*.csv",
            "data/integrated/vietnamese_transactions_integrated_*.csv"
        ]

    def create_backup(self):
        """Tạo backup toàn bộ dữ liệu hiện tại"""
        logger.info("🛡️ Tạo backup dữ liệu...")

        if self.backup_dir.exists():
            shutil.rmtree(self.backup_dir)

        shutil.copytree(self.data_dir, self.backup_dir)
        logger.info(f"✅ Backup hoàn tất tại: {self.backup_dir}")

    def clean_irrelevant_data(self):
        """Xóa các dữ liệu không liên quan đến điện tử VN"""
        logger.info("🧹 Bắt đầu dọn dẹp dữ liệu không liên quan...")

        # Xóa thư mục
        for dir_pattern in self.dirs_to_remove:
            dir_path = self.project_root / dir_pattern
            if dir_path.exists():
                shutil.rmtree(dir_path)
                logger.info(f"🗑️ Đã xóa thư mục: {dir_path}")

        # Xóa file đơn lẻ
        for file_pattern in self.files_to_remove:
            import glob
            files = glob.glob(str(self.project_root / file_pattern))
            for file in files:
                if os.path.exists(file):
                    os.remove(file)
                    logger.info(f"🗑️ Đã xóa file: {file}")

    def restructure_vietnam_electronics_data(self):
        """Tái cấu trúc dữ liệu điện tử Việt Nam"""
        logger.info("🔄 Tái cấu trúc dữ liệu điện tử Việt Nam...")

        # Tạo thư mục mới
        self.cleaned_dir.mkdir(exist_ok=True, parents=True)

        # Thu thập tất cả dữ liệu điện tử
        electronics_data = []

        # 1. Từ thư mục electronics
        electronics_dir = self.data_dir / "electronics"
        if electronics_dir.exists():
            for csv_file in electronics_dir.glob("electronics_*.csv"):
                try:
                    df = pd.read_csv(csv_file)
                    df['source_file'] = csv_file.name
                    df['data_category'] = csv_file.stem.replace('electronics_', '').replace('_20251001_213710', '')
                    electronics_data.append(df)
                    logger.info(f"📊 Đã đọc: {csv_file.name} - {len(df)} records")
                except Exception as e:
                    logger.error(f"❌ Lỗi đọc {csv_file}: {e}")

        # 2. Từ consolidated vietnam products
        consolidated_files = list(self.data_dir.glob("consolidated/vietnam_products_consolidated_*.csv"))
        for csv_file in consolidated_files:
            try:
                df = pd.read_csv(csv_file)
                # Chỉ lấy sản phẩm điện tử
                if 'category' in df.columns:
                    df = df[df['category'].str.contains('Electronics|electronics', case=False, na=False)]
                df['source_file'] = csv_file.name
                df['data_category'] = 'vietnam_consolidated'
                electronics_data.append(df)
                logger.info(f"📊 Đã đọc: {csv_file.name} - {len(df)} records điện tử")
            except Exception as e:
                logger.error(f"❌ Lỗi đọc {csv_file}: {e}")

        # 3. Từ expanded real
        expanded_files = [
            "expanded_real/dummyjson_expanded_electronics.csv",
            "expanded_real/platzi_electronics.csv"
        ]
        for file_path in expanded_files:
            full_path = self.data_dir / file_path
            if full_path.exists():
                try:
                    df = pd.read_csv(full_path)
                    df['source_file'] = full_path.name
                    df['data_category'] = full_path.stem
                    electronics_data.append(df)
                    logger.info(f"📊 Đã đọc: {full_path.name} - {len(df)} records")
                except Exception as e:
                    logger.error(f"❌ Lỗi đọc {full_path}: {e}")

        if electronics_data:
            # Gộp tất cả dữ liệu
            combined_df = pd.concat(electronics_data, ignore_index=True, sort=False)

            # Làm sạch và chuẩn hóa
            combined_df = self._clean_electronics_data(combined_df)

            # Lưu file master
            output_file = self.cleaned_dir / f"vietnam_electronics_master_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            combined_df.to_csv(output_file, index=False)
            logger.info(f"💾 Đã lưu file master: {output_file} - {len(combined_df)} records")

            # Tạo breakdown theo category
            self._create_category_breakdown(combined_df)

            # Tạo summary report
            self._create_summary_report(combined_df)

            return combined_df
        else:
            logger.warning("⚠️ Không tìm thấy dữ liệu điện tử nào!")
            return None

    def _clean_electronics_data(self, df):
        """Làm sạch và chuẩn hóa dữ liệu điện tử"""
        logger.info("🧽 Làm sạch dữ liệu điện tử...")

        # Chuẩn hóa tên cột
        column_mapping = {
            'product_name': 'name',
            'product_id': 'id',
            'price_vnd': 'price_vnd',
            'price_usd': 'price_usd',
            'subcategory': 'subcategory',
            'main_category': 'main_category'
        }

        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and new_col not in df.columns:
                df = df.rename(columns={old_col: new_col})

        # Đảm bảo các cột bắt buộc
        required_cols = ['name', 'brand', 'category', 'price_vnd']
        for col in required_cols:
            if col not in df.columns:
                df[col] = 'Unknown'

        # Lọc chỉ sản phẩm điện tử
        electronics_keywords = [
            'smartphone', 'phone', 'mobile', 'tablet', 'laptop', 'computer',
            'headphone', 'speaker', 'camera', 'electronics', 'audio', 'video',
            'gaming', 'smartwatch', 'earphone', 'charger', 'iphone', 'samsung',
            'xiaomi', 'oppo', 'vivo', 'huawei', 'apple'
        ]

        # Lọc theo tên sản phẩm hoặc category
        mask = False
        for keyword in electronics_keywords:
            mask |= df['name'].str.contains(keyword, case=False, na=False)
            if 'category' in df.columns:
                mask |= df['category'].str.contains(keyword, case=False, na=False)
            if 'subcategory' in df.columns:
                mask |= df['subcategory'].str.contains(keyword, case=False, na=False)

        df = df[mask]

        # Xóa duplicates
        before_count = len(df)
        df = df.drop_duplicates(subset=['name', 'brand'], keep='first')
        after_count = len(df)

        logger.info(f"✅ Làm sạch hoàn tất: {before_count} → {after_count} records")

        return df

    def _create_category_breakdown(self, df):
        """Tạo breakdown theo từng category"""
        logger.info("📂 Tạo breakdown theo category...")

        # Breakdown theo subcategory
        if 'subcategory' in df.columns:
            for subcategory in df['subcategory'].unique():
                if pd.notna(subcategory) and subcategory != 'Unknown':
                    subset = df[df['subcategory'] == subcategory]
                    if len(subset) > 0:
                        filename = f"vietnam_electronics_{subcategory.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv"
                        output_path = self.cleaned_dir / filename
                        subset.to_csv(output_path, index=False)
                        logger.info(f"💾 {subcategory}: {len(subset)} records → {filename}")

    def _create_summary_report(self, df):
        """Tạo báo cáo tổng hợp"""
        logger.info("📊 Tạo báo cáo tổng hợp...")

        summary = {
            'report_generated': datetime.now().isoformat(),
            'total_products': len(df),
            'unique_brands': df['brand'].nunique() if 'brand' in df.columns else 0,
            'categories': {},
            'price_analysis': {},
            'data_sources': {}
        }

        # Phân tích theo category
        if 'subcategory' in df.columns:
            summary['categories'] = df['subcategory'].value_counts().to_dict()

        # Phân tích giá
        if 'price_vnd' in df.columns:
            price_col = pd.to_numeric(df['price_vnd'], errors='coerce')
            summary['price_analysis'] = {
                'avg_price_vnd': float(price_col.mean()) if not price_col.isna().all() else 0,
                'min_price_vnd': float(price_col.min()) if not price_col.isna().all() else 0,
                'max_price_vnd': float(price_col.max()) if not price_col.isna().all() else 0
            }

        # Phân tích nguồn dữ liệu
        if 'data_category' in df.columns:
            summary['data_sources'] = df['data_category'].value_counts().to_dict()

        # Lưu báo cáo
        report_file = self.cleaned_dir / f"vietnam_electronics_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info(f"📋 Báo cáo tổng hợp: {report_file}")
        return summary

    def update_dags_for_electronics_only(self):
        """Cập nhật các DAG chỉ xử lý dữ liệu điện tử VN"""
        logger.info("🔧 Cập nhật DAG cho dữ liệu điện tử VN...")

        # Tạo DAG mới chuyên biệt
        dag_content = '''#!/usr/bin/env python3
"""
Vietnam Electronics E-commerce Data Pipeline
Chuyên xử lý dữ liệu sản phẩm điện tử TMĐT Việt Nam
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Configuration cho điện tử VN
VIETNAM_ELECTRONICS_PLATFORMS = {
    'shopee': {
        'name': 'Shopee Vietnam Electronics',
        'categories': ['smartphones', 'laptops', 'tablets', 'audio', 'gaming'],
        'api_endpoint': 'https://shopee.vn/api/v4/search/search_items?keyword=electronics'
    },
    'lazada': {
        'name': 'Lazada Vietnam Electronics',
        'categories': ['mobile-phones', 'computers', 'audio', 'cameras'],
        'api_endpoint': 'https://www.lazada.vn/catalog/?q=electronics'
    },
    'tiki': {
        'name': 'Tiki Electronics',
        'categories': ['dien-thoai-smartphone', 'laptop', 'tablet', 'am-thanh'],
        'api_endpoint': 'https://tiki.vn/api/v2/products?category=electronics'
    },
    'fptshop': {
        'name': 'FPT Shop Electronics',
        'categories': ['dien-thoai', 'laptop', 'tablet', 'am-thanh'],
        'api_endpoint': 'https://fptshop.com.vn/api/product?category=electronics'
    }
}

def collect_vietnam_electronics_data(**context):
    """Thu thập dữ liệu điện tử từ các platform VN"""
    import pandas as pd
    from datetime import datetime

    logger.info("🇻🇳 Thu thập dữ liệu điện tử Việt Nam...")

    # Implement collection logic here
    collected_data = []

    for platform, config in VIETNAM_ELECTRONICS_PLATFORMS.items():
        logger.info(f"📱 Thu thập từ {config['name']}...")
        # TODO: Implement actual collection logic

    return {"collected_count": len(collected_data)}

# DAG definition
dag = DAG(
    'vietnam_electronics_pipeline',
    default_args={
        'owner': 'vietnam-electronics-team',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    },
    description='Pipeline chuyên xử lý dữ liệu điện tử TMĐT Việt Nam',
    schedule_interval='0 */6 * * *',  # Mỗi 6 giờ
    catchup=False,
    tags=['vietnam', 'electronics', 'ecommerce']
)

# Tasks
collect_task = PythonOperator(
    task_id='collect_vietnam_electronics',
    python_callable=collect_vietnam_electronics_data,
    dag=dag
)
'''

        # Lưu DAG mới
        dag_file = self.project_root / "airflow" / "dags" / "vietnam_electronics_pipeline.py"
        dag_file.parent.mkdir(parents=True, exist_ok=True)

        with open(dag_file, 'w', encoding='utf-8') as f:
            f.write(dag_content)

        logger.info(f"📝 Đã tạo DAG mới: {dag_file}")

    def run_cleanup(self):
        """Chạy toàn bộ quá trình dọn dẹp"""
        logger.info("🚀 Bắt đầu dọn dẹp và tái cấu trúc dự án...")

        try:
            # 1. Backup
            self.create_backup()

            # 2. Dọn dẹp
            self.clean_irrelevant_data()

            # 3. Tái cấu trúc
            combined_df = self.restructure_vietnam_electronics_data()

            # 4. Cập nhật DAGs
            self.update_dags_for_electronics_only()

            logger.info("✅ Hoàn tất dọn dẹp và tái cấu trúc!")

            if combined_df is not None:
                logger.info(f"""
🎯 KẾT QUẢ CUỐI CÙNG:
- Tổng sản phẩm điện tử: {len(combined_df):,}
- Brands: {combined_df['brand'].nunique() if 'brand' in combined_df.columns else 'N/A'}
- Categories: {combined_df['subcategory'].nunique() if 'subcategory' in combined_df.columns else 'N/A'}
- Dữ liệu đã lưu tại: {self.cleaned_dir}
- Backup tại: {self.backup_dir}
                """)

            return True

        except Exception as e:
            logger.error(f"❌ Lỗi trong quá trình dọn dẹp: {e}")
            return False

if __name__ == "__main__":
    cleaner = VietnamElectronicsDataCleaner()
    success = cleaner.run_cleanup()

    if success:
        print("\n" + "="*60)
        print("🎉 DỌN DẸP HOÀN TẤT!")
        print("="*60)
        print("✅ Dự án đã được tái cấu trúc chỉ tập trung vào:")
        print("   📱 SẢN PHẨM ĐIỆN TỬ TMĐT VIỆT NAM")
        print("\n📂 Dữ liệu sạch tại: data/vietnam_electronics_clean/")
        print("🛡️ Backup tại: data_backup/")
        print("🔧 DAG mới: airflow/dags/vietnam_electronics_pipeline.py")
        print("="*60)
    else:
        print("❌ Có lỗi xảy ra trong quá trình dọn dẹp!")