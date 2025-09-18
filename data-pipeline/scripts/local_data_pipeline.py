# File: scripts/local_data_pipeline.py
#!/usr/bin/env python3

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import requests
import json
from loguru import logger

# Tạo thư mục data nếu chưa có
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

class LocalDataGenerator:
    def __init__(self):
        self.fake = Faker(['vi_VN', 'en_US'])  # Hỗ trợ tiếng Việt
        self.data_dir = DATA_DIR
        
    def generate_customers(self, count=100000):
        """Tạo dữ liệu khách hàng và lưu CSV"""
        logger.info(f"Tạo {count:,} khách hàng...")
        
        customers = []
        for i in range(count):
            customers.append({
                'customer_id': f'CUST_{i:08d}',
                'customer_name': self.fake.name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'address': self.fake.address(),
                'city': self.fake.city(),
                'country': random.choice(['Vietnam', 'USA', 'Japan', 'Korea', 'Singapore']),
                'registration_date': self.fake.date_between(start_date='-2y', end_date='today'),
                'customer_segment': random.choice(['VIP', 'Regular', 'Occasional', 'New']),
                'lifetime_value': round(random.uniform(50, 5000), 2),
                'age': random.randint(18, 70),
                'gender': random.choice(['Male', 'Female', 'Other']),
                'created_at': datetime.now()
            })
        
        df = pd.DataFrame(customers)
        csv_path = self.data_dir / 'customers.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.success(f"Đã lưu {len(df):,} khách hàng vào: {csv_path}")
        return df
    
    def generate_products(self, count=50000):
        """Tạo dữ liệu sản phẩm và lưu CSV"""
        logger.info(f"Tạo {count:,} sản phẩm...")
        
        categories = {
            'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphone', 'Camera'],
            'Clothing': ['Áo thun', 'Quần jean', 'Váy', 'Áo khoác', 'Giày'],
            'Food': ['Bánh kẹo', 'Đồ uống', 'Thực phẩm khô', 'Rau củ', 'Hoa quả'],
            'Books': ['Tiểu thuyết', 'Sách kỹ năng', 'Sách học', 'Truyện tranh'],
            'Home': ['Đồ nội thất', 'Đồ trang trí', 'Dụng cụ nhà bếp']
        }
        
        products = []
        for i in range(count):
            category = random.choice(list(categories.keys()))
            subcategory = random.choice(categories[category])
            
            base_price = random.uniform(10, 2000)
            products.append({
                'product_id': f'PROD_{i:08d}',
                'product_name': f'{subcategory} {i}',
                'category': category,
                'subcategory': subcategory,
                'brand': f'Brand_{random.randint(1, 200)}',
                'price': round(base_price, 2),
                'cost': round(base_price * random.uniform(0.3, 0.7), 2),
                'discount_percent': random.randint(0, 50),
                'stock_quantity': random.randint(0, 1000),
                'rating': round(random.uniform(1, 5), 1),
                'num_reviews': random.randint(0, 5000),
                'weight_kg': round(random.uniform(0.1, 10), 2),
                'created_date': self.fake.date_between(start_date='-3y', end_date='today'),
                'is_active': random.choice([True, False]),
                'created_at': datetime.now()
            })
        
        df = pd.DataFrame(products)
        csv_path = self.data_dir / 'products.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.success(f"Đã lưu {len(df):,} sản phẩm vào: {csv_path}")
        return df
    
    def generate_transactions(self, customers_df, products_df, batch_size=10000):
        """Tạo dữ liệu giao dịch theo batch để tránh hết RAM"""
        logger.info("Tạo dữ liệu giao dịch...")
        
        csv_path = self.data_dir / 'transactions.csv'
        total_transactions = 0
        
        # Xóa file cũ nếu có
        if csv_path.exists():
            csv_path.unlink()
        
        # Tạo transactions theo batch
        num_batches = len(customers_df) // batch_size + 1
        
        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(customers_df))
            
            if start_idx >= len(customers_df):
                break
                
            customer_batch = customers_df.iloc[start_idx:end_idx]
            logger.info(f"Batch {batch_num + 1}/{num_batches}: Khách hàng {start_idx:,} - {end_idx:,}")
            
            batch_transactions = []
            
            for _, customer in customer_batch.iterrows():
                # Mỗi khách hàng có 1-10 đơn hàng
                num_orders = np.random.poisson(3) + 1
                
                for order_num in range(num_orders):
                    order_date = self.fake.date_between(
                        start_date=customer['registration_date'], 
                        end_date='today'
                    )
                    
                    # Mỗi đơn hàng có 1-5 sản phẩm
                    num_items = random.randint(1, 5)
                    order_products = products_df.sample(n=num_items)
                    
                    order_id = f"ORDER_{customer['customer_id'][5:]}_{order_num:04d}"
                    
                    for _, product in order_products.iterrows():
                        quantity = random.randint(1, 3)
                        unit_price = product['price'] * (1 - product['discount_percent'] / 100)
                        item_total = round(unit_price * quantity, 2)
                        
                        batch_transactions.append({
                            'transaction_id': f"TXN_{len(batch_transactions) + total_transactions:010d}",
                            'order_id': order_id,
                            'customer_id': customer['customer_id'],
                            'product_id': product['product_id'],
                            'order_date': order_date,
                            'quantity': quantity,
                            'unit_price': round(unit_price, 2),
                            'item_total': item_total,
                            'payment_method': random.choice(['credit_card', 'debit_card', 'momo', 'banking', 'cod']),
                            'shipping_cost': round(random.uniform(0, 50), 2),
                            'order_status': random.choice(['completed', 'shipped', 'delivered', 'cancelled', 'pending']),
                            'created_at': datetime.now()
                        })
            
            # Lưu batch vào CSV
            batch_df = pd.DataFrame(batch_transactions)
            batch_df.to_csv(csv_path, mode='a', header=(batch_num == 0), index=False, encoding='utf-8')
            
            total_transactions += len(batch_transactions)
            logger.info(f"Đã lưu {len(batch_transactions):,} giao dịch (Tổng: {total_transactions:,})")
        
        logger.success(f"Hoàn thành! Tổng {total_transactions:,} giao dịch -> {csv_path}")
        return total_transactions

class ExternalDataCollector:
    def __init__(self):
        self.data_dir = DATA_DIR
        
    def collect_currency_data(self):
        """Thu thập dữ liệu tỷ giá"""
        try:
            logger.info("Thu thập dữ liệu tỷ giá...")
            response = requests.get('https://api.exchangerate-api.com/v4/latest/USD', timeout=10)
            data = response.json()
            
            rates_df = pd.DataFrame(list(data['rates'].items()), columns=['currency', 'rate'])
            rates_df['base_currency'] = data['base']
            rates_df['date'] = data['date']
            rates_df['collected_at'] = datetime.now()
            
            csv_path = self.data_dir / 'currency_rates.csv'
            rates_df.to_csv(csv_path, index=False)
            logger.success(f"Đã lưu {len(rates_df)} tỷ giá vào: {csv_path}")
            return rates_df
        except Exception as e:
            logger.error(f"Lỗi thu thập tỷ giá: {e}")
            return pd.DataFrame()
    
    def collect_fake_store_data(self):
        """Thu thập dữ liệu từ Fake Store API"""
        try:
            logger.info("Thu thập dữ liệu từ Fake Store API...")
            response = requests.get('https://fakestoreapi.com/products', timeout=10)
            products = response.json()
            
            df = pd.DataFrame(products)
            df['collected_at'] = datetime.now()
            df['source'] = 'fake_store_api'
            
            csv_path = self.data_dir / 'external_products.csv'
            df.to_csv(csv_path, index=False)
            logger.success(f"Đã lưu {len(df)} sản phẩm external vào: {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Lỗi thu thập Fake Store data: {e}")
            return pd.DataFrame()

def main():
    """Chạy pipeline tạo dữ liệu hoàn chỉnh"""
    logger.info("🚀 Bắt đầu tạo Big Data cho phân tích")
    
    # Khởi tạo generators
    generator = LocalDataGenerator()
    collector = ExternalDataCollector()
    
    # Tạo dữ liệu synthetic
    logger.info("📊 BƯỚC 1: Tạo dữ liệu synthetic")
    customers_df = generator.generate_customers(50000)  # Giảm xuống để test
    products_df = generator.generate_products(10000)    # Giảm xuống để test
    
    logger.info("📊 BƯỚC 2: Tạo giao dịch")
    total_transactions = generator.generate_transactions(customers_df, products_df)
    
    # Thu thập external data
    logger.info("🌐 BƯỚC 3: Thu thập dữ liệu external")
    collector.collect_currency_data()
    collector.collect_fake_store_data()
    
    # Tạo summary
    logger.info("📋 BƯỚC 4: Tạo báo cáo tổng hợp")
    summary = {
        'customers': len(customers_df),
        'products': len(products_df),
        'transactions': total_transactions,
        'generated_at': datetime.now().isoformat(),
        'data_directory': str(DATA_DIR.absolute())
    }
    
    with open(DATA_DIR / 'data_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    # In kết quả
    logger.success("✅ HOÀN THÀNH! Dữ liệu đã được tạo:")
    logger.info(f"📁 Thư mục: {DATA_DIR.absolute()}")
    logger.info(f"👥 Khách hàng: {summary['customers']:,}")
    logger.info(f"📦 Sản phẩm: {summary['products']:,}")
    logger.info(f"💰 Giao dịch: {summary['transactions']:,}")
    
    # Liệt kê các file đã tạo
    logger.info("\n📄 Các file dữ liệu:")
    for file_path in DATA_DIR.glob("*.csv"):
        size_mb = file_path.stat().st_size / 1024 / 1024
        logger.info(f"  {file_path.name}: {size_mb:.1f} MB")
    
    return True

if __name__ == "__main__":
    main()