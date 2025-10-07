#!/usr/bin/env python3
"""
Vietnam Electronics Data Collector
Thu thập dữ liệu sản phẩm điện tử từ các platform TMĐT Việt Nam
"""

import requests
import pandas as pd
import json
import time
import random
from datetime import datetime
from pathlib import Path
import logging
from fake_useragent import UserAgent
import urllib.parse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VietnamElectronicsCollector:
    def __init__(self):
        self.ua = UserAgent()
        self.session = requests.Session()
        self.output_dir = Path("data/vietnam_electronics_fresh")
        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Headers cơ bản
        self.headers = {
            'User-Agent': self.ua.random,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        # Cấu hình các platform Việt Nam
        self.platforms = {
            'tiki': {
                'name': 'Tiki Vietnam',
                'base_url': 'https://tiki.vn',
                'api_url': 'https://tiki.vn/api/personalish/v1/blocks/listings',
                'categories': {
                    'smartphones': '1795',
                    'laptops': '1846',
                    'tablets': '1794',
                    'audio': '1815',
                    'cameras': '1801'
                }
            },
            'fptshop': {
                'name': 'FPT Shop',
                'base_url': 'https://fptshop.com.vn',
                'categories': [
                    'dien-thoai',
                    'laptop',
                    'tablet',
                    'am-thanh',
                    'camera'
                ]
            },
            'cellphones': {
                'name': 'CellphoneS',
                'base_url': 'https://cellphones.com.vn',
                'categories': [
                    'mobile',
                    'tablet',
                    'laptop',
                    'audio',
                    'accessories'
                ]
            }
        }

        # Từ khóa điện tử Việt Nam
        self.electronics_keywords = [
            'điện thoại', 'smartphone', 'iPhone', 'Samsung', 'Xiaomi', 'Oppo', 'Vivo',
            'laptop', 'máy tính', 'Dell', 'HP', 'Asus', 'Acer', 'Lenovo',
            'tablet', 'iPad', 'máy tính bảng',
            'tai nghe', 'headphone', 'loa', 'speaker', 'âm thanh',
            'camera', 'máy ảnh', 'Canon', 'Sony', 'Nikon',
            'smartwatch', 'đồng hồ thông minh', 'Apple Watch',
            'sạc dự phòng', 'pin sạc', 'powerbank'
        ]

    def delay_request(self, min_delay=1, max_delay=3):
        """Random delay giữa các request"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def collect_from_tiki(self, category='smartphones', limit=50):
        """Thu thập dữ liệu từ Tiki"""
        logger.info(f"🛒 Thu thập từ Tiki - Category: {category}")

        try:
            products = []
            category_id = self.platforms['tiki']['categories'].get(category, '1795')

            # Tiki API parameters
            params = {
                'limit': 20,
                'include': 'advertisement',
                'aggregations': 2,
                'version': 'home-persionalized',
                'trackity_id': f"category_{category_id}",
                'category': category_id,
                'page': 1,
                'urlKey': category
            }

            pages_to_collect = min(5, (limit // 20) + 1)

            for page in range(1, pages_to_collect + 1):
                params['page'] = page

                try:
                    response = self.session.get(
                        self.platforms['tiki']['api_url'],
                        headers=self.headers,
                        params=params,
                        timeout=15
                    )

                    if response.status_code == 200:
                        data = response.json()

                        if 'data' in data:
                            for item in data['data'][:20]:  # Limit per page
                                try:
                                    product = self._parse_tiki_product(item)
                                    if product:
                                        products.append(product)
                                        if len(products) >= limit:
                                            break
                                except Exception as e:
                                    logger.warning(f"Lỗi parse sản phẩm Tiki: {e}")
                                    continue

                        logger.info(f"📱 Tiki page {page}: {len(products)} products collected")

                        if len(products) >= limit:
                            break

                    else:
                        logger.warning(f"Tiki API error: {response.status_code}")
                        break

                except Exception as e:
                    logger.error(f"Lỗi request Tiki page {page}: {e}")
                    continue

                self.delay_request(2, 4)

            return products[:limit]

        except Exception as e:
            logger.error(f"Lỗi thu thập từ Tiki: {e}")
            return []

    def _parse_tiki_product(self, item):
        """Parse sản phẩm từ Tiki API response"""
        try:
            return {
                'id': f"TIKI_{item.get('id', '')}",
                'name': item.get('name', '').strip(),
                'brand': item.get('brand_name', 'Unknown'),
                'price_vnd': item.get('price', 0),
                'original_price_vnd': item.get('list_price', 0),
                'discount_percentage': item.get('discount_rate', 0),
                'rating': item.get('rating_average', 0),
                'review_count': item.get('review_count', 0),
                'thumbnail': item.get('thumbnail_url', ''),
                'url': f"https://tiki.vn/{item.get('url_key', '')}-p{item.get('id', '')}.html",
                'category': 'Electronics',
                'subcategory': item.get('categories', {}).get('name', 'Unknown'),
                'platform': 'Tiki',
                'stock_quantity': item.get('qty_sold', 0),
                'description': item.get('short_description', ''),
                'collected_at': datetime.now().isoformat(),
                'source': 'Vietnam_Electronics_Collector',
                'country': 'Vietnam'
            }
        except Exception as e:
            logger.warning(f"Lỗi parse Tiki product: {e}")
            return None

    def collect_from_dummy_apis(self, category='electronics', limit=30):
        """Thu thập từ các API demo/dummy"""
        logger.info(f"🌐 Thu thập từ API demo - Category: {category}")

        try:
            products = []

            # DummyJSON Electronics
            dummy_url = f"https://dummyjson.com/products/category/{category}"
            try:
                response = self.session.get(dummy_url, headers=self.headers, timeout=15)
                if response.status_code == 200:
                    data = response.json()

                    for item in data.get('products', []):
                        product = self._parse_dummy_product(item)
                        if product:
                            products.append(product)

                    logger.info(f"📦 DummyJSON: {len(products)} products")

            except Exception as e:
                logger.warning(f"Lỗi DummyJSON: {e}")

            # Fake Store API
            try:
                fake_store_url = "https://fakestoreapi.com/products/category/electronics"
                response = self.session.get(fake_store_url, headers=self.headers, timeout=15)

                if response.status_code == 200:
                    items = response.json()

                    for item in items:
                        product = self._parse_fake_store_product(item)
                        if product:
                            products.append(product)

                    logger.info(f"🏪 FakeStore: {len(products)} products total")

            except Exception as e:
                logger.warning(f"Lỗi FakeStore API: {e}")

            return products[:limit]

        except Exception as e:
            logger.error(f"Lỗi thu thập từ APIs: {e}")
            return []

    def _parse_dummy_product(self, item):
        """Parse sản phẩm từ DummyJSON"""
        try:
            # Chuyển đổi giá USD sang VND (tỷ giá ~24,000)
            price_usd = item.get('price', 0)
            price_vnd = price_usd * 24000

            return {
                'id': f"DUMMY_{item.get('id', '')}",
                'name': item.get('title', '').strip(),
                'brand': item.get('brand', 'Unknown'),
                'price_vnd': price_vnd,
                'price_usd': price_usd,
                'discount_percentage': item.get('discountPercentage', 0),
                'rating': item.get('rating', 0),
                'review_count': random.randint(10, 500),  # Giả lập
                'thumbnail': item.get('thumbnail', ''),
                'images': item.get('images', []),
                'category': 'Electronics',
                'subcategory': item.get('category', 'electronics'),
                'platform': 'DummyJSON',
                'stock_quantity': item.get('stock', 0),
                'description': item.get('description', ''),
                'collected_at': datetime.now().isoformat(),
                'source': 'Vietnam_Electronics_Collector',
                'country': 'Global_Localized_VN'
            }
        except Exception as e:
            logger.warning(f"Lỗi parse Dummy product: {e}")
            return None

    def _parse_fake_store_product(self, item):
        """Parse sản phẩm từ FakeStore API"""
        try:
            price_usd = item.get('price', 0)
            price_vnd = price_usd * 24000

            return {
                'id': f"FAKE_{item.get('id', '')}",
                'name': item.get('title', '').strip(),
                'brand': 'Generic',
                'price_vnd': price_vnd,
                'price_usd': price_usd,
                'rating': item.get('rating', {}).get('rate', 0),
                'review_count': item.get('rating', {}).get('count', 0),
                'thumbnail': item.get('image', ''),
                'category': 'Electronics',
                'subcategory': item.get('category', 'electronics'),
                'platform': 'FakeStore',
                'description': item.get('description', ''),
                'collected_at': datetime.now().isoformat(),
                'source': 'Vietnam_Electronics_Collector',
                'country': 'Global_Localized_VN'
            }
        except Exception as e:
            logger.warning(f"Lỗi parse FakeStore product: {e}")
            return None

    def generate_synthetic_vietnam_electronics(self, count=100):
        """Tạo dữ liệu điện tử Việt Nam synthetic"""
        logger.info(f"🎲 Tạo {count} sản phẩm điện tử VN synthetic")

        try:
            # Dữ liệu base cho Việt Nam
            vn_brands = ['Samsung', 'Xiaomi', 'Oppo', 'Vivo', 'iPhone', 'Huawei', 'Realme', 'Nokia']
            vn_categories = {
                'Smartphones': ['Galaxy', 'iPhone', 'Mi', 'Redmi', 'A-series', 'Y-series'],
                'Laptops': ['Thinkpad', 'Vivobook', 'Inspiron', 'Pavilion', 'Ideapad'],
                'Tablets': ['iPad', 'Galaxy Tab', 'Xiaomi Pad', 'Huawei MatePad'],
                'Audio': ['AirPods', 'Galaxy Buds', 'Mi Earbuds', 'Sony WH', 'JBL'],
                'Cameras': ['EOS', 'Alpha', 'Lumix', 'Coolpix', 'PowerShot']
            }

            vn_platforms = ['Shopee', 'Lazada', 'Tiki', 'Sendo', 'FPT Shop', 'CellphoneS']
            vn_payment_methods = ['COD', 'MoMo', 'ZaloPay', 'VNPay', 'Bank Transfer', 'Credit Card']

            synthetic_products = []

            for i in range(count):
                category = random.choice(list(vn_categories.keys()))
                brand = random.choice(vn_brands)
                model = random.choice(vn_categories[category])

                # Tạo tên sản phẩm
                name = f"{brand} {model} {random.choice(['Pro', 'Plus', 'Ultra', 'Standard', 'Lite'])}"

                # Giá theo thị trường VN
                if category == 'Smartphones':
                    base_price = random.uniform(3000000, 30000000)  # 3-30 triệu VND
                elif category == 'Laptops':
                    base_price = random.uniform(8000000, 50000000)  # 8-50 triệu VND
                elif category == 'Tablets':
                    base_price = random.uniform(2000000, 20000000)  # 2-20 triệu VND
                elif category == 'Audio':
                    base_price = random.uniform(500000, 8000000)   # 0.5-8 triệu VND
                else:
                    base_price = random.uniform(5000000, 40000000)  # 5-40 triệu VND

                discount = random.uniform(0, 30)
                final_price = base_price * (1 - discount/100)

                product = {
                    'id': f'VN_SYNTH_{i+1:06d}',
                    'name': name,
                    'brand': brand,
                    'category': 'Electronics',
                    'subcategory': category,
                    'price_vnd': round(final_price),
                    'original_price_vnd': round(base_price),
                    'discount_percentage': round(discount, 1),
                    'rating': round(random.uniform(3.5, 5.0), 1),
                    'review_count': random.randint(50, 2000),
                    'stock_quantity': random.randint(10, 500),
                    'warranty_months': random.choice([6, 12, 18, 24, 36]),
                    'platform': random.choice(vn_platforms),
                    'payment_methods': random.sample(vn_payment_methods, k=random.randint(3, 5)),
                    'description': f'Sản phẩm {category.lower()} chất lượng cao phổ biến tại thị trường Việt Nam',
                    'market_focus': 'Vietnam',
                    'country': 'Vietnam',
                    'language': 'Vietnamese',
                    'collected_at': datetime.now().isoformat(),
                    'source': 'Vietnam_Synthetic_Generator',
                    'data_type': 'synthetic'
                }

                synthetic_products.append(product)

            logger.info(f"✅ Đã tạo {len(synthetic_products)} sản phẩm synthetic")
            return synthetic_products

        except Exception as e:
            logger.error(f"Lỗi tạo synthetic data: {e}")
            return []

    def collect_all_electronics(self):
        """Thu thập tất cả dữ liệu điện tử từ mọi nguồn"""
        logger.info("🚀 Bắt đầu thu thập toàn bộ dữ liệu điện tử Việt Nam")

        all_products = []

        try:
            # 1. Thu thập từ Tiki
            tiki_categories = ['smartphones', 'laptops', 'tablets', 'audio']
            for category in tiki_categories:
                products = self.collect_from_tiki(category, limit=30)
                all_products.extend(products)
                logger.info(f"✅ Tiki {category}: {len(products)} products")
                self.delay_request(3, 5)

            # 2. Thu thập từ Demo APIs
            api_products = self.collect_from_dummy_apis(limit=50)
            all_products.extend(api_products)
            logger.info(f"✅ Demo APIs: {len(api_products)} products")

            # 3. Tạo synthetic data
            synthetic_products = self.generate_synthetic_vietnam_electronics(count=200)
            all_products.extend(synthetic_products)
            logger.info(f"✅ Synthetic: {len(synthetic_products)} products")

            # 4. Lưu dữ liệu
            if all_products:
                df = pd.DataFrame(all_products)

                # Làm sạch dữ liệu
                df = self._clean_collected_data(df)

                # Lưu file
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = self.output_dir / f'vietnam_electronics_fresh_{timestamp}.csv'
                df.to_csv(output_file, index=False, encoding='utf-8')

                # Lưu summary
                summary = self._create_collection_summary(df)
                summary_file = self.output_dir / f'collection_summary_{timestamp}.json'
                with open(summary_file, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, ensure_ascii=False, indent=2)

                logger.info(f"💾 Đã lưu {len(df)} sản phẩm tại: {output_file}")
                logger.info(f"📊 Summary: {summary_file}")

                return df, summary
            else:
                logger.warning("⚠️ Không thu thập được dữ liệu nào!")
                return None, None

        except Exception as e:
            logger.error(f"❌ Lỗi trong quá trình thu thập: {e}")
            return None, None

    def _clean_collected_data(self, df):
        """Làm sạch dữ liệu đã thu thập"""
        logger.info("🧽 Làm sạch dữ liệu thu thập...")

        # Xóa duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['name', 'brand'], keep='first')

        # Chuẩn hóa dữ liệu
        df['name'] = df['name'].str.strip()
        df['brand'] = df['brand'].fillna('Unknown')
        df['price_vnd'] = pd.to_numeric(df['price_vnd'], errors='coerce').fillna(0)

        logger.info(f"✅ Làm sạch: {initial_count} → {len(df)} products")
        return df

    def _create_collection_summary(self, df):
        """Tạo báo cáo tổng hợp thu thập"""
        return {
            'collection_timestamp': datetime.now().isoformat(),
            'total_products': len(df),
            'unique_brands': df['brand'].nunique(),
            'platforms': df['platform'].value_counts().to_dict() if 'platform' in df.columns else {},
            'categories': df['subcategory'].value_counts().to_dict() if 'subcategory' in df.columns else {},
            'price_range_vnd': {
                'min': float(df['price_vnd'].min()) if 'price_vnd' in df.columns else 0,
                'max': float(df['price_vnd'].max()) if 'price_vnd' in df.columns else 0,
                'avg': float(df['price_vnd'].mean()) if 'price_vnd' in df.columns else 0
            },
            'data_sources': df['source'].value_counts().to_dict() if 'source' in df.columns else {}
        }

if __name__ == "__main__":
    collector = VietnamElectronicsCollector()

    print("🇻🇳 VIETNAM ELECTRONICS COLLECTOR")
    print("=" * 50)
    print("Thu thập dữ liệu sản phẩm điện tử từ các platform Việt Nam")
    print()

    # Chạy thu thập
    df, summary = collector.collect_all_electronics()

    if df is not None and summary is not None:
        print("✅ THU THẬP HOÀN TẤT!")
        print(f"📱 Tổng sản phẩm: {summary['total_products']:,}")
        print(f"🏷️ Brands: {summary['unique_brands']}")
        print(f"💰 Giá trung bình: {summary['price_range_vnd']['avg']:,.0f} VND")
        print(f"📂 Dữ liệu lưu tại: data/vietnam_electronics_fresh/")
        print("=" * 50)
    else:
        print("❌ THU THẬP THẤT BẠI!")
        print("Kiểm tra kết nối mạng và thử lại.")