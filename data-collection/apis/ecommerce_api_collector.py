#!/usr/bin/env python3
"""
E-commerce Focused API Collector
Thu thập dữ liệu từ 3 APIs chuyên về e-commerce
"""

import asyncio
import aiohttp
import json
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from pymongo import MongoClient
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class EcommerceProduct:
    """E-commerce product data structure"""
    source: str
    product_id: str
    title: str
    price: float
    category: str
    description: str
    image: Optional[str]
    rating: Optional[float]
    rating_count: Optional[int]
    brand: Optional[str]
    stock: Optional[int]
    discount_percentage: Optional[float]
    timestamp: datetime

@dataclass
class EcommerceUser:
    """E-commerce user/customer data"""
    source: str
    user_id: str
    name: str
    email: str
    phone: Optional[str]
    address: Optional[Dict[str, Any]]
    registration_date: Optional[str]
    timestamp: datetime

@dataclass
class ShoppingCart:
    """Shopping cart data"""
    source: str
    cart_id: str
    user_id: str
    products: List[Dict[str, Any]]
    total_price: float
    total_products: int
    date: str
    timestamp: datetime

class EcommerceAPICollector:
    """Collector chuyên dụng cho e-commerce APIs"""

    def __init__(self, config_path: str = "config/data_sources.json"):
        self.load_config(config_path)
        self.setup_connections()
        self.session = None

    def load_config(self, config_path: str):
        """Load configuration"""
        try:
            config_file = Path(__file__).parent.parent / config_path
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
                logger.info("✅ E-commerce API configuration loaded")
        except Exception as e:
            logger.error(f"❌ Failed to load config: {e}")
            raise

    def setup_connections(self):
        """Setup database connections"""
        try:
            # Kafka producer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )

            # MongoDB
            self.mongo_client = MongoClient('mongodb://localhost:27017/')
            self.mongo_db = self.mongo_client.dss_streaming

            # Redis for caching
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

            logger.info("✅ E-commerce API collector connections established")
        except Exception as e:
            logger.error(f"❌ Failed to setup connections: {e}")
            raise

    async def get_session(self):
        """Get HTTP session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def collect_fakestore_data(self) -> Dict[str, List]:
        """Thu thập dữ liệu từ FakeStore API"""
        results = {'products': [], 'users': [], 'carts': []}

        try:
            session = await self.get_session()

            # 1. Thu thập Products
            logger.info("📦 Collecting products from FakeStore API...")
            async with session.get("https://fakestoreapi.com/products") as response:
                if response.status == 200:
                    products = await response.json()

                    for product in products:
                        try:
                            product_data = EcommerceProduct(
                                source='fakestore',
                                product_id=str(product.get('id')),
                                title=product.get('title', 'Unknown'),
                                price=float(product.get('price', 0)),
                                category=product.get('category', 'general'),
                                description=product.get('description', ''),
                                image=product.get('image'),
                                rating=product.get('rating', {}).get('rate') if product.get('rating') else None,
                                rating_count=product.get('rating', {}).get('count') if product.get('rating') else None,
                                brand=None,
                                stock=None,
                                discount_percentage=None,
                                timestamp=datetime.now()
                            )
                            results['products'].append(product_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing FakeStore product: {e}")

            # 2. Thu thập Users
            logger.info("👥 Collecting users from FakeStore API...")
            async with session.get("https://fakestoreapi.com/users") as response:
                if response.status == 200:
                    users = await response.json()

                    for user in users:
                        try:
                            user_data = EcommerceUser(
                                source='fakestore',
                                user_id=str(user.get('id')),
                                name=f"{user.get('name', {}).get('firstname', '')} {user.get('name', {}).get('lastname', '')}".strip(),
                                email=user.get('email', ''),
                                phone=user.get('phone'),
                                address=user.get('address'),
                                registration_date=None,
                                timestamp=datetime.now()
                            )
                            results['users'].append(user_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing FakeStore user: {e}")

            # 3. Thu thập Carts
            logger.info("🛒 Collecting carts from FakeStore API...")
            async with session.get("https://fakestoreapi.com/carts") as response:
                if response.status == 200:
                    carts = await response.json()

                    for cart in carts:
                        try:
                            cart_data = ShoppingCart(
                                source='fakestore',
                                cart_id=str(cart.get('id')),
                                user_id=str(cart.get('userId')),
                                products=cart.get('products', []),
                                total_price=sum(p.get('price', 0) * p.get('quantity', 0) for p in cart.get('products', [])),
                                total_products=len(cart.get('products', [])),
                                date=cart.get('date', ''),
                                timestamp=datetime.now()
                            )
                            results['carts'].append(cart_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing FakeStore cart: {e}")

            logger.info(f"✅ FakeStore: {len(results['products'])} products, {len(results['users'])} users, {len(results['carts'])} carts")

        except Exception as e:
            logger.error(f"❌ Error collecting FakeStore data: {e}")

        return results

    async def collect_dummyjson_data(self) -> Dict[str, List]:
        """Thu thập dữ liệu từ DummyJSON API"""
        results = {'products': [], 'users': [], 'carts': []}

        try:
            session = await self.get_session()

            # 1. Thu thập Products (có nhiều thông tin hơn)
            logger.info("📦 Collecting products from DummyJSON API...")
            async with session.get("https://dummyjson.com/products?limit=50") as response:
                if response.status == 200:
                    data = await response.json()
                    products = data.get('products', [])

                    for product in products:
                        try:
                            product_data = EcommerceProduct(
                                source='dummyjson',
                                product_id=str(product.get('id')),
                                title=product.get('title', 'Unknown'),
                                price=float(product.get('price', 0)),
                                category=product.get('category', 'general'),
                                description=product.get('description', ''),
                                image=product.get('thumbnail'),
                                rating=float(product.get('rating', 0)),
                                rating_count=None,
                                brand=product.get('brand'),
                                stock=product.get('stock'),
                                discount_percentage=product.get('discountPercentage'),
                                timestamp=datetime.now()
                            )
                            results['products'].append(product_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing DummyJSON product: {e}")

            # 2. Thu thập Users
            logger.info("👥 Collecting users from DummyJSON API...")
            async with session.get("https://dummyjson.com/users?limit=30") as response:
                if response.status == 200:
                    data = await response.json()
                    users = data.get('users', [])

                    for user in users:
                        try:
                            user_data = EcommerceUser(
                                source='dummyjson',
                                user_id=str(user.get('id')),
                                name=f"{user.get('firstName', '')} {user.get('lastName', '')}".strip(),
                                email=user.get('email', ''),
                                phone=user.get('phone'),
                                address=user.get('address'),
                                registration_date=None,
                                timestamp=datetime.now()
                            )
                            results['users'].append(user_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing DummyJSON user: {e}")

            # 3. Thu thập Carts
            logger.info("🛒 Collecting carts from DummyJSON API...")
            async with session.get("https://dummyjson.com/carts?limit=20") as response:
                if response.status == 200:
                    data = await response.json()
                    carts = data.get('carts', [])

                    for cart in carts:
                        try:
                            cart_data = ShoppingCart(
                                source='dummyjson',
                                cart_id=str(cart.get('id')),
                                user_id=str(cart.get('userId')),
                                products=cart.get('products', []),
                                total_price=float(cart.get('total', 0)),
                                total_products=cart.get('totalProducts', 0),
                                date='',
                                timestamp=datetime.now()
                            )
                            results['carts'].append(cart_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing DummyJSON cart: {e}")

            logger.info(f"✅ DummyJSON: {len(results['products'])} products, {len(results['users'])} users, {len(results['carts'])} carts")

        except Exception as e:
            logger.error(f"❌ Error collecting DummyJSON data: {e}")

        return results

    async def collect_platzi_data(self) -> Dict[str, List]:
        """Thu thập dữ liệu từ Platzi Fake API"""
        results = {'products': [], 'users': [], 'categories': []}

        try:
            session = await self.get_session()

            # 1. Thu thập Products
            logger.info("📦 Collecting products from Platzi API...")
            async with session.get("https://api.escuelajs.co/api/v1/products?offset=0&limit=50") as response:
                if response.status == 200:
                    products = await response.json()

                    for product in products:
                        try:
                            product_data = EcommerceProduct(
                                source='platzi',
                                product_id=str(product.get('id')),
                                title=product.get('title', 'Unknown'),
                                price=float(product.get('price', 0)),
                                category=product.get('category', {}).get('name', 'general') if product.get('category') else 'general',
                                description=product.get('description', ''),
                                image=product.get('images', [None])[0] if product.get('images') else None,
                                rating=None,
                                rating_count=None,
                                brand=None,
                                stock=None,
                                discount_percentage=None,
                                timestamp=datetime.now()
                            )
                            results['products'].append(product_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing Platzi product: {e}")

            # 2. Thu thập Users
            logger.info("👥 Collecting users from Platzi API...")
            async with session.get("https://api.escuelajs.co/api/v1/users?limit=30") as response:
                if response.status == 200:
                    users = await response.json()

                    for user in users:
                        try:
                            user_data = EcommerceUser(
                                source='platzi',
                                user_id=str(user.get('id')),
                                name=user.get('name', 'Unknown'),
                                email=user.get('email', ''),
                                phone=None,
                                address=None,
                                registration_date=user.get('creationAt'),
                                timestamp=datetime.now()
                            )
                            results['users'].append(user_data)
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing Platzi user: {e}")

            logger.info(f"✅ Platzi: {len(results['products'])} products, {len(results['users'])} users")

        except Exception as e:
            logger.error(f"❌ Error collecting Platzi data: {e}")

        return results

    def save_to_storage(self, all_results: Dict[str, Dict[str, List]]):
        """Lưu tất cả dữ liệu vào storage systems"""
        try:
            total_saved = 0

            for source, data_types in all_results.items():
                for data_type, items in data_types.items():
                    if not items:
                        continue

                    # Topic mapping
                    topic_mapping = {
                        'products': 'products_raw',
                        'users': 'customers_raw',
                        'carts': 'carts_raw'
                    }

                    topic = topic_mapping.get(data_type, 'raw_data')

                    # Collection mapping
                    collection_mapping = {
                        'products': 'products_raw',
                        'users': 'customers_raw',
                        'carts': 'carts_raw'
                    }

                    collection_name = collection_mapping.get(data_type, 'raw_data')

                    # Save each item
                    for item in items:
                        data_dict = asdict(item)

                        # Send to Kafka
                        key = f"{item.source}_{getattr(item, 'product_id', getattr(item, 'user_id', getattr(item, 'cart_id', 'unknown')))}"
                        self.kafka_producer.send(
                            topic,
                            key=key,
                            value=data_dict
                        )

                        # Save to MongoDB
                        getattr(self.mongo_db, collection_name).insert_one(data_dict)

                        total_saved += 1

            # Flush Kafka messages
            self.kafka_producer.flush()

            logger.info(f"💾 Saved {total_saved} e-commerce records to storage systems")

        except Exception as e:
            logger.error(f"❌ Error saving e-commerce data: {e}")

    async def run_ecommerce_collection(self):
        """Chạy thu thập dữ liệu từ tất cả 3 e-commerce APIs"""
        logger.info("🛒 Starting E-commerce API Collection Session...")

        try:
            session_results = {
                'start_time': datetime.now().isoformat(),
                'apis_collected': {},
                'total_records': 0,
                'success': True
            }

            all_results = {}

            # Thu thập từ FakeStore API
            logger.info("📦 Collecting from FakeStore API...")
            fakestore_results = await self.collect_fakestore_data()
            all_results['fakestore'] = fakestore_results
            session_results['apis_collected']['fakestore'] = {
                'products': len(fakestore_results['products']),
                'users': len(fakestore_results['users']),
                'carts': len(fakestore_results['carts'])
            }

            await asyncio.sleep(1)  # Rate limiting

            # Thu thập từ DummyJSON API
            logger.info("📦 Collecting from DummyJSON API...")
            dummyjson_results = await self.collect_dummyjson_data()
            all_results['dummyjson'] = dummyjson_results
            session_results['apis_collected']['dummyjson'] = {
                'products': len(dummyjson_results['products']),
                'users': len(dummyjson_results['users']),
                'carts': len(dummyjson_results['carts'])
            }

            await asyncio.sleep(1)  # Rate limiting

            # Thu thập từ Platzi API
            logger.info("📦 Collecting from Platzi API...")
            platzi_results = await self.collect_platzi_data()
            all_results['platzi'] = platzi_results
            session_results['apis_collected']['platzi'] = {
                'products': len(platzi_results['products']),
                'users': len(platzi_results['users'])
            }

            # Lưu tất cả dữ liệu
            self.save_to_storage(all_results)

            # Tính tổng records
            total_records = 0
            for api_data in session_results['apis_collected'].values():
                total_records += sum(api_data.values())

            session_results['total_records'] = total_records
            session_results['end_time'] = datetime.now().isoformat()

            # Summary statistics
            summary = {
                'total_products': sum(len(r.get('products', [])) for r in all_results.values()),
                'total_users': sum(len(r.get('users', [])) for r in all_results.values()),
                'total_carts': sum(len(r.get('carts', [])) for r in all_results.values()),
                'apis_used': 3,
                'data_sources': ['fakestore', 'dummyjson', 'platzi']
            }

            session_results['summary'] = summary

            logger.info(f"✅ E-commerce collection completed: {session_results}")
            return session_results

        except Exception as e:
            logger.error(f"❌ E-commerce collection failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

        finally:
            if self.session and not self.session.closed:
                await self.session.close()

    def close_connections(self):
        """Close all connections"""
        try:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()
            logger.info("🔌 E-commerce API collector connections closed")
        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")

# Main execution
async def main():
    """Main execution function"""
    collector = EcommerceAPICollector()

    try:
        # Chạy thu thập e-commerce data
        result = await collector.run_ecommerce_collection()
        print(f"🛒 E-commerce Collection Results:")
        print(json.dumps(result, indent=2, default=str))

    finally:
        collector.close_connections()

if __name__ == "__main__":
    print("""
    🛒 E-commerce API Data Collector
    ===============================

    Thu thập dữ liệu từ 3 APIs chuyên về e-commerce:
    • 📦 FakeStore API - Products, Users, Shopping Carts
    • 🛍️ DummyJSON - Rich product data with ratings, brands, stock
    • 🏪 Platzi Fake API - Products with images, Categories, Users

    Tất cả APIs đều miễn phí và tập trung vào e-commerce data!
    """)

    asyncio.run(main())