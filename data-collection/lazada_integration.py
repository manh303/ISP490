#!/usr/bin/env python3
"""
Lazada Crawler Integration Module
Tích hợp Lazada crawler với database và batch processing system
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent))

# Import existing modules
try:
    from backend.app.models.database_models import (
        Product, Customer, Order, OrderItem,
        create_tables, ProductCreate, ProductResponse
    )
    from backend.app.database import get_db_session
except ImportError:
    logging.warning("Could not import database models, using fallback")

# Import our crawler
from lazada_electronics_crawler import LazadaElectronicsCrawler, LazadaProduct

# Database imports
import sqlalchemy
from sqlalchemy import create_engine, text
import psycopg2
import pymongo
from kafka import KafkaProducer
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LazadaDataProcessor:
    """Process và transform Lazada data cho database integration"""

    def __init__(self):
        self.db_config = self.load_db_config()
        self.setup_connections()

    def load_db_config(self) -> Dict[str, Any]:
        """Load database configuration"""
        return {
            'postgresql': {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'database': os.getenv('POSTGRES_DB', 'ecommerce_dss'),
                'user': os.getenv('POSTGRES_USER', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD', 'password'),
                'port': int(os.getenv('POSTGRES_PORT', 5432))
            },
            'mongodb': {
                'host': os.getenv('MONGO_HOST', 'localhost'),
                'port': int(os.getenv('MONGO_PORT', 27017)),
                'database': os.getenv('MONGO_DB', 'dss_streaming')
            },
            'kafka': {
                'bootstrap_servers': [os.getenv('KAFKA_SERVERS', 'localhost:9092')]
            },
            'redis': {
                'host': os.getenv('REDIS_HOST', 'localhost'),
                'port': int(os.getenv('REDIS_PORT', 6379))
            }
        }

    def setup_connections(self):
        """Setup database connections"""
        try:
            # PostgreSQL
            pg_config = self.db_config['postgresql']
            self.pg_engine = create_engine(
                f"postgresql://{pg_config['user']}:{pg_config['password']}@"
                f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
            )

            # MongoDB
            mongo_config = self.db_config['mongodb']
            self.mongo_client = pymongo.MongoClient(
                f"mongodb://{mongo_config['host']}:{mongo_config['port']}/"
            )
            self.mongo_db = self.mongo_client[mongo_config['database']]

            # Kafka Producer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.db_config['kafka']['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )

            # Redis
            redis_config = self.db_config['redis']
            self.redis_client = redis.Redis(
                host=redis_config['host'],
                port=redis_config['port'],
                decode_responses=True
            )

            logger.info("✅ All database connections established")

        except Exception as e:
            logger.error(f"❌ Failed to setup connections: {e}")
            raise

    def transform_lazada_product(self, lazada_product: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Lazada product data to standardized format"""
        try:
            # Generate unique product ID
            product_id = f"lazada_{lazada_product.get('product_id', '')}"

            # Transform to standard product format
            transformed = {
                'product_id': product_id,
                'name': lazada_product.get('name', ''),
                'description': lazada_product.get('description', ''),
                'category': self.standardize_category(lazada_product.get('category', '')),
                'subcategory': lazada_product.get('subcategory'),
                'brand': lazada_product.get('brand'),
                'price': float(lazada_product.get('price', 0)),
                'cost': None,  # Not available from crawling
                'margin_percent': None,
                'stock_quantity': 100,  # Default assumption
                'reorder_level': 10,
                'supplier_id': 'lazada_vn',
                'weight': None,
                'dimensions': None,
                'color': None,
                'size': None,
                'total_sold': self.extract_sold_count(lazada_product.get('sold_count')),
                'total_revenue': 0,  # Will be calculated
                'avg_rating': lazada_product.get('rating'),
                'review_count': lazada_product.get('review_count', 0),
                'is_active': lazada_product.get('availability', True),
                'is_featured': False,
                'created_at': datetime.now(),
                'updated_at': datetime.now(),

                # Lazada specific metadata
                'metadata': {
                    'source': 'lazada_vn',
                    'original_url': lazada_product.get('url'),
                    'original_price': lazada_product.get('original_price'),
                    'discount_percent': lazada_product.get('discount_percent'),
                    'seller_name': lazada_product.get('seller_name'),
                    'seller_rating': lazada_product.get('seller_rating'),
                    'key_features': lazada_product.get('key_features', []),
                    'specifications': lazada_product.get('specifications', {}),
                    'images': lazada_product.get('images', []),
                    'shipping_info': lazada_product.get('shipping_info', {}),
                    'location': lazada_product.get('location'),
                    'crawl_timestamp': lazada_product.get('crawl_timestamp')
                }
            }

            return transformed

        except Exception as e:
            logger.error(f"❌ Error transforming product: {e}")
            return None

    def standardize_category(self, category: str) -> str:
        """Standardize category names"""
        category_mapping = {
            'Điện Thoại Di Động': 'Smartphones',
            'Máy Tính Xách Tay': 'Laptops',
            'Máy Tính Bảng': 'Tablets',
            'Đồng Hồ Thông Minh': 'Smartwatches',
            'Tai Nghe': 'Audio',
            'Loa': 'Audio',
            'Máy Ảnh': 'Cameras',
            'Phụ Kiện Điện Thoại': 'Accessories'
        }

        return category_mapping.get(category, 'Electronics')

    def extract_sold_count(self, sold_text: Optional[str]) -> int:
        """Extract numeric sold count from text"""
        if not sold_text:
            return 0

        try:
            import re
            numbers = re.findall(r'\d+', sold_text.replace('.', '').replace(',', ''))
            if numbers:
                return int(numbers[0])
        except:
            pass

        return 0

    def save_to_postgresql(self, products: List[Dict[str, Any]]) -> bool:
        """Save products to PostgreSQL"""
        try:
            if not products:
                return True

            # Convert to DataFrame
            df = pd.DataFrame(products)

            # Save to raw table first
            df.to_sql(
                'lazada_products_raw',
                self.pg_engine,
                if_exists='append',
                index=False,
                method='multi'
            )

            logger.info(f"✅ Saved {len(products)} products to PostgreSQL (raw table)")

            # Also save to main products table with conflict resolution
            self.upsert_to_products_table(products)

            return True

        except Exception as e:
            logger.error(f"❌ Error saving to PostgreSQL: {e}")
            return False

    def upsert_to_products_table(self, products: List[Dict[str, Any]]):
        """Upsert products to main products table"""
        try:
            with self.pg_engine.connect() as conn:
                for product in products:
                    # Check if product exists
                    result = conn.execute(
                        text("SELECT id FROM products WHERE product_id = :product_id"),
                        {'product_id': product['product_id']}
                    )

                    if result.fetchone():
                        # Update existing
                        conn.execute(
                            text("""
                                UPDATE products SET
                                    name = :name,
                                    price = :price,
                                    avg_rating = :avg_rating,
                                    review_count = :review_count,
                                    total_sold = :total_sold,
                                    updated_at = :updated_at
                                WHERE product_id = :product_id
                            """),
                            {
                                'name': product['name'],
                                'price': product['price'],
                                'avg_rating': product['avg_rating'],
                                'review_count': product['review_count'],
                                'total_sold': product['total_sold'],
                                'updated_at': product['updated_at'],
                                'product_id': product['product_id']
                            }
                        )
                    else:
                        # Insert new
                        conn.execute(
                            text("""
                                INSERT INTO products (
                                    product_id, name, description, category, brand,
                                    price, stock_quantity, avg_rating, review_count,
                                    total_sold, is_active, created_at, updated_at
                                ) VALUES (
                                    :product_id, :name, :description, :category, :brand,
                                    :price, :stock_quantity, :avg_rating, :review_count,
                                    :total_sold, :is_active, :created_at, :updated_at
                                )
                            """),
                            {
                                'product_id': product['product_id'],
                                'name': product['name'],
                                'description': product['description'],
                                'category': product['category'],
                                'brand': product['brand'],
                                'price': product['price'],
                                'stock_quantity': product['stock_quantity'],
                                'avg_rating': product['avg_rating'],
                                'review_count': product['review_count'],
                                'total_sold': product['total_sold'],
                                'is_active': product['is_active'],
                                'created_at': product['created_at'],
                                'updated_at': product['updated_at']
                            }
                        )

                conn.commit()
                logger.info(f"✅ Upserted {len(products)} products to main products table")

        except Exception as e:
            logger.error(f"❌ Error upserting to products table: {e}")

    def save_to_mongodb(self, products: List[Dict[str, Any]]) -> bool:
        """Save raw products to MongoDB"""
        try:
            if not products:
                return True

            collection = self.mongo_db.lazada_products_raw

            # Add timestamp to each product
            for product in products:
                product['inserted_at'] = datetime.now()

            result = collection.insert_many(products)

            logger.info(f"✅ Saved {len(result.inserted_ids)} products to MongoDB")
            return True

        except Exception as e:
            logger.error(f"❌ Error saving to MongoDB: {e}")
            return False

    def send_to_kafka(self, products: List[Dict[str, Any]]) -> bool:
        """Send products to Kafka for streaming processing"""
        try:
            if not products:
                return True

            topic = 'lazada_products_stream'

            for product in products:
                key = product.get('product_id', str(uuid.uuid4()))
                self.kafka_producer.send(
                    topic,
                    key=key,
                    value=product
                )

            self.kafka_producer.flush()

            logger.info(f"✅ Sent {len(products)} products to Kafka topic: {topic}")
            return True

        except Exception as e:
            logger.error(f"❌ Error sending to Kafka: {e}")
            return False

    def cache_statistics(self, products: List[Dict[str, Any]]):
        """Cache crawl statistics to Redis"""
        try:
            if not products:
                return

            stats = {
                'total_products': len(products),
                'categories': list(set(p.get('category', '') for p in products)),
                'avg_price': sum(p.get('price', 0) for p in products) / len(products),
                'timestamp': datetime.now().isoformat(),
                'source': 'lazada_crawler'
            }

            # Cache for 24 hours
            self.redis_client.setex(
                'lazada_crawl_stats',
                86400,
                json.dumps(stats, default=str)
            )

            logger.info(f"✅ Cached statistics to Redis")

        except Exception as e:
            logger.error(f"❌ Error caching statistics: {e}")

class LazadaCrawlerOrchestrator:
    """Orchestrate complete Lazada crawling workflow"""

    def __init__(self, headless: bool = True):
        self.crawler = LazadaElectronicsCrawler(headless=headless)
        self.processor = LazadaDataProcessor()

    async def run_complete_workflow(
        self,
        categories: List[str] = None,
        max_pages_per_category: int = 3
    ) -> Dict[str, Any]:
        """Run complete crawling and processing workflow"""

        logger.info("🚀 Starting complete Lazada crawling workflow")

        workflow_start = datetime.now()

        try:
            # Step 1: Crawl products
            logger.info("📦 Step 1: Crawling products from Lazada")
            crawl_result = self.crawler.run_crawler(categories, max_pages_per_category)

            if not crawl_result['success']:
                raise Exception(f"Crawling failed: {crawl_result['error']}")

            raw_products = crawl_result['products']
            logger.info(f"✅ Crawled {len(raw_products)} products")

            # Step 2: Transform products
            logger.info("🔄 Step 2: Transforming product data")
            transformed_products = []

            for product in raw_products:
                transformed = self.processor.transform_lazada_product(product)
                if transformed:
                    transformed_products.append(transformed)

            logger.info(f"✅ Transformed {len(transformed_products)} products")

            # Step 3: Save to databases
            logger.info("💾 Step 3: Saving to databases")

            # PostgreSQL
            pg_success = self.processor.save_to_postgresql(transformed_products)

            # MongoDB (raw data)
            mongo_success = self.processor.save_to_mongodb(raw_products)

            # Kafka (streaming)
            kafka_success = self.processor.send_to_kafka(transformed_products)

            # Redis (statistics)
            self.processor.cache_statistics(transformed_products)

            # Step 4: Generate final report
            workflow_end = datetime.now()
            duration = workflow_end - workflow_start

            final_report = {
                'workflow': {
                    'start_time': workflow_start.isoformat(),
                    'end_time': workflow_end.isoformat(),
                    'duration_seconds': duration.total_seconds(),
                    'status': 'success'
                },
                'crawling': {
                    'categories_crawled': len(categories) if categories else len(self.crawler.electronics_categories),
                    'raw_products_found': len(raw_products),
                    'success_rate': crawl_result['report']['performance']['success_rate']
                },
                'processing': {
                    'transformed_products': len(transformed_products),
                    'transformation_rate': (len(transformed_products) / len(raw_products)) * 100 if raw_products else 0
                },
                'storage': {
                    'postgresql_saved': pg_success,
                    'mongodb_saved': mongo_success,
                    'kafka_sent': kafka_success,
                    'redis_cached': True
                },
                'sample_data': {
                    'categories': list(set(p.get('category', '') for p in transformed_products)),
                    'price_range': {
                        'min': min(p.get('price', 0) for p in transformed_products) if transformed_products else 0,
                        'max': max(p.get('price', 0) for p in transformed_products) if transformed_products else 0,
                        'avg': sum(p.get('price', 0) for p in transformed_products) / len(transformed_products) if transformed_products else 0
                    }
                }
            }

            logger.info("✅ Complete workflow finished successfully")
            return final_report

        except Exception as e:
            logger.error(f"❌ Workflow failed: {e}")
            return {
                'workflow': {
                    'start_time': workflow_start.isoformat(),
                    'end_time': datetime.now().isoformat(),
                    'status': 'failed',
                    'error': str(e)
                }
            }

# Convenience functions for integration
def crawl_lazada_electronics(
    categories: List[str] = None,
    max_pages: int = 3,
    headless: bool = True
) -> Dict[str, Any]:
    """Convenience function for complete Lazada crawling"""

    orchestrator = LazadaCrawlerOrchestrator(headless=headless)
    return asyncio.run(orchestrator.run_complete_workflow(categories, max_pages))

def get_lazada_statistics() -> Dict[str, Any]:
    """Get cached Lazada crawl statistics"""
    try:
        processor = LazadaDataProcessor()
        stats_json = processor.redis_client.get('lazada_crawl_stats')

        if stats_json:
            return json.loads(stats_json)
        else:
            return {'error': 'No cached statistics found'}

    except Exception as e:
        return {'error': str(e)}

# Main execution for testing
if __name__ == "__main__":
    print("""
    🛒 Lazada Integration Module
    ===========================

    Tích hợp hoàn chỉnh Lazada crawler với database systems
    """)

    # Test crawl with small dataset
    result = crawl_lazada_electronics(
        categories=['smartphones', 'headphones'],
        max_pages=1,
        headless=False
    )

    print(f"\n📊 Workflow Result:")
    print(json.dumps(result, indent=2, default=str))