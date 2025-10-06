#!/usr/bin/env python3
"""
Real-time Data Generator for E-commerce Big Data Simulation
Generates high-velocity, realistic e-commerce data streams
"""

import asyncio
import json
import logging
import random
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import uuid
from faker import Faker
import numpy as np

from kafka import KafkaProducer
from pymongo import MongoClient
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker for different locales
fake_vi = Faker('vi_VN')  # Vietnamese
fake_en = Faker('en_US')  # English

@dataclass
class UserEvent:
    """User behavior event data"""
    event_id: str
    user_id: str
    session_id: str
    event_type: str  # page_view, click, search, add_to_cart, purchase, etc.
    timestamp: datetime
    page_url: str
    product_id: Optional[str]
    category: Optional[str]
    search_query: Optional[str]
    device_info: Dict[str, str]
    location: Dict[str, str]
    referrer: Optional[str]
    duration_seconds: Optional[float]

@dataclass
class TransactionEvent:
    """E-commerce transaction event"""
    transaction_id: str
    user_id: str
    product_id: str
    product_name: str
    quantity: int
    unit_price: float
    total_amount: float
    currency: str
    payment_method: str
    shipping_address: Dict[str, str]
    transaction_status: str  # pending, completed, failed, refunded
    timestamp: datetime
    device_type: str
    platform: str  # web, mobile_app, api

@dataclass
class InventoryUpdate:
    """Product inventory update event"""
    product_id: str
    sku: str
    current_stock: int
    previous_stock: int
    change_type: str  # sale, restock, adjustment, return
    warehouse_id: str
    supplier_id: Optional[str]
    timestamp: datetime
    reason: Optional[str]

@dataclass
class PriceUpdate:
    """Product price change event"""
    product_id: str
    old_price: float
    new_price: float
    currency: str
    change_reason: str  # promotion, competitor, cost_change, algorithm
    effective_date: datetime
    expiry_date: Optional[datetime]
    discount_percentage: Optional[float]
    promotion_id: Optional[str]

class RealtimeDataGenerator:
    """High-performance real-time data generator"""

    def __init__(self):
        self.setup_connections()
        self.initialize_data_pools()

        # Generation rates (events per minute)
        self.generation_rates = {
            'user_events': 1000,      # 1000 user events/minute
            'transactions': 100,       # 100 transactions/minute
            'inventory_updates': 50,   # 50 inventory updates/minute
            'price_updates': 20        # 20 price updates/minute
        }

        # Business logic parameters
        self.business_params = {
            'peak_hours': [9, 12, 15, 18, 21],  # Peak traffic hours
            'conversion_rate': 0.03,  # 3% conversion rate
            'average_cart_value': 850000,  # 850k VND
            'return_rate': 0.05,  # 5% return rate
            'mobile_traffic_ratio': 0.7  # 70% mobile traffic
        }

        self.running = False

    def setup_connections(self):
        """Setup database connections"""
        try:
            # Kafka producer with optimized settings
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                batch_size=16384,  # 16KB batches
                linger_ms=5,       # 5ms linger time
                acks=1,            # Wait for leader acknowledgment
                retries=3
            )

            # MongoDB connection
            self.mongo_client = MongoClient('mongodb://localhost:27017/')
            self.mongo_db = self.mongo_client.dss_streaming

            # Redis for caching and state management
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

            logger.info("✅ Real-time generator connections established")

        except Exception as e:
            logger.error(f"❌ Failed to setup connections: {e}")
            raise

    def initialize_data_pools(self):
        """Initialize realistic data pools for generation"""
        # Product categories
        self.categories = [
            'Electronics', 'Fashion', 'Home & Garden', 'Books', 'Sports',
            'Beauty', 'Automotive', 'Toys', 'Health', 'Food & Beverage'
        ]

        # Vietnamese cities for realistic location data
        self.vietnamese_cities = [
            'Ho Chi Minh City', 'Hanoi', 'Da Nang', 'Can Tho', 'Hai Phong',
            'Bien Hoa', 'Hue', 'Nha Trang', 'Buon Ma Thuot', 'Quy Nhon'
        ]

        # Device types and platforms
        self.devices = ['mobile', 'desktop', 'tablet']
        self.platforms = ['web', 'mobile_app', 'api']
        self.browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
        self.operating_systems = ['iOS', 'Android', 'Windows', 'macOS', 'Linux']

        # Payment methods
        self.payment_methods = [
            'credit_card', 'debit_card', 'bank_transfer', 'e_wallet',
            'cash_on_delivery', 'installment', 'crypto'
        ]

        # E-commerce event types
        self.event_types = [
            'page_view', 'product_view', 'category_browse', 'search',
            'add_to_cart', 'remove_from_cart', 'checkout_start', 'checkout_complete',
            'add_to_wishlist', 'share_product', 'review_submit', 'filter_apply'
        ]

        # Generate product pools
        self.generate_product_pools()

        # Generate user pools
        self.generate_user_pools()

        logger.info("✅ Data pools initialized")

    def generate_product_pools(self):
        """Generate realistic product data pools"""
        self.products = []

        for _ in range(10000):  # 10K products
            category = random.choice(self.categories)
            product_id = f"prod_{random.randint(100000, 999999)}"

            # Generate realistic Vietnamese product names
            if random.choice([True, False]):
                product_name = fake_vi.catch_phrase()
            else:
                product_name = fake_en.catch_phrase()

            # Realistic pricing based on category
            price_ranges = {
                'Electronics': (500000, 50000000),    # 500K - 50M VND
                'Fashion': (100000, 5000000),         # 100K - 5M VND
                'Home & Garden': (50000, 10000000),   # 50K - 10M VND
                'Books': (20000, 500000),             # 20K - 500K VND
                'Sports': (100000, 15000000),         # 100K - 15M VND
                'Beauty': (50000, 2000000),           # 50K - 2M VND
                'Automotive': (100000, 100000000),    # 100K - 100M VND
                'Toys': (50000, 3000000),             # 50K - 3M VND
                'Health': (30000, 5000000),           # 30K - 5M VND
                'Food & Beverage': (10000, 1000000)   # 10K - 1M VND
            }

            min_price, max_price = price_ranges.get(category, (50000, 5000000))
            price = random.randint(min_price, max_price)

            product = {
                'product_id': product_id,
                'name': product_name,
                'category': category,
                'price': price,
                'stock': random.randint(0, 1000),
                'rating': round(random.uniform(3.0, 5.0), 1),
                'review_count': random.randint(0, 1000)
            }

            self.products.append(product)

    def generate_user_pools(self):
        """Generate realistic user data pools"""
        self.users = []

        for _ in range(50000):  # 50K users
            user_id = f"user_{random.randint(100000, 999999)}"

            # Mix of Vietnamese and international users
            if random.choice([True, False, True]):  # 2/3 Vietnamese users
                name = fake_vi.name()
                city = random.choice(self.vietnamese_cities)
                country = 'Vietnam'
            else:
                name = fake_en.name()
                city = fake_en.city()
                country = fake_en.country()

            user = {
                'user_id': user_id,
                'name': name,
                'city': city,
                'country': country,
                'registration_date': fake_vi.date_between(start_date='-2y', end_date='today'),
                'preferred_device': random.choice(self.devices),
                'lifetime_value': random.randint(0, 50000000)  # 0 - 50M VND
            }

            self.users.append(user)

    def get_current_traffic_multiplier(self) -> float:
        """Calculate traffic multiplier based on time of day"""
        current_hour = datetime.now().hour

        # Peak hours have 3x traffic
        if current_hour in self.business_params['peak_hours']:
            return 3.0
        # Night hours (0-6) have 0.3x traffic
        elif 0 <= current_hour <= 6:
            return 0.3
        # Regular hours have 1x traffic
        else:
            return 1.0

    def generate_user_event(self) -> UserEvent:
        """Generate realistic user behavior event"""
        user = random.choice(self.users)
        event_type = random.choice(self.event_types)

        # Generate session ID (users can have multiple sessions)
        session_id = f"sess_{random.randint(100000, 999999)}"

        # Device info based on user preference and randomness
        if random.random() < 0.8:  # 80% chance to use preferred device
            device_type = user['preferred_device']
        else:
            device_type = random.choice(self.devices)

        device_info = {
            'type': device_type,
            'browser': random.choice(self.browsers),
            'os': random.choice(self.operating_systems),
            'screen_resolution': random.choice(['1920x1080', '1366x768', '375x667', '414x896'])
        }

        # Location info
        location = {
            'city': user['city'],
            'country': user['country'],
            'ip_address': fake_en.ipv4()
        }

        # Event-specific data
        product_id = None
        category = None
        search_query = None
        page_url = 'https://ecommerce-site.com/'

        if event_type in ['product_view', 'add_to_cart', 'remove_from_cart']:
            product = random.choice(self.products)
            product_id = product['product_id']
            category = product['category']
            page_url += f"products/{product_id}"

        elif event_type == 'category_browse':
            category = random.choice(self.categories)
            page_url += f"category/{category.lower().replace(' ', '-')}"

        elif event_type == 'search':
            search_query = random.choice([
                fake_vi.word(), fake_en.word(),
                random.choice(self.categories).lower(),
                fake_vi.catch_phrase()[:20]
            ])
            page_url += f"search?q={search_query}"

        # Duration (in seconds)
        duration_mapping = {
            'page_view': random.uniform(5, 30),
            'product_view': random.uniform(30, 180),
            'category_browse': random.uniform(10, 60),
            'search': random.uniform(5, 45),
            'add_to_cart': random.uniform(2, 10),
            'checkout_start': random.uniform(60, 300),
            'checkout_complete': random.uniform(30, 120)
        }

        duration = duration_mapping.get(event_type, random.uniform(5, 30))

        return UserEvent(
            event_id=str(uuid.uuid4()),
            user_id=user['user_id'],
            session_id=session_id,
            event_type=event_type,
            timestamp=datetime.now(),
            page_url=page_url,
            product_id=product_id,
            category=category,
            search_query=search_query,
            device_info=device_info,
            location=location,
            referrer=random.choice([None, 'google.com', 'facebook.com', 'direct']),
            duration_seconds=round(duration, 2)
        )

    def generate_transaction_event(self) -> TransactionEvent:
        """Generate realistic transaction event"""
        user = random.choice(self.users)
        product = random.choice(self.products)

        # Quantity based on product type and price
        if product['price'] > 10000000:  # Expensive items
            quantity = 1
        elif product['price'] < 100000:   # Cheap items
            quantity = random.randint(1, 5)
        else:
            quantity = random.randint(1, 3)

        total_amount = product['price'] * quantity

        # Add shipping cost
        shipping_cost = random.choice([0, 30000, 50000, 100000])  # Free, standard, fast, express
        total_amount += shipping_cost

        # Transaction status (most are completed)
        status_weights = [0.85, 0.05, 0.05, 0.05]  # completed, pending, failed, refunded
        transaction_status = random.choices(
            ['completed', 'pending', 'failed', 'refunded'],
            weights=status_weights
        )[0]

        # Shipping address
        shipping_address = {
            'city': user['city'],
            'country': user['country'],
            'address': fake_vi.address() if user['country'] == 'Vietnam' else fake_en.address(),
            'postal_code': fake_vi.postcode() if user['country'] == 'Vietnam' else fake_en.postcode()
        }

        return TransactionEvent(
            transaction_id=f"txn_{random.randint(1000000, 9999999)}",
            user_id=user['user_id'],
            product_id=product['product_id'],
            product_name=product['name'],
            quantity=quantity,
            unit_price=product['price'],
            total_amount=total_amount,
            currency='VND',
            payment_method=random.choice(self.payment_methods),
            shipping_address=shipping_address,
            transaction_status=transaction_status,
            timestamp=datetime.now(),
            device_type=random.choice(self.devices),
            platform=random.choice(self.platforms)
        )

    def generate_inventory_update(self) -> InventoryUpdate:
        """Generate inventory update event"""
        product = random.choice(self.products)

        # Different types of inventory changes
        change_types = ['sale', 'restock', 'adjustment', 'return']
        change_weights = [0.6, 0.2, 0.1, 0.1]
        change_type = random.choices(change_types, weights=change_weights)[0]

        previous_stock = product['stock']

        if change_type == 'sale':
            change_amount = -random.randint(1, min(5, previous_stock))
        elif change_type == 'restock':
            change_amount = random.randint(50, 500)
        elif change_type == 'return':
            change_amount = random.randint(1, 3)
        else:  # adjustment
            change_amount = random.randint(-10, 10)

        current_stock = max(0, previous_stock + change_amount)

        # Update product stock in memory
        product['stock'] = current_stock

        return InventoryUpdate(
            product_id=product['product_id'],
            sku=f"SKU_{product['product_id'][-6:]}",
            current_stock=current_stock,
            previous_stock=previous_stock,
            change_type=change_type,
            warehouse_id=f"WH_{random.randint(1, 10):02d}",
            supplier_id=f"SUP_{random.randint(100, 999)}" if change_type == 'restock' else None,
            timestamp=datetime.now(),
            reason=f"Automated {change_type} update"
        )

    def generate_price_update(self) -> PriceUpdate:
        """Generate price update event"""
        product = random.choice(self.products)
        old_price = product['price']

        # Price change reasons and their typical changes
        change_reasons = {
            'promotion': (-0.3, -0.05),      # 5-30% discount
            'competitor': (-0.15, 0.15),     # ±15% adjustment
            'cost_change': (-0.1, 0.2),      # Cost-based adjustment
            'algorithm': (-0.05, 0.05)       # Small algorithmic adjustment
        }

        reason = random.choice(list(change_reasons.keys()))
        min_change, max_change = change_reasons[reason]
        price_multiplier = 1 + random.uniform(min_change, max_change)

        new_price = round(old_price * price_multiplier, -3)  # Round to nearest 1000 VND

        # Update product price in memory
        product['price'] = new_price

        # Calculate discount percentage
        discount_percentage = None
        if new_price < old_price:
            discount_percentage = round(((old_price - new_price) / old_price) * 100, 2)

        # Effective and expiry dates
        effective_date = datetime.now()
        expiry_date = None
        if reason == 'promotion':
            expiry_date = effective_date + timedelta(days=random.randint(1, 30))

        return PriceUpdate(
            product_id=product['product_id'],
            old_price=old_price,
            new_price=new_price,
            currency='VND',
            change_reason=reason,
            effective_date=effective_date,
            expiry_date=expiry_date,
            discount_percentage=discount_percentage,
            promotion_id=f"PROMO_{random.randint(1000, 9999)}" if reason == 'promotion' else None
        )

    async def send_to_kafka(self, data: Any, topic: str, key: Optional[str] = None):
        """Send data to Kafka topic"""
        try:
            data_dict = asdict(data) if hasattr(data, '__dict__') else data

            self.kafka_producer.send(
                topic,
                key=key,
                value=data_dict
            )

        except Exception as e:
            logger.error(f"❌ Error sending to Kafka topic {topic}: {e}")

    async def save_to_mongodb(self, data: Any, collection_name: str):
        """Save data to MongoDB collection"""
        try:
            data_dict = asdict(data) if hasattr(data, '__dict__') else data
            getattr(self.mongo_db, collection_name).insert_one(data_dict)

        except Exception as e:
            logger.error(f"❌ Error saving to MongoDB collection {collection_name}: {e}")

    async def generate_user_events_stream(self):
        """Generate continuous stream of user events"""
        while self.running:
            try:
                traffic_multiplier = self.get_current_traffic_multiplier()
                rate = self.generation_rates['user_events'] * traffic_multiplier
                interval = 60 / rate  # seconds between events

                event = self.generate_user_event()

                # Send to both Kafka and MongoDB
                await self.send_to_kafka(event, 'user_events', event.user_id)
                await self.save_to_mongodb(event, 'user_events_raw')

                await asyncio.sleep(interval)

            except Exception as e:
                logger.error(f"❌ Error in user events stream: {e}")
                await asyncio.sleep(1)

    async def generate_transactions_stream(self):
        """Generate continuous stream of transactions"""
        while self.running:
            try:
                traffic_multiplier = self.get_current_traffic_multiplier()
                rate = self.generation_rates['transactions'] * traffic_multiplier
                interval = 60 / rate

                transaction = self.generate_transaction_event()

                # Send to both Kafka and MongoDB
                await self.send_to_kafka(transaction, 'transactions', transaction.transaction_id)
                await self.save_to_mongodb(transaction, 'transactions_raw')

                await asyncio.sleep(interval)

            except Exception as e:
                logger.error(f"❌ Error in transactions stream: {e}")
                await asyncio.sleep(5)

    async def generate_inventory_stream(self):
        """Generate continuous stream of inventory updates"""
        while self.running:
            try:
                rate = self.generation_rates['inventory_updates']
                interval = 60 / rate

                inventory_update = self.generate_inventory_update()

                # Send to both Kafka and MongoDB
                await self.send_to_kafka(inventory_update, 'inventory_updates', inventory_update.product_id)
                await self.save_to_mongodb(inventory_update, 'inventory_updates_raw')

                await asyncio.sleep(interval)

            except Exception as e:
                logger.error(f"❌ Error in inventory stream: {e}")
                await asyncio.sleep(10)

    async def generate_price_updates_stream(self):
        """Generate continuous stream of price updates"""
        while self.running:
            try:
                rate = self.generation_rates['price_updates']
                interval = 60 / rate

                price_update = self.generate_price_update()

                # Send to both Kafka and MongoDB
                await self.send_to_kafka(price_update, 'price_updates', price_update.product_id)
                await self.save_to_mongodb(price_update, 'price_updates_raw')

                await asyncio.sleep(interval)

            except Exception as e:
                logger.error(f"❌ Error in price updates stream: {e}")
                await asyncio.sleep(15)

    async def run_statistics_logger(self):
        """Log generation statistics periodically"""
        start_time = datetime.now()

        while self.running:
            try:
                # Calculate statistics
                current_time = datetime.now()
                running_time = (current_time - start_time).total_seconds()

                # Get current rates
                traffic_multiplier = self.get_current_traffic_multiplier()

                stats = {
                    'timestamp': current_time.isoformat(),
                    'running_time_minutes': round(running_time / 60, 2),
                    'traffic_multiplier': traffic_multiplier,
                    'current_rates': {
                        'user_events_per_minute': round(self.generation_rates['user_events'] * traffic_multiplier),
                        'transactions_per_minute': round(self.generation_rates['transactions'] * traffic_multiplier),
                        'inventory_updates_per_minute': self.generation_rates['inventory_updates'],
                        'price_updates_per_minute': self.generation_rates['price_updates']
                    }
                }

                logger.info(f"📊 Generation Stats: {json.dumps(stats, indent=2)}")

                # Store stats in Redis
                self.redis_client.setex(
                    'realtime_generator_stats',
                    300,  # 5 minutes TTL
                    json.dumps(stats)
                )

                await asyncio.sleep(60)  # Log every minute

            except Exception as e:
                logger.error(f"❌ Error in statistics logger: {e}")
                await asyncio.sleep(60)

    async def start_generation(self):
        """Start all data generation streams"""
        logger.info("🚀 Starting real-time data generation...")

        self.running = True

        # Start all generation tasks concurrently
        tasks = [
            asyncio.create_task(self.generate_user_events_stream()),
            asyncio.create_task(self.generate_transactions_stream()),
            asyncio.create_task(self.generate_inventory_stream()),
            asyncio.create_task(self.generate_price_updates_stream()),
            asyncio.create_task(self.run_statistics_logger())
        ]

        try:
            # Run all tasks concurrently
            await asyncio.gather(*tasks)

        except KeyboardInterrupt:
            logger.info("🛑 Stopping data generation...")
            self.running = False

            # Wait for tasks to complete
            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

        finally:
            # Flush remaining messages
            self.kafka_producer.flush()
            logger.info("✅ Real-time data generation stopped")

    def close_connections(self):
        """Close all connections"""
        try:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()
            logger.info("🔌 Generator connections closed")
        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")

# Main execution
async def main():
    """Main execution function"""
    generator = RealtimeDataGenerator()

    try:
        await generator.start_generation()
    finally:
        generator.close_connections()

if __name__ == "__main__":
    asyncio.run(main())