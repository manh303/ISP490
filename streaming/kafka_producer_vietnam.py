#!/usr/bin/env python3
"""
Vietnam E-commerce Kafka Producer
=================================
Real-time data producer for Vietnam e-commerce data warehouse
Generates realistic Vietnamese e-commerce events and streams to Kafka

Features:
- Vietnamese customer data generation
- Vietnam-specific product catalog
- Realistic order patterns for Vietnam market
- Support for major platforms: Shopee, Lazada, Tiki, Sendo
- Cultural event simulation (Tet, festivals)
- Multiple data formats: JSON, Avro

Author: DSS Team
Version: 1.0.0
"""

import os
import sys
import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import threading
from concurrent.futures import ThreadPoolExecutor

# Kafka imports
from kafka import KafkaProducer
from kafka.errors import KafkaError
# import avro.schema
# import avro.io
import io

# Data generation
import pandas as pd
import numpy as np
from faker import Faker
import uuid

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Vietnamese Faker
fake = Faker('vi_VN')
fake_en = Faker('en_US')

# ====================================================================
# VIETNAM E-COMMERCE DATA MODELS
# ====================================================================

@dataclass
class VietnameseCustomer:
    """Vietnamese Customer Model"""
    customer_id: str
    full_name: str
    email: str
    phone: str
    date_of_birth: str
    age: int
    gender: str
    city: str
    district: str
    ward: str
    province: str
    region: str
    postal_code: str
    address: str
    income_level: str
    customer_segment: str
    preferred_device: str
    preferred_platform: str
    preferred_payment: str
    registration_date: str
    created_at: str

@dataclass
class VietnameseProduct:
    """Vietnamese Product Model"""
    product_id: str
    product_name_vn: str
    product_name_en: str
    brand: str
    category_l1: str
    category_l2: str
    category_l3: str
    price_vnd: int
    price_usd: float
    discount_percent: float
    rating: float
    review_count: int
    stock_quantity: int
    is_featured: bool
    available_platforms: List[str]
    payment_methods: List[str]
    vietnam_popularity: float
    made_in_vietnam: bool
    launch_date: str
    created_at: str

@dataclass
class VietnameseSalesEvent:
    """Vietnamese Sales Event Model"""
    event_id: str
    order_id: str
    customer_id: str
    product_id: str
    platform: str
    quantity: int
    unit_price_vnd: int
    unit_price_usd: float
    total_amount_vnd: int
    total_amount_usd: float
    discount_amount_vnd: int
    tax_amount_vnd: int
    shipping_fee_vnd: int
    payment_method: str
    shipping_method: str
    order_status: str
    is_cod: bool
    is_tet_order: bool
    is_festival_order: bool
    order_source: str
    device_type: str
    shipping_province: str
    shipping_region: str
    event_timestamp: str
    order_timestamp: str

# ====================================================================
# VIETNAM DATA GENERATORS
# ====================================================================

class VietnameseDataGenerator:
    """Generate realistic Vietnamese e-commerce data"""

    def __init__(self):
        # Vietnamese provinces mapping
        self.provinces = {
            'Hà Nội': {'region': 'Miền Bắc', 'postal': '100000', 'code': 'HN'},
            'TP. Hồ Chí Minh': {'region': 'Miền Nam', 'postal': '700000', 'code': 'HCM'},
            'Đà Nẵng': {'region': 'Miền Trung', 'postal': '550000', 'code': 'DN'},
            'Hải Phòng': {'region': 'Miền Bắc', 'postal': '180000', 'code': 'HP'},
            'Cần Thơ': {'region': 'Miền Nam', 'postal': '900000', 'code': 'CT'},
            'Nghệ An': {'region': 'Miền Trung', 'postal': '460000', 'code': 'NA'},
            'Thanh Hóa': {'region': 'Miền Trung', 'postal': '440000', 'code': 'TH'},
            'Quảng Ninh': {'region': 'Miền Bắc', 'postal': '200000', 'code': 'QN'},
            'Bình Dương': {'region': 'Miền Nam', 'postal': '750000', 'code': 'BD'},
            'Đồng Nai': {'region': 'Miền Nam', 'postal': '760000', 'code': 'DN2'}
        }

        # Vietnamese platforms with market share
        self.platforms = {
            'Shopee': {'market_share': 0.352, 'commission': 0.06},
            'Lazada': {'market_share': 0.285, 'commission': 0.055},
            'Tiki': {'market_share': 0.158, 'commission': 0.08},
            'Sendo': {'market_share': 0.103, 'commission': 0.05},
            'FPT Shop': {'market_share': 0.052, 'commission': 0.04},
            'CellphoneS': {'market_share': 0.035, 'commission': 0.035},
            'Thế Giới Di Động': {'market_share': 0.015, 'commission': 0.03}
        }

        # Vietnamese payment methods
        self.payment_methods = {
            'COD': 0.45,  # Cash on Delivery - very popular in Vietnam
            'MoMo': 0.25,  # Most popular e-wallet
            'ZaloPay': 0.15,  # Zalo ecosystem
            'VNPay': 0.08,   # Banking integration
            'Banking': 0.05,  # Direct bank transfer
            'Credit_Card': 0.02  # Less popular in Vietnam
        }

        # Vietnamese product categories
        self.product_categories = {
            'Điện tử': ['Điện thoại', 'Laptop', 'Tablet', 'Phụ kiện', 'Tivi', 'Âm thanh'],
            'Thời trang': ['Quần áo nam', 'Quần áo nữ', 'Giày dép', 'Túi xách', 'Phụ kiện'],
            'Gia dụng': ['Nội thất', 'Đồ dùng nhà bếp', 'Đồ gia dụng', 'Điện lạnh'],
            'Làm đẹp': ['Mỹ phẩm', 'Chăm sóc da', 'Nước hoa', 'Dụng cụ làm đẹp'],
            'Sức khỏe': ['Thực phẩm chức năng', 'Dụng cụ y tế', 'Thuốc', 'Vitamin'],
            'Thể thao': ['Quần áo thể thao', 'Giày thể thao', 'Dụng cụ tập luyện'],
            'Mẹ và bé': ['Đồ cho bé', 'Đồ chơi', 'Sữa bột', 'Tã em bé'],
            'Sách': ['Sách văn học', 'Sách giáo khoa', 'Truyện tranh', 'Sách kỹ năng']
        }

        # Vietnamese brands
        self.vietnamese_brands = [
            'FPT', 'Viettel', 'VinSmart', 'Bkav', 'CMC', 'TH True Milk',
            'Kinh Đô', 'Phúc Long', 'Highlands Coffee', 'Trung Nguyên',
            'An Nam', 'Biti\'s', 'NEM', 'Canifa', 'Yame', 'Routine'
        ]

        self.international_brands = [
            'Samsung', 'Apple', 'Xiaomi', 'Oppo', 'Vivo', 'Huawei',
            'Sony', 'LG', 'Panasonic', 'Nike', 'Adidas', 'Uniqlo'
        ]

    def generate_vietnamese_customer(self) -> VietnameseCustomer:
        """Generate a realistic Vietnamese customer"""
        province = random.choice(list(self.provinces.keys()))
        province_info = self.provinces[province]

        # Generate Vietnamese name
        full_name = fake.name()

        # Generate realistic email
        email_prefix = ''.join(full_name.lower().split())
        email_domain = random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 'fpt.edu.vn'])
        email = f"{email_prefix}{random.randint(1, 999)}@{email_domain}"

        # Generate Vietnamese phone number
        phone_prefixes = ['032', '033', '034', '035', '036', '037', '038', '039',
                         '070', '076', '077', '078', '079', '081', '082', '083', '084', '085']
        phone = f"+84{random.choice(phone_prefixes)}{random.randint(1000000, 9999999)}"

        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=65)
        age = (datetime.now().date() - birth_date).days // 365

        return VietnameseCustomer(
            customer_id=f"VN_CUST_{uuid.uuid4().hex[:8].upper()}",
            full_name=full_name,
            email=email,
            phone=phone,
            date_of_birth=birth_date.isoformat(),
            age=age,
            gender=random.choice(['Nam', 'Nữ']),
            city=province,
            district=f"Quận {random.randint(1, 12)}" if province in ['Hà Nội', 'TP. Hồ Chí Minh'] else f"Huyện {fake.city()}",
            ward=f"Phường {random.randint(1, 20)}",
            province=province,
            region=province_info['region'],
            postal_code=province_info['postal'],
            address=fake.address(),
            income_level=random.choices(['Thấp', 'Trung bình', 'Cao'], weights=[0.4, 0.5, 0.1])[0],
            customer_segment=random.choices(['Thường', 'VIP', 'Cao cấp'], weights=[0.7, 0.25, 0.05])[0],
            preferred_device=random.choices(['Mobile', 'Desktop', 'Tablet'], weights=[0.75, 0.2, 0.05])[0],
            preferred_platform=random.choices(list(self.platforms.keys()),
                                            weights=[p['market_share'] for p in self.platforms.values()])[0],
            preferred_payment=random.choices(list(self.payment_methods.keys()),
                                           weights=list(self.payment_methods.values()))[0],
            registration_date=(datetime.now() - timedelta(days=random.randint(1, 1460))).isoformat(),
            created_at=datetime.now().isoformat()
        )

    def generate_vietnamese_product(self) -> VietnameseProduct:
        """Generate a realistic Vietnamese product"""
        category_l1 = random.choice(list(self.product_categories.keys()))
        category_l2 = random.choice(self.product_categories[category_l1])

        # Generate Vietnamese product name
        brand = random.choice(self.vietnamese_brands + self.international_brands)
        made_in_vietnam = brand in self.vietnamese_brands

        product_names = {
            'Điện thoại': f"{brand} {random.choice(['Galaxy', 'iPhone', 'Mi', 'Reno', 'V'])} {random.randint(10, 15)}",
            'Laptop': f"{brand} {random.choice(['ThinkPad', 'MacBook', 'Pavilion', 'Inspiron'])} {random.randint(2020, 2024)}",
            'Quần áo nam': f"Áo {random.choice(['thun', 'sơ mi', 'polo'])} {brand}",
            'Mỹ phẩm': f"Kem {random.choice(['dưỡng da', 'chống nắng', 'trang điểm'])} {brand}"
        }

        product_name_vn = product_names.get(category_l2, f"{category_l2} {brand} {random.randint(1, 100)}")
        product_name_en = product_name_vn  # Simplified for this example

        # Price generation (VND)
        base_price_vnd = random.randint(50000, 50000000)  # 50K to 50M VND
        price_usd = base_price_vnd / 24000  # Convert to USD

        # Available platforms based on market presence
        available_platforms = random.sample(
            list(self.platforms.keys()),
            k=random.randint(1, min(4, len(self.platforms)))
        )

        # Payment methods supported
        payment_methods = random.sample(
            list(self.payment_methods.keys()),
            k=random.randint(2, len(self.payment_methods))
        )

        return VietnameseProduct(
            product_id=f"VN_PROD_{uuid.uuid4().hex[:8].upper()}",
            product_name_vn=product_name_vn,
            product_name_en=product_name_en,
            brand=brand,
            category_l1=category_l1,
            category_l2=category_l2,
            category_l3=random.choice(['Cao cấp', 'Phổ thông', 'Giá rẻ']),
            price_vnd=base_price_vnd,
            price_usd=round(price_usd, 2),
            discount_percent=round(random.uniform(0, 50), 2),
            rating=round(random.uniform(1, 5), 2),
            review_count=random.randint(0, 5000),
            stock_quantity=random.randint(0, 1000),
            is_featured=random.choice([True, False]),
            available_platforms=available_platforms,
            payment_methods=payment_methods,
            vietnam_popularity=round(random.uniform(0, 1), 3),
            made_in_vietnam=made_in_vietnam,
            launch_date=(datetime.now() - timedelta(days=random.randint(1, 730))).isoformat(),
            created_at=datetime.now().isoformat()
        )

    def generate_sales_event(self, customer: VietnameseCustomer, product: VietnameseProduct) -> VietnameseSalesEvent:
        """Generate a realistic Vietnamese sales event"""

        # Choose platform based on customer preference and product availability
        available_platforms = list(set([customer.preferred_platform] + product.available_platforms))
        platform = random.choice(available_platforms)

        # Quantity and pricing
        quantity = random.randint(1, 5)
        unit_price_vnd = int(product.price_vnd * (1 - product.discount_percent / 100))
        unit_price_usd = round(unit_price_vnd / 24000, 2)

        total_amount_vnd = unit_price_vnd * quantity
        total_amount_usd = round(total_amount_vnd / 24000, 2)

        # Fees and taxes (Vietnam specific)
        discount_amount_vnd = int(product.price_vnd * quantity * product.discount_percent / 100)
        tax_amount_vnd = int(total_amount_vnd * 0.1)  # 10% VAT
        shipping_fee_vnd = random.choice([0, 15000, 25000, 35000])  # Free or paid shipping

        # Payment method
        payment_method = customer.preferred_payment
        is_cod = payment_method == 'COD'

        # Cultural events
        current_date = datetime.now()
        is_tet_order = self._is_tet_season(current_date)
        is_festival_order = self._is_shopping_festival(current_date)

        return VietnameseSalesEvent(
            event_id=f"EVT_{uuid.uuid4().hex[:12].upper()}",
            order_id=f"ORD_{uuid.uuid4().hex[:10].upper()}",
            customer_id=customer.customer_id,
            product_id=product.product_id,
            platform=platform,
            quantity=quantity,
            unit_price_vnd=unit_price_vnd,
            unit_price_usd=unit_price_usd,
            total_amount_vnd=total_amount_vnd,
            total_amount_usd=total_amount_usd,
            discount_amount_vnd=discount_amount_vnd,
            tax_amount_vnd=tax_amount_vnd,
            shipping_fee_vnd=shipping_fee_vnd,
            payment_method=payment_method,
            shipping_method=random.choice(['Standard', 'Express', 'Same_Day']),
            order_status=random.choices(['Pending', 'Confirmed', 'Shipped', 'Delivered'],
                                      weights=[0.1, 0.2, 0.3, 0.4])[0],
            is_cod=is_cod,
            is_tet_order=is_tet_order,
            is_festival_order=is_festival_order,
            order_source=random.choices(['Mobile_App', 'Website', 'Social_Media'], weights=[0.7, 0.25, 0.05])[0],
            device_type=customer.preferred_device,
            shipping_province=customer.province,
            shipping_region=customer.region,
            event_timestamp=datetime.now().isoformat(),
            order_timestamp=datetime.now().isoformat()
        )

    def _is_tet_season(self, date: datetime) -> bool:
        """Check if date is in Tet season (Vietnamese New Year)"""
        # Simplified: January 15 - February 20
        return (date.month == 1 and date.day >= 15) or (date.month == 2 and date.day <= 20)

    def _is_shopping_festival(self, date: datetime) -> bool:
        """Check if date is a shopping festival"""
        # 9/9, 10/10, 11/11, 12/12
        return (date.month == 9 and date.day == 9) or \
               (date.month == 10 and date.day == 10) or \
               (date.month == 11 and date.day == 11) or \
               (date.month == 12 and date.day == 12)

# ====================================================================
# KAFKA PRODUCER
# ====================================================================

class VietnamEcommerceKafkaProducer:
    """Kafka Producer for Vietnam E-commerce Data"""

    def __init__(self,
                 bootstrap_servers: str = 'localhost:9092',
                 topics_config: Dict[str, Dict] = None):

        self.bootstrap_servers = bootstrap_servers
        self.data_generator = VietnameseDataGenerator()
        self.running = False
        self.threads = []

        # Default topics configuration
        self.topics_config = topics_config or {
            'vietnam_customers': {
                'partitions': 3,
                'replication_factor': 1,
                'retention_ms': 2592000000,  # 30 days
                'rate_per_second': 5
            },
            'vietnam_products': {
                'partitions': 4,
                'replication_factor': 1,
                'retention_ms': 1209600000,  # 14 days
                'rate_per_second': 3
            },
            'vietnam_sales_events': {
                'partitions': 6,
                'replication_factor': 1,
                'retention_ms': 604800000,  # 7 days
                'rate_per_second': 20
            },
            'vietnam_user_activities': {
                'partitions': 4,
                'replication_factor': 1,
                'retention_ms': 86400000,  # 1 day
                'rate_per_second': 50
            }
        }

        # Initialize Kafka producer
        self.producer = None
        self._init_producer()

        logger.info(f"Vietnam E-commerce Kafka Producer initialized")
        logger.info(f"Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Topics: {list(self.topics_config.keys())}")

    def _init_producer(self):
        """Initialize Kafka producer with optimized settings"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                batch_size=16384,  # 16KB batches
                linger_ms=10,  # Wait 10ms for batching
                buffer_memory=33554432,  # 32MB buffer
                compression_type='gzip',  # Compress messages
                enable_idempotence=True,  # Exactly-once semantics
                max_in_flight_requests_per_connection=1  # Ordering guarantee
            )
            logger.info("✅ Kafka producer initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise

    def create_topics(self):
        """Create Kafka topics with Vietnam-specific configuration"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic

            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='vietnam_ecommerce_admin'
            )

            topics_to_create = []

            for topic_name, config in self.topics_config.items():
                topic = NewTopic(
                    name=topic_name,
                    num_partitions=config['partitions'],
                    replication_factor=config['replication_factor'],
                    topic_configs={
                        'retention.ms': str(config['retention_ms']),
                        'compression.type': 'snappy',
                        'cleanup.policy': 'delete'
                    }
                )
                topics_to_create.append(topic)

            # Create topics
            result = admin_client.create_topics(topics_to_create, validate_only=False)

            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic_name}' created successfully")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(f"ℹ️ Topic '{topic_name}' already exists")
                    else:
                        logger.error(f"❌ Failed to create topic '{topic_name}': {e}")

            admin_client.close()

        except Exception as e:
            logger.warning(f"⚠️ Failed to create topics: {e}")

    def send_customer_data(self, rate_per_second: int = 5):
        """Send customer data to Kafka"""
        logger.info(f"🚀 Starting customer data stream at {rate_per_second} records/second")

        while self.running:
            try:
                customer = self.data_generator.generate_vietnamese_customer()
                customer_dict = asdict(customer)

                # Send to Kafka
                future = self.producer.send(
                    'vietnam_customers',
                    key=customer.customer_id,
                    value=customer_dict
                )

                # Add metadata
                customer_dict['_metadata'] = {
                    'producer_timestamp': datetime.now().isoformat(),
                    'topic': 'vietnam_customers',
                    'partition_key': customer.customer_id
                }

                logger.debug(f"📤 Customer sent: {customer.customer_id}")

                time.sleep(1.0 / rate_per_second)

            except Exception as e:
                logger.error(f"❌ Error sending customer data: {e}")
                time.sleep(1)

    def send_product_data(self, rate_per_second: int = 3):
        """Send product data to Kafka"""
        logger.info(f"🚀 Starting product data stream at {rate_per_second} records/second")

        while self.running:
            try:
                product = self.data_generator.generate_vietnamese_product()
                product_dict = asdict(product)

                # Send to Kafka
                future = self.producer.send(
                    'vietnam_products',
                    key=product.product_id,
                    value=product_dict
                )

                # Add metadata
                product_dict['_metadata'] = {
                    'producer_timestamp': datetime.now().isoformat(),
                    'topic': 'vietnam_products',
                    'partition_key': product.product_id
                }

                logger.debug(f"📤 Product sent: {product.product_id}")

                time.sleep(1.0 / rate_per_second)

            except Exception as e:
                logger.error(f"❌ Error sending product data: {e}")
                time.sleep(1)

    def send_sales_events(self, rate_per_second: int = 20):
        """Send sales events to Kafka"""
        logger.info(f"🚀 Starting sales events stream at {rate_per_second} events/second")

        # Pre-generate some customers and products for realistic sales
        customers = [self.data_generator.generate_vietnamese_customer() for _ in range(100)]
        products = [self.data_generator.generate_vietnamese_product() for _ in range(200)]

        while self.running:
            try:
                # Generate sales event
                customer = random.choice(customers)
                product = random.choice(products)
                sales_event = self.data_generator.generate_sales_event(customer, product)
                sales_dict = asdict(sales_event)

                # Send to Kafka
                future = self.producer.send(
                    'vietnam_sales_events',
                    key=sales_event.order_id,
                    value=sales_dict
                )

                # Add metadata
                sales_dict['_metadata'] = {
                    'producer_timestamp': datetime.now().isoformat(),
                    'topic': 'vietnam_sales_events',
                    'partition_key': sales_event.order_id
                }

                logger.debug(f"📤 Sales event sent: {sales_event.order_id} - {sales_event.total_amount_vnd:,} VND")

                time.sleep(1.0 / rate_per_second)

            except Exception as e:
                logger.error(f"❌ Error sending sales event: {e}")
                time.sleep(1)

    def send_user_activities(self, rate_per_second: int = 50):
        """Send user activity events to Kafka"""
        logger.info(f"🚀 Starting user activities stream at {rate_per_second} events/second")

        activity_types = ['page_view', 'product_view', 'add_to_cart', 'search', 'filter', 'share']

        while self.running:
            try:
                activity = {
                    'activity_id': f"ACT_{uuid.uuid4().hex[:10].upper()}",
                    'session_id': f"SES_{uuid.uuid4().hex[:8].upper()}",
                    'customer_id': f"VN_CUST_{uuid.uuid4().hex[:8].upper()}",
                    'activity_type': random.choice(activity_types),
                    'platform': random.choice(list(self.data_generator.platforms.keys())),
                    'device_type': random.choices(['Mobile', 'Desktop', 'Tablet'], weights=[0.75, 0.2, 0.05])[0],
                    'page_url': f"/products/{uuid.uuid4().hex[:8]}",
                    'duration_seconds': random.randint(5, 300),
                    'timestamp': datetime.now().isoformat()
                }

                # Send to Kafka
                future = self.producer.send(
                    'vietnam_user_activities',
                    key=activity['session_id'],
                    value=activity
                )

                logger.debug(f"📤 Activity sent: {activity['activity_type']}")

                time.sleep(1.0 / rate_per_second)

            except Exception as e:
                logger.error(f"❌ Error sending user activity: {e}")
                time.sleep(1)

    def start_streaming(self):
        """Start all streaming threads"""
        if self.running:
            logger.warning("⚠️ Streaming is already running")
            return

        self.running = True

        # Create topics first
        self.create_topics()

        # Start streaming threads
        threads_config = [
            ('customers', self.send_customer_data, self.topics_config['vietnam_customers']['rate_per_second']),
            ('products', self.send_product_data, self.topics_config['vietnam_products']['rate_per_second']),
            ('sales', self.send_sales_events, self.topics_config['vietnam_sales_events']['rate_per_second']),
            ('activities', self.send_user_activities, self.topics_config['vietnam_user_activities']['rate_per_second'])
        ]

        for name, func, rate in threads_config:
            thread = threading.Thread(
                target=func,
                args=(rate,),
                daemon=True,
                name=f"vietnam_{name}_producer"
            )
            thread.start()
            self.threads.append(thread)
            logger.info(f"✅ Started {name} producer thread")

        logger.info("🎉 All Vietnam e-commerce data streams started!")

    def stop_streaming(self):
        """Stop all streaming threads"""
        logger.info("🛑 Stopping Vietnam e-commerce data streams...")

        self.running = False

        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5)

        # Close producer
        if self.producer:
            self.producer.flush()
            self.producer.close()

        logger.info("✅ All streaming stopped successfully")

    def get_stats(self) -> Dict:
        """Get producer statistics"""
        if not self.producer:
            return {}

        metrics = self.producer.metrics()
        return {
            'producer_node_id': metrics.get('producer-node-metrics', {}).get('node-id', 'N/A'),
            'record_send_rate': metrics.get('producer-metrics', {}).get('record-send-rate', 0),
            'byte_rate': metrics.get('producer-metrics', {}).get('byte-rate', 0),
            'batch_size_avg': metrics.get('producer-metrics', {}).get('batch-size-avg', 0),
            'compression_rate': metrics.get('producer-metrics', {}).get('compression-rate-avg', 0)
        }

# ====================================================================
# MAIN EXECUTION
# ====================================================================

def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Vietnam E-commerce Kafka Producer')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--duration', type=int, default=0, help='Running duration in seconds (0 = infinite)')
    parser.add_argument('--customer-rate', type=int, default=5, help='Customer events per second')
    parser.add_argument('--product-rate', type=int, default=3, help='Product events per second')
    parser.add_argument('--sales-rate', type=int, default=20, help='Sales events per second')
    parser.add_argument('--activity-rate', type=int, default=50, help='Activity events per second')

    args = parser.parse_args()

    # Custom topics configuration
    topics_config = {
        'vietnam_customers': {
            'partitions': 3,
            'replication_factor': 1,
            'retention_ms': 2592000000,
            'rate_per_second': args.customer_rate
        },
        'vietnam_products': {
            'partitions': 4,
            'replication_factor': 1,
            'retention_ms': 1209600000,
            'rate_per_second': args.product_rate
        },
        'vietnam_sales_events': {
            'partitions': 6,
            'replication_factor': 1,
            'retention_ms': 604800000,
            'rate_per_second': args.sales_rate
        },
        'vietnam_user_activities': {
            'partitions': 4,
            'replication_factor': 1,
            'retention_ms': 86400000,
            'rate_per_second': args.activity_rate
        }
    }

    # Initialize producer
    producer = VietnamEcommerceKafkaProducer(
        bootstrap_servers=args.kafka_servers,
        topics_config=topics_config
    )

    try:
        # Start streaming
        producer.start_streaming()

        logger.info("🎯 Vietnam E-commerce data streaming started!")
        logger.info(f"📊 Rates: Customers={args.customer_rate}/s, Products={args.product_rate}/s, Sales={args.sales_rate}/s, Activities={args.activity_rate}/s")

        if args.duration > 0:
            logger.info(f"⏱️ Running for {args.duration} seconds...")
            time.sleep(args.duration)
        else:
            logger.info("⏱️ Running indefinitely (Ctrl+C to stop)...")
            while True:
                time.sleep(10)
                stats = producer.get_stats()
                logger.info(f"📈 Producer stats: {stats}")

    except KeyboardInterrupt:
        logger.info("⌨️ Interrupted by user")

    except Exception as e:
        logger.error(f"❌ Error: {e}")

    finally:
        producer.stop_streaming()
        logger.info("🏁 Vietnam E-commerce Kafka Producer stopped")

if __name__ == "__main__":
    main()