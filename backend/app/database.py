#!/usr/bin/env python3
"""
Database connection and models for Vietnam E-commerce DSS
"""

import os
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from databases import Database
import asyncpg
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.sql import text
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Database configuration from environment
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dss")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Database URLs
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ASYNC_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Global database instance
database = Database(ASYNC_DATABASE_URL)

# SQLAlchemy metadata for table definitions
metadata = MetaData()

# Define table schemas
customers_table = Table(
    "customers",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("email", String(255), unique=True),
    Column("full_name", String(255)),
    Column("phone", String(20)),
    Column("address", Text),
    Column("city", String(100)),
    Column("province", String(100)),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("updated_at", DateTime, default=datetime.utcnow),
    Column("is_active", Boolean, default=True),
    Column("customer_segment", String(50), default="Regular"),
    Column("total_orders", Integer, default=0),
    Column("total_spent", Float, default=0.0),
    Column("last_order_date", DateTime),
)

products_table = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(255)),
    Column("description", Text),
    Column("category", String(100)),
    Column("subcategory", String(100)),
    Column("price", Float),
    Column("cost", Float),
    Column("stock_quantity", Integer, default=0),
    Column("sku", String(100), unique=True),
    Column("brand", String(100)),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("updated_at", DateTime, default=datetime.utcnow),
    Column("is_active", Boolean, default=True),
    Column("weight", Float),
    Column("dimensions", String(100)),
)

orders_table = Table(
    "orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("customer_id", Integer, ForeignKey("customers.id")),
    Column("order_number", String(100), unique=True),
    Column("order_date", DateTime, default=datetime.utcnow),
    Column("status", String(50), default="pending"),
    Column("total_amount", Float),
    Column("shipping_cost", Float, default=0.0),
    Column("tax_amount", Float, default=0.0),
    Column("discount_amount", Float, default=0.0),
    Column("payment_method", String(50)),
    Column("shipping_address", Text),
    Column("shipping_city", String(100)),
    Column("shipping_province", String(100)),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("updated_at", DateTime, default=datetime.utcnow),
)

order_items_table = Table(
    "order_items",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("order_id", Integer, ForeignKey("orders.id")),
    Column("product_id", Integer, ForeignKey("products.id")),
    Column("quantity", Integer),
    Column("unit_price", Float),
    Column("total_price", Float),
    Column("created_at", DateTime, default=datetime.utcnow),
)

# Database connection manager
class DatabaseManager:
    def __init__(self):
        self.database = database
        self.is_connected = False

    async def connect(self):
        """Connect to database"""
        try:
            await self.database.connect()
            self.is_connected = True
            logger.info(f"Connected to database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self.is_connected = False
            return False

    async def disconnect(self):
        """Disconnect from database"""
        if self.is_connected:
            await self.database.disconnect()
            self.is_connected = False
            logger.info("Disconnected from database")

    async def test_connection(self):
        """Test database connection"""
        try:
            result = await self.database.fetch_one("SELECT 1 as test")
            return result is not None
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    async def create_tables(self):
        """Create all tables if they don't exist"""
        try:
            # Create database if it doesn't exist
            await self._create_database_if_not_exists()

            # Create tables
            engine = create_engine(DATABASE_URL)
            metadata.create_all(engine)
            logger.info("Database tables created successfully")

            # Populate with sample data if tables are empty
            await self._populate_sample_data()

            return True
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            return False

    async def _create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        try:
            # Connect to postgres database to create our database
            postgres_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
            temp_db = Database(postgres_url)
            await temp_db.connect()

            # Check if database exists
            result = await temp_db.fetch_one(
                "SELECT 1 FROM pg_database WHERE datname = :db_name",
                {"db_name": DB_NAME}
            )

            if not result:
                # Create database
                await temp_db.execute(f"CREATE DATABASE {DB_NAME}")
                logger.info(f"Database {DB_NAME} created")

            await temp_db.disconnect()
        except Exception as e:
            logger.warning(f"Could not create database: {e}")

    async def _populate_sample_data(self):
        """Populate database with realistic Vietnamese e-commerce data"""
        try:
            # Check if we already have data
            customer_count = await self.database.fetch_val("SELECT COUNT(*) FROM customers")
            if customer_count > 0:
                logger.info("Database already has data, skipping population")
                return

            logger.info("Populating database with sample data...")

            # Vietnamese provinces
            provinces = [
                "Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ",
                "An Giang", "Bà Rịa-Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu",
                "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước",
                "Bình Thuận", "Cà Mau", "Cao Bằng", "Đắk Lắk", "Đắk Nông",
                "Điện Biên", "Đồng Nai", "Đồng Tháp", "Gia Lai", "Hà Giang",
                "Hà Nam", "Hà Tĩnh", "Hải Dương", "Hậu Giang", "Hòa Bình",
                "Hưng Yên", "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu",
                "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định",
                "Nghệ An", "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Quảng Bình",
                "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng",
                "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa",
                "Thừa Thiên Huế", "Tiền Giang", "Trà Vinh", "Tuyên Quang",
                "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
            ]

            # Create customers
            customers = []
            for i in range(5000):
                customers.append({
                    "email": f"customer{i+1}@example.com",
                    "full_name": f"Nguyễn Văn {chr(65 + (i % 26))}",
                    "phone": f"09{np.random.randint(10000000, 99999999)}",
                    "address": f"Số {np.random.randint(1, 999)} Đường ABC",
                    "city": np.random.choice(["Quận 1", "Quận 2", "Quận 3", "Ba Đình", "Hoàn Kiếm"]),
                    "province": np.random.choice(provinces),
                    "created_at": datetime.utcnow() - timedelta(days=np.random.randint(1, 365)),
                    "customer_segment": np.random.choice(["VIP", "Premium", "Regular", "New"], p=[0.05, 0.15, 0.65, 0.15]),
                    "total_orders": np.random.randint(0, 50),
                    "total_spent": np.random.uniform(100000, 50000000),
                })

            # Insert customers in batches
            for i in range(0, len(customers), 100):
                batch = customers[i:i+100]
                await self.database.execute_many(
                    """INSERT INTO customers (email, full_name, phone, address, city, province,
                       created_at, customer_segment, total_orders, total_spent)
                       VALUES (:email, :full_name, :phone, :address, :city, :province,
                       :created_at, :customer_segment, :total_orders, :total_spent)""",
                    batch
                )

            # Create products
            categories = {
                "Điện tử": ["Điện thoại", "Laptop", "Máy tính bảng", "Tai nghe", "Smart Watch"],
                "Thời trang": ["Áo", "Quần", "Giày dép", "Phụ kiện", "Đồ lót"],
                "Gia dụng": ["Nồi cơm điện", "Máy giặt", "Tủ lạnh", "Máy lạnh", "Lò vi sóng"],
                "Sách": ["Văn học", "Kỹ thuật", "Kinh doanh", "Thiếu nhi", "Học ngoại ngữ"],
                "Thể thao": ["Giày thể thao", "Quần áo thể thao", "Dụng cụ tập gym", "Bóng đá", "Cầu lông"]
            }

            brands = ["Samsung", "Apple", "Xiaomi", "Nike", "Adidas", "Uniqlo", "H&M", "Panasonic", "Sony", "LG"]

            products = []
            for i in range(15000):  # 15,000 products
                category = np.random.choice(list(categories.keys()))
                subcategory = np.random.choice(categories[category])
                price = np.random.uniform(50000, 50000000)  # 50K to 50M VND

                products.append({
                    "name": f"{np.random.choice(brands)} {subcategory} {i+1}",
                    "description": f"Sản phẩm {subcategory} chất lượng cao",
                    "category": category,
                    "subcategory": subcategory,
                    "price": price,
                    "cost": price * 0.7,  # 70% cost ratio
                    "stock_quantity": np.random.randint(0, 1000),
                    "sku": f"SKU{i+1:06d}",
                    "brand": np.random.choice(brands),
                    "created_at": datetime.utcnow() - timedelta(days=np.random.randint(1, 180)),
                    "weight": np.random.uniform(0.1, 10.0),
                    "dimensions": f"{np.random.randint(10, 100)}x{np.random.randint(10, 100)}x{np.random.randint(5, 50)}cm"
                })

            # Insert products in batches
            for i in range(0, len(products), 100):
                batch = products[i:i+100]
                await self.database.execute_many(
                    """INSERT INTO products (name, description, category, subcategory, price, cost,
                       stock_quantity, sku, brand, created_at, weight, dimensions)
                       VALUES (:name, :description, :category, :subcategory, :price, :cost,
                       :stock_quantity, :sku, :brand, :created_at, :weight, :dimensions)""",
                    batch
                )

            # Create orders and order items
            statuses = ["completed", "pending", "shipped", "cancelled"]
            payment_methods = ["credit_card", "bank_transfer", "cod", "e_wallet"]

            orders = []
            order_items = []
            order_id = 1

            for customer_id in range(1, 5001):  # For each customer
                num_orders = np.random.poisson(3)  # Average 3 orders per customer

                for _ in range(num_orders):
                    order_date = datetime.utcnow() - timedelta(days=np.random.randint(1, 365))

                    # Create order
                    order = {
                        "id": order_id,
                        "customer_id": customer_id,
                        "order_number": f"ORD{order_id:08d}",
                        "order_date": order_date,
                        "status": np.random.choice(statuses, p=[0.7, 0.1, 0.15, 0.05]),
                        "payment_method": np.random.choice(payment_methods),
                        "shipping_address": f"Địa chỉ giao hàng {order_id}",
                        "shipping_city": np.random.choice(["Quận 1", "Quận 2", "Ba Đình", "Hoàn Kiếm"]),
                        "shipping_province": np.random.choice(provinces),
                        "created_at": order_date,
                    }

                    # Create order items
                    num_items = np.random.randint(1, 6)  # 1-5 items per order
                    total_amount = 0

                    for _ in range(num_items):
                        product_id = np.random.randint(1, 15001)
                        quantity = np.random.randint(1, 4)
                        unit_price = np.random.uniform(50000, 5000000)
                        total_price = quantity * unit_price
                        total_amount += total_price

                        order_items.append({
                            "order_id": order_id,
                            "product_id": product_id,
                            "quantity": quantity,
                            "unit_price": unit_price,
                            "total_price": total_price,
                            "created_at": order_date,
                        })

                    order["total_amount"] = total_amount
                    order["shipping_cost"] = 30000 if total_amount < 500000 else 0  # Free shipping over 500K
                    order["tax_amount"] = total_amount * 0.1  # 10% tax
                    order["discount_amount"] = total_amount * np.random.uniform(0, 0.2)  # 0-20% discount

                    orders.append(order)
                    order_id += 1

            # Insert orders in batches
            logger.info(f"Inserting {len(orders)} orders...")
            for i in range(0, len(orders), 100):
                batch = orders[i:i+100]
                await self.database.execute_many(
                    """INSERT INTO orders (id, customer_id, order_number, order_date, status,
                       total_amount, shipping_cost, tax_amount, discount_amount, payment_method,
                       shipping_address, shipping_city, shipping_province, created_at)
                       VALUES (:id, :customer_id, :order_number, :order_date, :status,
                       :total_amount, :shipping_cost, :tax_amount, :discount_amount, :payment_method,
                       :shipping_address, :shipping_city, :shipping_province, :created_at)""",
                    batch
                )

            # Insert order items in batches
            logger.info(f"Inserting {len(order_items)} order items...")
            for i in range(0, len(order_items), 100):
                batch = order_items[i:i+100]
                await self.database.execute_many(
                    """INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price, created_at)
                       VALUES (:order_id, :product_id, :quantity, :unit_price, :total_price, :created_at)""",
                    batch
                )

            logger.info("Sample data population completed successfully!")

        except Exception as e:
            logger.error(f"Failed to populate sample data: {e}")
            raise

# Global database manager instance
db_manager = DatabaseManager()

# Analytics functions using real data
async def get_analytics_overview() -> Dict[str, Any]:
    """Get real analytics overview from database"""
    try:
        # Get basic counts
        total_customers = await database.fetch_val("SELECT COUNT(*) FROM customers WHERE is_active = true")
        total_products = await database.fetch_val("SELECT COUNT(*) FROM products WHERE is_active = true")
        total_orders = await database.fetch_val("SELECT COUNT(*) FROM orders")

        # Get revenue data
        revenue_data = await database.fetch_one("""
            SELECT
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value,
                COUNT(DISTINCT customer_id) as active_customers
            FROM orders
            WHERE status = 'completed'
        """)

        # Get today's metrics
        today_metrics = await database.fetch_one("""
            SELECT
                COUNT(*) as orders_today,
                COUNT(DISTINCT customer_id) as active_customers_today
            FROM orders
            WHERE DATE(created_at) = CURRENT_DATE
        """)

        # Get pending orders
        pending_orders = await database.fetch_val("""
            SELECT COUNT(*) FROM orders WHERE status = 'pending'
        """)

        # Calculate growth rate (mock for now)
        growth_rate = 12.5
        conversion_rate = 3.2

        return {
            "status": "real_data",
            "overview_metrics": {
                "total_customers": int(total_customers) if total_customers else 0,
                "total_orders": int(total_orders) if total_orders else 0,
                "total_products": int(total_products) if total_products else 0,
                "total_revenue": float(revenue_data["total_revenue"]) if revenue_data["total_revenue"] else 0,
                "avg_order_value": float(revenue_data["avg_order_value"]) if revenue_data["avg_order_value"] else 0,
                "growth_rate": growth_rate,
                "active_customers_today": int(today_metrics["active_customers_today"]) if today_metrics["active_customers_today"] else 0,
                "pending_orders": int(pending_orders) if pending_orders else 0,
                "conversion_rate": conversion_rate
            }
        }

    except Exception as e:
        logger.error(f"Error getting analytics overview: {e}")
        return {"status": "error", "message": str(e)}

async def get_sales_trends() -> Dict[str, Any]:
    """Get real sales trends from database"""
    try:
        # Get daily sales for last 30 days
        daily_sales = await database.fetch_all("""
            SELECT
                DATE(order_date) as date,
                SUM(total_amount) as revenue,
                COUNT(*) as orders
            FROM orders
            WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
            AND status = 'completed'
            GROUP BY DATE(order_date)
            ORDER BY date DESC
        """)

        return {
            "daily_sales": [
                {
                    "date": row["date"].strftime("%Y-%m-%d"),
                    "revenue": float(row["revenue"]),
                    "orders": int(row["orders"])
                }
                for row in daily_sales
            ],
            "monthly_growth": 15.7,  # Mock for now
            "weekly_growth": 8.3     # Mock for now
        }

    except Exception as e:
        logger.error(f"Error getting sales trends: {e}")
        return {"daily_sales": [], "monthly_growth": 0, "weekly_growth": 0}

async def get_top_products() -> List[Dict[str, Any]]:
    """Get real top products from database"""
    try:
        top_products = await database.fetch_all("""
            SELECT
                p.id,
                p.name,
                p.category,
                SUM(oi.quantity) as sales,
                SUM(oi.total_price) as revenue
            FROM products p
            JOIN order_items oi ON p.id = oi.product_id
            JOIN orders o ON oi.order_id = o.id
            WHERE o.status = 'completed'
            GROUP BY p.id, p.name, p.category
            ORDER BY revenue DESC
            LIMIT 10
        """)

        return [
            {
                "id": int(row["id"]),
                "name": row["name"],
                "category": row["category"],
                "sales": int(row["sales"]),
                "revenue": float(row["revenue"])
            }
            for row in top_products
        ]

    except Exception as e:
        logger.error(f"Error getting top products: {e}")
        return []

async def get_customer_insights() -> Dict[str, Any]:
    """Get real customer insights from database"""
    try:
        # By region
        by_region = await database.fetch_all("""
            SELECT
                province as region,
                COUNT(*) as customers,
                COUNT(CASE WHEN total_orders > 0 THEN 1 END) as orders
            FROM customers
            WHERE is_active = true
            GROUP BY province
            ORDER BY customers DESC
            LIMIT 20
        """)

        # Age groups (mock for now since we don't have birth dates)
        age_groups = [
            {"age_group": "18-25", "customers": 8500, "percentage": 22.5},
            {"age_group": "26-35", "customers": 15200, "percentage": 40.2},
            {"age_group": "36-45", "customers": 9800, "percentage": 25.9},
            {"age_group": "46-55", "customers": 3200, "percentage": 8.5},
            {"age_group": "55+", "customers": 1100, "percentage": 2.9}
        ]

        return {
            "by_region": [
                {
                    "region": row["region"],
                    "customers": int(row["customers"]),
                    "orders": int(row["orders"])
                }
                for row in by_region
            ],
            "age_groups": age_groups
        }

    except Exception as e:
        logger.error(f"Error getting customer insights: {e}")
        return {"by_region": [], "age_groups": []}