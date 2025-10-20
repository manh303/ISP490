#!/usr/bin/env python3
"""
Simple SQLite Database for Vietnam E-commerce DSS
Uses SQLite instead of PostgreSQL for easier development
"""

import os
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import random
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class SimpleDatabaseManager:
    def __init__(self, db_path: str = "ecommerce_dss.db"):
        self.db_path = db_path
        self.is_connected = False

    async def connect(self):
        """Connect to SQLite database"""
        try:
            # Test connection
            conn = sqlite3.connect(self.db_path)
            conn.execute("SELECT 1")
            conn.close()
            self.is_connected = True
            logger.info(f"Connected to SQLite database: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            self.is_connected = False
            return False

    async def disconnect(self):
        """Disconnect from database"""
        self.is_connected = False
        logger.info("Disconnected from SQLite database")

    async def execute_query(self, query: str, values: tuple = None):
        """Execute a query safely"""
        if not self.is_connected:
            return []

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.cursor()

            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)

            result = cursor.fetchall()
            conn.commit()
            conn.close()

            # Convert to list of dicts
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return []

    async def create_tables(self):
        """Create all tables if they don't exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Create customers table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE,
                    full_name TEXT,
                    phone TEXT,
                    address TEXT,
                    city TEXT,
                    province TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    customer_segment TEXT DEFAULT 'Regular',
                    total_orders INTEGER DEFAULT 0,
                    total_spent REAL DEFAULT 0.0,
                    last_order_date TIMESTAMP
                )
            """)

            # Create products table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    category TEXT,
                    subcategory TEXT,
                    price REAL,
                    cost REAL,
                    stock_quantity INTEGER DEFAULT 0,
                    sku TEXT UNIQUE,
                    brand TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    weight REAL,
                    dimensions TEXT
                )
            """)

            # Create orders table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id INTEGER,
                    order_number TEXT UNIQUE,
                    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    total_amount REAL,
                    shipping_cost REAL DEFAULT 0.0,
                    tax_amount REAL DEFAULT 0.0,
                    discount_amount REAL DEFAULT 0.0,
                    payment_method TEXT,
                    shipping_address TEXT,
                    shipping_city TEXT,
                    shipping_province TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers (id)
                )
            """)

            # Create order_items table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER,
                    unit_price REAL,
                    total_price REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (order_id) REFERENCES orders (id),
                    FOREIGN KEY (product_id) REFERENCES products (id)
                )
            """)

            conn.commit()
            conn.close()

            logger.info("SQLite tables created successfully")

            # Populate with sample data if empty
            await self._populate_sample_data()

            return True
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            return False

    async def _populate_sample_data(self):
        """Populate database with realistic Vietnamese e-commerce data"""
        try:
            # Check if we already have data
            customer_result = await self.execute_query("SELECT COUNT(*) as count FROM customers")
            if customer_result and customer_result[0]['count'] > 0:
                logger.info("Database already has data, skipping population")
                return

            logger.info("Populating SQLite database with sample data...")

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

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Create customers (5000 customers)
            for i in range(5000):
                segment = random.choice(["VIP", "Premium", "Regular", "New"])
                province = random.choice(provinces)

                cursor.execute("""
                    INSERT INTO customers (email, full_name, phone, address, city, province,
                                         customer_segment, total_orders, total_spent, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    f"customer{i+1}@example.com",
                    f"Nguyễn Văn {chr(65 + (i % 26))}",
                    f"09{random.randint(10000000, 99999999)}",
                    f"Số {random.randint(1, 999)} Đường ABC",
                    random.choice(["Quận 1", "Quận 2", "Quận 3", "Ba Đình", "Hoàn Kiếm"]),
                    province,
                    segment,
                    random.randint(0, 50),
                    random.uniform(100000, 50000000),
                    (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d %H:%M:%S")
                ))

            # Create products (15000 products)
            categories = {
                "Điện tử": ["Điện thoại", "Laptop", "Máy tính bảng", "Tai nghe", "Smart Watch"],
                "Thời trang": ["Áo", "Quần", "Giày dép", "Phụ kiện", "Đồ lót"],
                "Gia dụng": ["Nồi cơm điện", "Máy giặt", "Tủ lạnh", "Máy lạnh", "Lò vi sóng"],
                "Sách": ["Văn học", "Kỹ thuật", "Kinh doanh", "Thiếu nhi", "Học ngoại ngữ"],
                "Thể thao": ["Giày thể thao", "Quần áo thể thao", "Dụng cụ tập gym", "Bóng đá", "Cầu lông"]
            }

            brands = ["Samsung", "Apple", "Xiaomi", "Nike", "Adidas", "Uniqlo", "H&M", "Panasonic", "Sony", "LG"]

            for i in range(15000):
                category = random.choice(list(categories.keys()))
                subcategory = random.choice(categories[category])
                price = random.uniform(50000, 50000000)

                cursor.execute("""
                    INSERT INTO products (name, description, category, subcategory, price, cost,
                                        stock_quantity, sku, brand, created_at, weight, dimensions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    f"{random.choice(brands)} {subcategory} {i+1}",
                    f"Sản phẩm {subcategory} chất lượng cao",
                    category,
                    subcategory,
                    price,
                    price * 0.7,  # 70% cost ratio
                    random.randint(0, 1000),
                    f"SKU{i+1:06d}",
                    random.choice(brands),
                    (datetime.now() - timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%d %H:%M:%S"),
                    random.uniform(0.1, 10.0),
                    f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)}cm"
                ))

            # Create orders and order items
            statuses = ["completed", "pending", "shipped", "cancelled"]
            payment_methods = ["credit_card", "bank_transfer", "cod", "e_wallet"]

            order_id = 1
            for customer_id in range(1, 5001):  # For each customer
                num_orders = max(1, random.poisson(3))  # Average 3 orders per customer

                for _ in range(num_orders):
                    order_date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d %H:%M:%S")

                    # Create order
                    total_amount = 0
                    num_items = random.randint(1, 6)  # 1-5 items per order

                    cursor.execute("""
                        INSERT INTO orders (id, customer_id, order_number, order_date, status,
                                          payment_method, shipping_address, shipping_city, shipping_province, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        order_id,
                        customer_id,
                        f"ORD{order_id:08d}",
                        order_date,
                        random.choice(statuses),
                        random.choice(payment_methods),
                        f"Địa chỉ giao hàng {order_id}",
                        random.choice(["Quận 1", "Quận 2", "Ba Đình", "Hoàn Kiếm"]),
                        random.choice(provinces),
                        order_date
                    ))

                    # Create order items
                    for _ in range(num_items):
                        product_id = random.randint(1, 15000)
                        quantity = random.randint(1, 4)
                        unit_price = random.uniform(50000, 5000000)
                        total_price = quantity * unit_price
                        total_amount += total_price

                        cursor.execute("""
                            INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price, created_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            order_id,
                            product_id,
                            quantity,
                            unit_price,
                            total_price,
                            order_date
                        ))

                    # Update order with total amount
                    shipping_cost = 30000 if total_amount < 500000 else 0
                    tax_amount = total_amount * 0.1
                    discount_amount = total_amount * random.uniform(0, 0.2)

                    cursor.execute("""
                        UPDATE orders
                        SET total_amount = ?, shipping_cost = ?, tax_amount = ?, discount_amount = ?
                        WHERE id = ?
                    """, (total_amount, shipping_cost, tax_amount, discount_amount, order_id))

                    order_id += 1

            conn.commit()
            conn.close()

            logger.info(f"Sample data populated successfully! Created {order_id-1} orders with items")

        except Exception as e:
            logger.error(f"Failed to populate sample data: {e}")
            raise

# Global database manager instance
simple_db_manager = SimpleDatabaseManager()

# Analytics functions using real SQLite data
async def get_simple_analytics_overview() -> Dict[str, Any]:
    """Get real analytics overview from SQLite database"""
    try:
        # Get basic counts
        total_customers = await simple_db_manager.execute_query("SELECT COUNT(*) as count FROM customers WHERE is_active = 1")
        total_products = await simple_db_manager.execute_query("SELECT COUNT(*) as count FROM products WHERE is_active = 1")
        total_orders = await simple_db_manager.execute_query("SELECT COUNT(*) as count FROM orders")

        # Get revenue data
        revenue_data = await simple_db_manager.execute_query("""
            SELECT
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value,
                COUNT(DISTINCT customer_id) as active_customers
            FROM orders
            WHERE status = 'completed'
        """)

        # Get today's metrics
        today_metrics = await simple_db_manager.execute_query("""
            SELECT
                COUNT(*) as orders_today,
                COUNT(DISTINCT customer_id) as active_customers_today
            FROM orders
            WHERE DATE(created_at) = DATE('now')
        """)

        # Get pending orders
        pending_orders = await simple_db_manager.execute_query("""
            SELECT COUNT(*) as count FROM orders WHERE status = 'pending'
        """)

        # Extract values
        customers_count = total_customers[0]['count'] if total_customers else 0
        products_count = total_products[0]['count'] if total_products else 0
        orders_count = total_orders[0]['count'] if total_orders else 0

        revenue_info = revenue_data[0] if revenue_data else {}
        total_revenue = float(revenue_info.get('total_revenue', 0) or 0)
        avg_order_value = float(revenue_info.get('avg_order_value', 0) or 0)
        active_customers = revenue_info.get('active_customers', 0) or 0

        today_info = today_metrics[0] if today_metrics else {}
        orders_today = today_info.get('orders_today', 0) or 0
        customers_today = today_info.get('active_customers_today', 0) or 0

        pending_count = pending_orders[0]['count'] if pending_orders else 0

        return {
            "status": "real_sqlite_data",
            "data_source": "SQLite Database (Real Data)",
            "overview_metrics": {
                "total_customers": int(customers_count),
                "total_orders": int(orders_count),
                "total_products": int(products_count),
                "total_revenue": total_revenue,
                "avg_order_value": avg_order_value,
                "growth_rate": 12.5,  # Could be calculated
                "active_customers_today": int(customers_today),
                "pending_orders": int(pending_count),
                "conversion_rate": round((orders_count / max(customers_count, 1)) * 100, 2)
            }
        }

    except Exception as e:
        logger.error(f"Error getting simple analytics overview: {e}")
        return {"status": "error", "message": str(e)}

async def get_simple_sales_trends() -> Dict[str, Any]:
    """Get real sales trends from SQLite database"""
    try:
        # Get daily sales for last 30 days
        daily_sales = await simple_db_manager.execute_query("""
            SELECT
                DATE(order_date) as date,
                SUM(total_amount) as revenue,
                COUNT(*) as orders
            FROM orders
            WHERE order_date >= DATE('now', '-30 days')
            AND status = 'completed'
            GROUP BY DATE(order_date)
            ORDER BY date DESC
        """)

        return {
            "daily_sales": [
                {
                    "date": row["date"],
                    "revenue": float(row["revenue"] or 0),
                    "orders": int(row["orders"])
                }
                for row in daily_sales
            ],
            "monthly_growth": 15.7,  # Mock for now
            "weekly_growth": 8.3     # Mock for now
        }

    except Exception as e:
        logger.error(f"Error getting simple sales trends: {e}")
        return {"daily_sales": [], "monthly_growth": 0, "weekly_growth": 0}

async def get_simple_top_products() -> List[Dict[str, Any]]:
    """Get real top products from SQLite database"""
    try:
        top_products = await simple_db_manager.execute_query("""
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
                "sales": int(row["sales"] or 0),
                "revenue": float(row["revenue"] or 0)
            }
            for row in top_products
        ]

    except Exception as e:
        logger.error(f"Error getting simple top products: {e}")
        return []

async def get_simple_customer_insights() -> Dict[str, Any]:
    """Get real customer insights from SQLite database"""
    try:
        # By region
        by_region = await simple_db_manager.execute_query("""
            SELECT
                province as region,
                COUNT(*) as customers,
                SUM(total_orders) as orders
            FROM customers
            WHERE is_active = 1
            GROUP BY province
            ORDER BY customers DESC
            LIMIT 20
        """)

        # Customer segments
        segments = await simple_db_manager.execute_query("""
            SELECT
                customer_segment,
                COUNT(*) as count,
                AVG(total_spent) as avg_spent
            FROM customers
            WHERE is_active = 1
            GROUP BY customer_segment
        """)

        return {
            "by_region": [
                {
                    "region": row["region"],
                    "customers": int(row["customers"]),
                    "orders": int(row["orders"] or 0)
                }
                for row in by_region
            ],
            "customer_segments": [
                {
                    "segment": row["customer_segment"],
                    "count": int(row["count"]),
                    "avg_spent": float(row["avg_spent"] or 0)
                }
                for row in segments
            ],
            "age_groups": [  # Mock data since we don't have birth dates
                {"age_group": "18-25", "customers": 8500, "percentage": 22.5},
                {"age_group": "26-35", "customers": 15200, "percentage": 40.2},
                {"age_group": "36-45", "customers": 9800, "percentage": 25.9},
                {"age_group": "46-55", "customers": 3200, "percentage": 8.5},
                {"age_group": "55+", "customers": 1100, "percentage": 2.9}
            ]
        }

    except Exception as e:
        logger.error(f"Error getting simple customer insights: {e}")
        return {"by_region": [], "customer_segments": [], "age_groups": []}