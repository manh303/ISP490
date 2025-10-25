#!/usr/bin/env python3
"""
Create Vietnam E-commerce Data Lake Structure
Tạo cấu trúc data lake cho dữ liệu Vietnam e-commerce
"""
import os
import json
import logging
from datetime import datetime, timedelta
import psycopg2
from pymongo import MongoClient
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VietnamDataLake:
    def __init__(self):
        # PostgreSQL configuration (Data Warehouse)
        self.postgres_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        # MongoDB configuration (Data Lake)
        self.mongo_config = {
            'host': 'localhost',
            'port': 27017,
            'database': 'vietnam_ecommerce_lake'
        }

        self.pg_conn = None
        self.mongo_client = None
        self.mongo_db = None

    def connect_postgresql(self):
        """Connect to PostgreSQL"""
        try:
            self.pg_conn = psycopg2.connect(**self.postgres_config)
            logger.info("✅ Connected to PostgreSQL Data Warehouse")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            return False

    def connect_mongodb(self):
        """Connect to MongoDB"""
        try:
            self.mongo_client = MongoClient(
                self.mongo_config['host'],
                self.mongo_config['port']
            )
            self.mongo_db = self.mongo_client[self.mongo_config['database']]
            logger.info("✅ Connected to MongoDB Data Lake")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            return False

    def create_postgres_views(self):
        """Create useful views in PostgreSQL for better visibility"""
        try:
            cursor = self.pg_conn.cursor()

            # Create a unified view of all Vietnam data
            create_view_sql = """
            CREATE OR REPLACE VIEW vietnam_dw.v_ecommerce_overview AS
            SELECT
                'customers' as table_type,
                COUNT(*) as record_count,
                'Active customers in system' as description
            FROM vietnam_dw.dim_customer_vn

            UNION ALL

            SELECT
                'products' as table_type,
                COUNT(*) as record_count,
                'Available products' as description
            FROM vietnam_dw.dim_product_vn

            UNION ALL

            SELECT
                'sales' as table_type,
                COUNT(*) as record_count,
                'Sales transactions' as description
            FROM vietnam_dw.fact_sales_vn

            UNION ALL

            SELECT
                'activities' as table_type,
                COUNT(*) as record_count,
                'Customer activities tracked' as description
            FROM vietnam_dw.fact_customer_activity_vn;
            """

            cursor.execute(create_view_sql)

            # Create daily sales summary view
            daily_sales_view = """
            CREATE OR REPLACE VIEW vietnam_dw.v_daily_sales_summary AS
            SELECT
                sale_date,
                COUNT(*) as total_orders,
                SUM(total_amount_vnd) as total_revenue_vnd,
                ROUND(AVG(total_amount_vnd)) as avg_order_value_vnd,
                COUNT(DISTINCT customer_key) as unique_customers,
                SUM(CASE WHEN is_cod_order THEN 1 ELSE 0 END) as cod_orders,
                SUM(CASE WHEN is_tet_order THEN 1 ELSE 0 END) as tet_orders
            FROM vietnam_dw.fact_sales_vn
            GROUP BY sale_date
            ORDER BY sale_date DESC;
            """

            cursor.execute(daily_sales_view)

            # Create customer insights view
            customer_insights_view = """
            CREATE OR REPLACE VIEW vietnam_dw.v_customer_insights AS
            SELECT
                c.customer_name,
                c.province,
                c.customer_segment,
                COUNT(s.sale_key) as total_orders,
                SUM(s.total_amount_vnd) as total_spent_vnd,
                MAX(s.sale_date) as last_order_date,
                CASE
                    WHEN MAX(s.sale_date) >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
                    WHEN MAX(s.sale_date) >= CURRENT_DATE - INTERVAL '90 days' THEN 'Recent'
                    ELSE 'Inactive'
                END as customer_status
            FROM vietnam_dw.dim_customer_vn c
            LEFT JOIN vietnam_dw.fact_sales_vn s ON c.customer_key = s.customer_key
            GROUP BY c.customer_key, c.customer_name, c.province, c.customer_segment
            ORDER BY total_spent_vnd DESC;
            """

            cursor.execute(customer_insights_view)

            self.pg_conn.commit()
            cursor.close()

            logger.info("✅ Created PostgreSQL views successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Error creating PostgreSQL views: {e}")
            return False

    def create_data_lake_collections(self):
        """Create MongoDB collections for data lake"""
        try:
            # Raw data collections
            collections = {
                'raw_sales_events': 'Raw sales events from streaming',
                'raw_customer_data': 'Raw customer information',
                'raw_product_data': 'Raw product catalog',
                'raw_user_activities': 'Raw user behavior data',

                # Processed data collections
                'processed_sales_daily': 'Daily aggregated sales data',
                'processed_customer_profiles': 'Enhanced customer profiles',
                'processed_product_analytics': 'Product performance analytics',

                # Analytics collections
                'analytics_trends': 'Market trends and insights',
                'analytics_forecasts': 'Sales forecasting results',
                'analytics_segments': 'Customer segmentation results',

                # ML model collections
                'ml_model_results': 'Machine learning model outputs',
                'ml_feature_store': 'Feature engineering results',
                'ml_predictions': 'Real-time predictions'
            }

            for collection_name, description in collections.items():
                collection = self.mongo_db[collection_name]

                # Create index on timestamp for time-series data
                if 'raw_' in collection_name or 'processed_' in collection_name:
                    collection.create_index([("timestamp", -1)])
                    collection.create_index([("created_at", -1)])

                # Insert metadata document
                metadata = {
                    "collection_info": {
                        "name": collection_name,
                        "description": description,
                        "created_at": datetime.now().isoformat(),
                        "data_type": "vietnam_ecommerce",
                        "schema_version": "1.0"
                    }
                }

                collection.insert_one(metadata)
                logger.info(f"✅ Created collection: {collection_name}")

            return True

        except Exception as e:
            logger.error(f"❌ Error creating MongoDB collections: {e}")
            return False

    def seed_sample_data_lake(self):
        """Seed some sample data into the data lake"""
        try:
            # Sample analytics data
            trends_data = {
                "trend_analysis": {
                    "analysis_date": datetime.now().isoformat(),
                    "top_selling_categories": [
                        {"category": "Điện thoại", "sales_volume": 1250, "revenue_vnd": 45000000},
                        {"category": "Laptop", "sales_volume": 890, "revenue_vnd": 78000000},
                        {"category": "Thời trang", "sales_volume": 2100, "revenue_vnd": 32000000}
                    ],
                    "regional_performance": {
                        "Ho_Chi_Minh": {"orders": 3500, "revenue_vnd": 125000000},
                        "Ha_Noi": {"orders": 2800, "revenue_vnd": 98000000},
                        "Da_Nang": {"orders": 1200, "revenue_vnd": 42000000}
                    },
                    "payment_method_trends": {
                        "COD": 45.2,
                        "Banking": 28.7,
                        "MoMo": 18.3,
                        "VNPay": 7.8
                    }
                },
                "created_at": datetime.now().isoformat(),
                "data_source": "vietnam_streaming_pipeline"
            }

            self.mongo_db.analytics_trends.insert_one(trends_data)

            # Sample ML predictions
            predictions_data = {
                "prediction_batch": {
                    "batch_id": f"pred_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "model_version": "customer_lifetime_value_v1.2",
                    "predictions": [
                        {"customer_id": "VN_CUST_001", "predicted_clv_vnd": 2450000, "confidence": 0.87},
                        {"customer_id": "VN_CUST_002", "predicted_clv_vnd": 1890000, "confidence": 0.92},
                        {"customer_id": "VN_CUST_003", "predicted_clv_vnd": 3200000, "confidence": 0.78}
                    ],
                    "prediction_date": datetime.now().isoformat()
                },
                "created_at": datetime.now().isoformat()
            }

            self.mongo_db.ml_predictions.insert_one(predictions_data)

            logger.info("✅ Seeded sample data into data lake")
            return True

        except Exception as e:
            logger.error(f"❌ Error seeding data lake: {e}")
            return False

    def create_data_lake_summary(self):
        """Create a summary of the data lake structure"""
        try:
            # Get PostgreSQL table info
            cursor = self.pg_conn.cursor()
            cursor.execute("""
                SELECT schemaname, tablename,
                       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                FROM pg_tables
                WHERE schemaname = 'vietnam_dw'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """)

            pg_tables = cursor.fetchall()
            cursor.close()

            # Get MongoDB collection info
            mongo_collections = []
            for collection_name in self.mongo_db.list_collection_names():
                count = self.mongo_db[collection_name].count_documents({})
                mongo_collections.append({
                    "collection": collection_name,
                    "document_count": count
                })

            # Create summary
            summary = {
                "vietnam_ecommerce_data_platform": {
                    "created_at": datetime.now().isoformat(),
                    "data_warehouse": {
                        "technology": "PostgreSQL",
                        "schema": "vietnam_dw",
                        "tables": [
                            {"schema": row[0], "table": row[1], "size": row[2]}
                            for row in pg_tables
                        ]
                    },
                    "data_lake": {
                        "technology": "MongoDB",
                        "database": "vietnam_ecommerce_lake",
                        "collections": mongo_collections
                    }
                }
            }

            # Save summary to file
            with open('vietnam_data_platform_summary.json', 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            logger.info("✅ Created data platform summary")
            return summary

        except Exception as e:
            logger.error(f"❌ Error creating summary: {e}")
            return None

    def show_access_instructions(self):
        """Show instructions for accessing the data"""
        instructions = """

🎯 VIETNAM E-COMMERCE DATA PLATFORM ACCESS GUIDE
===============================================

📊 PostgreSQL Data Warehouse:
-----------------------------
docker exec -it ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss

Trong psql:
SET search_path = vietnam_dw, public;
\\d                                    -- Xem tất cả tables
SELECT * FROM v_ecommerce_overview;    -- Tổng quan dữ liệu
SELECT * FROM v_daily_sales_summary;   -- Báo cáo bán hàng hằng ngày
SELECT * FROM v_customer_insights;     -- Phân tích khách hàng

🗄️  MongoDB Data Lake:
-----------------------
docker exec -it ecommerce-dss-project-mongodb-1 mongosh vietnam_ecommerce_lake

Trong MongoDB:
show collections                       -- Xem tất cả collections
db.analytics_trends.find().pretty()    -- Xem xu hướng thị trường
db.ml_predictions.find().pretty()      -- Xem dự đoán ML
db.raw_sales_events.find().limit(5)    -- Xem dữ liệu streaming thô

📈 Analytics Views:
------------------
SELECT * FROM vietnam_dw.v_daily_sales_summary WHERE sale_date >= CURRENT_DATE - 7;
SELECT * FROM vietnam_dw.v_customer_insights WHERE customer_status = 'Active';

🔍 Data Lake Queries:
--------------------
db.analytics_trends.find({"trend_analysis.analysis_date": {$gte: new Date()}})
db.ml_predictions.find().sort({"created_at": -1}).limit(10)
        """

        print(instructions)
        logger.info("✅ Access instructions displayed")

def main():
    logger.info("🚀 Creating Vietnam E-commerce Data Lake Platform...")

    data_lake = VietnamDataLake()

    # Connect to databases
    if not data_lake.connect_postgresql():
        logger.error("❌ Cannot proceed without PostgreSQL connection")
        return

    if not data_lake.connect_mongodb():
        logger.error("❌ Cannot proceed without MongoDB connection")
        return

    # Create PostgreSQL views
    data_lake.create_postgres_views()

    # Create MongoDB data lake
    data_lake.create_data_lake_collections()

    # Seed sample data
    data_lake.seed_sample_data_lake()

    # Create summary
    summary = data_lake.create_data_lake_summary()
    if summary:
        print("\n" + "="*60)
        print("🎉 VIETNAM E-COMMERCE DATA PLATFORM CREATED SUCCESSFULLY!")
        print("="*60)
        print(f"📊 PostgreSQL Tables: {len(summary['vietnam_ecommerce_data_platform']['data_warehouse']['tables'])}")
        print(f"🗄️  MongoDB Collections: {len(summary['vietnam_ecommerce_data_platform']['data_lake']['collections'])}")
        print("📄 Summary saved to: vietnam_data_platform_summary.json")

    # Show access instructions
    data_lake.show_access_instructions()

    # Close connections
    if data_lake.pg_conn:
        data_lake.pg_conn.close()
    if data_lake.mongo_client:
        data_lake.mongo_client.close()

    logger.info("✅ Vietnam Data Lake Platform setup completed!")

if __name__ == "__main__":
    main()