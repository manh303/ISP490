#!/usr/bin/env python3
"""
Test script to run complete data pipeline and verify data flow
"""

import os
import sys
import time
import json
import logging
import pandas as pd
import psycopg2
import pymongo
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataPipelineValidator:
    """Complete data pipeline validation"""

    def __init__(self):
        # Database connections
        self.postgres_url = "postgresql://admin:admin@localhost:5432/ecom"
        self.mongo_url = "mongodb://admin:admin_password@localhost:27017/"
        self.backend_url = "http://localhost:8000"

        # Initialize connections
        self.pg_engine = create_engine(self.postgres_url)
        self.mongo_client = pymongo.MongoClient(self.mongo_url)
        self.mongo_db = self.mongo_client['ecommerce_dss']

        logger.info("✅ Database connections initialized")

    def run_data_collection(self):
        """Step 1: Run data collection"""
        logger.info("🔄 Step 1: Running data collection...")

        try:
            # Run Vietnam electronics data collector
            os.system('python "C:\\DoAn_FPT_FALL2025\\ecommerce-dss-project\\vietnam_electronics_collector.py"')

            logger.info("✅ Data collection completed")
            return True
        except Exception as e:
            logger.error(f"❌ Data collection failed: {e}")
            return False

    def verify_postgres_data(self):
        """Step 2: Verify PostgreSQL data"""
        logger.info("🔍 Step 2: Verifying PostgreSQL data...")

        try:
            # Check tables
            tables_query = """
            SELECT table_name,
                   (SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_name = t.table_name) as column_count
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
            """

            tables_df = pd.read_sql(tables_query, self.pg_engine)
            logger.info(f"📊 Found {len(tables_df)} tables in PostgreSQL:")
            for _, row in tables_df.iterrows():
                logger.info(f"  - {row['table_name']}: {row['column_count']} columns")

            # Check data in main tables
            main_tables = ['customers', 'products', 'orders', 'order_items']
            data_summary = {}

            for table in main_tables:
                try:
                    count_query = f"SELECT COUNT(*) as count FROM {table}"
                    result = pd.read_sql(count_query, self.pg_engine)
                    data_summary[table] = result.iloc[0]['count']
                    logger.info(f"  📈 {table}: {data_summary[table]} records")
                except Exception as e:
                    logger.warning(f"  ⚠️ Could not query {table}: {e}")
                    data_summary[table] = 0

            # Show sample data from products table
            if data_summary.get('products', 0) > 0:
                sample_query = "SELECT * FROM products LIMIT 3"
                sample_df = pd.read_sql(sample_query, self.pg_engine)
                logger.info(f"  🔍 Sample products data:")
                logger.info(f"    Columns: {list(sample_df.columns)}")
                logger.info(f"    First product: {sample_df.iloc[0].to_dict() if len(sample_df) > 0 else 'No data'}")

            logger.info("✅ PostgreSQL verification completed")
            return data_summary

        except Exception as e:
            logger.error(f"❌ PostgreSQL verification failed: {e}")
            return {}

    def verify_mongodb_data(self):
        """Step 3: Verify MongoDB data"""
        logger.info("🔍 Step 3: Verifying MongoDB data...")

        try:
            # List collections
            collections = self.mongo_db.list_collection_names()
            logger.info(f"📊 Found {len(collections)} collections in MongoDB:")

            data_summary = {}
            for collection_name in collections:
                collection = self.mongo_db[collection_name]
                count = collection.count_documents({})
                data_summary[collection_name] = count
                logger.info(f"  - {collection_name}: {count} documents")

                # Show sample document
                if count > 0:
                    sample = collection.find_one()
                    logger.info(f"    Sample fields: {list(sample.keys()) if sample else 'No data'}")

            # Check specific collections
            expected_collections = ['vietnam_products', 'analytics_data', 'processed_data']
            for col in expected_collections:
                if col in data_summary:
                    logger.info(f"  ✅ {col}: {data_summary[col]} documents")
                else:
                    logger.warning(f"  ⚠️ {col}: Collection not found")

            logger.info("✅ MongoDB verification completed")
            return data_summary

        except Exception as e:
            logger.error(f"❌ MongoDB verification failed: {e}")
            return {}

    def test_backend_endpoints(self):
        """Step 4: Test backend API endpoints"""
        logger.info("🔍 Step 4: Testing backend API endpoints...")

        endpoints = [
            "/health",
            "/api/v1/analytics/summary",
            "/api/v1/dashboard/metrics",
            "/api/v1/reports/list"
        ]

        results = {}

        for endpoint in endpoints:
            try:
                url = f"{self.backend_url}{endpoint}"
                response = requests.get(url, timeout=10)

                results[endpoint] = {
                    "status_code": response.status_code,
                    "response_time": response.elapsed.total_seconds(),
                    "success": response.status_code in [200, 201]
                }

                if response.status_code == 200:
                    try:
                        data = response.json()
                        results[endpoint]["data_size"] = len(str(data))
                        logger.info(f"  ✅ {endpoint}: {response.status_code} ({len(str(data))} bytes)")
                    except:
                        logger.info(f"  ✅ {endpoint}: {response.status_code} (non-JSON response)")
                else:
                    logger.warning(f"  ⚠️ {endpoint}: {response.status_code}")

            except Exception as e:
                logger.error(f"  ❌ {endpoint}: {e}")
                results[endpoint] = {"error": str(e), "success": False}

        logger.info("✅ Backend API testing completed")
        return results

    def generate_data_quality_report(self, pg_summary, mongo_summary, api_results):
        """Generate comprehensive data quality report"""
        logger.info("📊 Generating data quality report...")

        report = {
            "timestamp": datetime.now().isoformat(),
            "postgresql": {
                "tables_count": len(pg_summary),
                "total_records": sum(pg_summary.values()),
                "tables": pg_summary
            },
            "mongodb": {
                "collections_count": len(mongo_summary),
                "total_documents": sum(mongo_summary.values()),
                "collections": mongo_summary
            },
            "api_endpoints": {
                "total_endpoints": len(api_results),
                "successful_endpoints": sum(1 for r in api_results.values() if r.get("success", False)),
                "endpoints": api_results
            }
        }

        # Calculate overall health score
        postgres_health = min(100, (sum(pg_summary.values()) / 1000) * 100) if pg_summary else 0
        mongo_health = min(100, (sum(mongo_summary.values()) / 500) * 100) if mongo_summary else 0
        api_health = (report["api_endpoints"]["successful_endpoints"] / max(1, len(api_results))) * 100

        report["overall_health"] = {
            "postgres_score": round(postgres_health, 2),
            "mongodb_score": round(mongo_health, 2),
            "api_score": round(api_health, 2),
            "overall_score": round((postgres_health + mongo_health + api_health) / 3, 2)
        }

        # Save report
        report_file = f"pipeline_health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"📄 Report saved to: {report_file}")

        # Print summary
        print("\n" + "="*80)
        print("🎯 DATA PIPELINE HEALTH SUMMARY")
        print("="*80)
        print(f"📊 PostgreSQL: {report['postgresql']['total_records']} records across {report['postgresql']['tables_count']} tables")
        print(f"📊 MongoDB: {report['mongodb']['total_documents']} documents across {report['mongodb']['collections_count']} collections")
        print(f"🔗 API Health: {report['api_endpoints']['successful_endpoints']}/{report['api_endpoints']['total_endpoints']} endpoints working")
        print(f"🏆 Overall Score: {report['overall_health']['overall_score']}/100")
        print("="*80)

        return report

    def run_complete_validation(self):
        """Run complete pipeline validation"""
        logger.info("🚀 Starting complete data pipeline validation...")

        # Step 1: Data Collection
        collection_success = self.run_data_collection()
        time.sleep(5)  # Wait for data to be processed

        # Step 2: PostgreSQL Verification
        pg_summary = self.verify_postgres_data()

        # Step 3: MongoDB Verification
        mongo_summary = self.verify_mongodb_data()

        # Step 4: API Testing
        api_results = self.test_backend_endpoints()

        # Step 5: Generate Report
        report = self.generate_data_quality_report(pg_summary, mongo_summary, api_results)

        logger.info("🎉 Complete pipeline validation finished!")
        return report

if __name__ == "__main__":
    validator = DataPipelineValidator()
    report = validator.run_complete_validation()

    # Print final status
    if report["overall_health"]["overall_score"] > 70:
        print("✅ Pipeline is healthy and ready for dashboard creation!")
    else:
        print("⚠️ Pipeline needs attention before dashboard creation.")