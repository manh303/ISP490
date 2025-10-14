#!/usr/bin/env python3
"""
Analytics API Endpoints
=======================
Provide analytics data from both PostgreSQL and MongoDB
"""

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import pymongo
import os

router = APIRouter()
logger = logging.getLogger(__name__)

# Database configurations
POSTGRES_URL = os.getenv('DATABASE_URL', 'postgresql://dss_user:dss_password_123@localhost:5432/ecommerce_dss')
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://admin:admin_password@localhost:27017/')

class AnalyticsService:
    """Service for analytics data"""

    def __init__(self):
        try:
            self.pg_engine = create_engine(POSTGRES_URL)
            self.mongo_client = pymongo.MongoClient(MONGO_URL)
            self.mongo_db = self.mongo_client['ecommerce_dss']
        except Exception as e:
            logger.error(f"Failed to initialize analytics service: {e}")

    def get_postgres_summary(self) -> Dict[str, Any]:
        """Get PostgreSQL data summary"""
        try:
            # Get table counts
            tables_query = """
            SELECT
                'customers' as table_name, COUNT(*) as count FROM customers
            UNION ALL
            SELECT 'products' as table_name, COUNT(*) as count FROM products
            UNION ALL
            SELECT 'orders' as table_name, COUNT(*) as count FROM orders
            """

            df = pd.read_sql(tables_query, self.pg_engine)
            table_counts = dict(zip(df['table_name'], df['count']))

            # Get basic stats
            stats_query = """
            SELECT
                COUNT(DISTINCT id) as total_customers,
                (SELECT COUNT(*) FROM products) as total_products,
                (SELECT COUNT(*) FROM orders) as total_orders
            FROM customers
            """

            stats_df = pd.read_sql(stats_query, self.pg_engine)
            stats_data = stats_df.iloc[0].to_dict() if len(stats_df) > 0 else {}

            return {
                "table_counts": table_counts,
                "stats": stats_data,
                "last_updated": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error getting PostgreSQL summary: {e}")
            return {"error": str(e)}

    def get_mongodb_summary(self) -> Dict[str, Any]:
        """Get MongoDB data summary"""
        try:
            collections = self.mongo_db.list_collection_names()
            collection_stats = {}

            for collection_name in collections:
                collection = self.mongo_db[collection_name]
                count = collection.count_documents({})
                collection_stats[collection_name] = count

            return {
                "collection_counts": collection_stats,
                "total_documents": sum(collection_stats.values()),
                "last_updated": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error getting MongoDB summary: {e}")
            return {"error": str(e)}

# Initialize service
analytics_service = AnalyticsService()

@router.get("/summary")
async def get_analytics_summary():
    """Get comprehensive analytics summary"""
    try:
        postgres_data = analytics_service.get_postgres_summary()
        mongodb_data = analytics_service.get_mongodb_summary()

        summary = {
            "timestamp": datetime.now().isoformat(),
            "postgresql": postgres_data,
            "mongodb": mongodb_data,
            "overall_health": {
                "postgres_tables": len(postgres_data.get("table_counts", {})),
                "mongo_collections": len(mongodb_data.get("collection_counts", {})),
                "total_records": sum(postgres_data.get("table_counts", {}).values()),
                "total_documents": mongodb_data.get("total_documents", 0)
            }
        }

        return JSONResponse(content=summary)

    except Exception as e:
        logger.error(f"Error getting analytics summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get analytics summary: {str(e)}"
        )

@router.get("/kpi")
def kpi():
    """Get key performance indicators"""
    try:
        postgres_data = analytics_service.get_postgres_summary()
        mongodb_data = analytics_service.get_mongodb_summary()

        return {
            "orders": postgres_data.get("table_counts", {}).get("orders", 0),
            "customers": postgres_data.get("table_counts", {}).get("customers", 0),
            "products": postgres_data.get("table_counts", {}).get("products", 0),
            "ml_features": mongodb_data.get("collection_counts", {}).get("ml_features_clv", 0),
            "recommendations": mongodb_data.get("collection_counts", {}).get("ml_recommendations", 0)
        }
    except Exception as e:
        return {"orders": 0, "customers": 0, "products": 0, "error": str(e)}