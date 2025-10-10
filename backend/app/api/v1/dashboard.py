#!/usr/bin/env python3
"""
Dashboard API Endpoints
========================
Provide dashboard data and metrics
"""

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from typing import Dict, List, Any
import logging
from datetime import datetime, timedelta
import pymongo
import os

router = APIRouter()
logger = logging.getLogger(__name__)

# Database configurations
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://admin:admin_password@localhost:27017/')

class DashboardService:
    """Service for dashboard data"""

    def __init__(self):
        try:
            self.mongo_client = pymongo.MongoClient(MONGO_URL)
            self.mongo_db = self.mongo_client['ecommerce_dss']
        except Exception as e:
            logger.error(f"Failed to initialize dashboard service: {e}")

    def get_key_metrics(self) -> Dict[str, Any]:
        """Get key metrics for dashboard"""
        try:
            metrics = {}

            # ML Models count
            ml_models = self.mongo_db.ml_models.count_documents({})
            metrics["ml_models"] = ml_models

            # Customer features
            customer_features = self.mongo_db.ml_features_clv.count_documents({})
            metrics["customer_features"] = customer_features

            # Product features
            product_features = self.mongo_db.ml_features_products.count_documents({})
            metrics["product_features"] = product_features

            # Recommendations
            recommendations = self.mongo_db.ml_recommendations.count_documents({})
            metrics["recommendations"] = recommendations

            # Active alerts
            active_alerts = self.mongo_db.alerts.count_documents({"is_active": True})
            metrics["active_alerts"] = active_alerts

            return metrics

        except Exception as e:
            logger.error(f"Error getting key metrics: {e}")
            return {}

# Initialize service
dashboard_service = DashboardService()

@router.get("/metrics")
async def get_dashboard_metrics():
    """Get key metrics for dashboard"""
    try:
        metrics = dashboard_service.get_key_metrics()

        response = {
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics,
            "system_status": "operational"
        }

        return JSONResponse(content=response)

    except Exception as e:
        logger.error(f"Error getting dashboard metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get dashboard metrics: {str(e)}"
        )
