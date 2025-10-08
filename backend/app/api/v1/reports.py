#!/usr/bin/env python3
"""
Reports API Endpoints
=====================
Provide reporting data and analytics reports
"""

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from typing import Dict, List, Any
import logging
from datetime import datetime
import pymongo
import os

router = APIRouter()
logger = logging.getLogger(__name__)

# Database configurations
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://admin:admin_password@localhost:27017/')

class ReportsService:
    """Service for reports data"""

    def __init__(self):
        try:
            self.mongo_client = pymongo.MongoClient(MONGO_URL)
            self.mongo_db = self.mongo_client['ecommerce_dss']
        except Exception as e:
            logger.error(f"Failed to initialize reports service: {e}")

    def get_performance_reports(self) -> List[Dict[str, Any]]:
        """Get performance reports"""
        try:
            reports = list(self.mongo_db.performance_reports.find(
                {}, {"report_id": 1, "execution_date": 1, "pipeline_version": 1, "generated_at": 1}
            ).sort("generated_at", -1).limit(10))

            return reports

        except Exception as e:
            logger.error(f"Error getting performance reports: {e}")
            return []

# Initialize service
reports_service = ReportsService()

@router.get("/list")
async def get_reports_list():
    """Get list of available reports"""
    try:
        performance_reports = reports_service.get_performance_reports()

        reports_list = {
            "performance_reports": performance_reports,
            "total_reports": len(performance_reports),
            "last_updated": datetime.now().isoformat()
        }

        return JSONResponse(content=reports_list)

    except Exception as e:
        logger.error(f"Error getting reports list: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get reports list: {str(e)}"
        )
