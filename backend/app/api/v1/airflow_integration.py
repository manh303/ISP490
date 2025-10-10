#!/usr/bin/env python3
"""
Airflow Integration API Endpoints
=================================
Connect backend with Airflow for pipeline management
"""

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.responses import JSONResponse
from typing import Dict, List, Any, Optional
import httpx
import asyncio
import logging
from datetime import datetime, timedelta

from app.core.config import settings
from app.api.dependencies import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)

# Airflow Configuration
AIRFLOW_BASE_URL = settings.AIRFLOW_URL  # e.g., "https://your-airflow.railway.app"
AIRFLOW_USERNAME = settings.AIRFLOW_USERNAME
AIRFLOW_PASSWORD = settings.AIRFLOW_PASSWORD

class AirflowClient:
    """Client for Airflow REST API"""

    def __init__(self):
        self.base_url = AIRFLOW_BASE_URL
        self.auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

    async def get_dag_status(self, dag_id: str) -> Dict[str, Any]:
        """Get DAG status and last run info"""
        async with httpx.AsyncClient() as client:
            try:
                # Get DAG info
                dag_response = await client.get(
                    f"{self.base_url}/api/v1/dags/{dag_id}",
                    auth=self.auth
                )
                dag_response.raise_for_status()

                # Get latest DAG runs
                runs_response = await client.get(
                    f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                    params={"limit": 5, "order_by": "-execution_date"},
                    auth=self.auth
                )
                runs_response.raise_for_status()

                dag_info = dag_response.json()
                runs_info = runs_response.json()

                return {
                    "dag_id": dag_id,
                    "is_active": dag_info.get("is_active", False),
                    "is_paused": dag_info.get("is_paused", True),
                    "last_parsed_time": dag_info.get("last_parsed_time"),
                    "recent_runs": runs_info.get("dag_runs", []),
                    "next_dagrun": dag_info.get("next_dagrun"),
                    "tags": dag_info.get("tags", [])
                }

            except Exception as e:
                logger.error(f"Error getting DAG status: {e}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Unable to connect to Airflow: {str(e)}"
                )

    async def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None) -> Dict[str, Any]:
        """Trigger a DAG run manually"""
        async with httpx.AsyncClient() as client:
            try:
                payload = {
                    "execution_date": datetime.utcnow().isoformat(),
                    "conf": conf or {}
                }

                response = await client.post(
                    f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                    json=payload,
                    auth=self.auth
                )
                response.raise_for_status()

                return response.json()

            except Exception as e:
                logger.error(f"Error triggering DAG: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to trigger DAG: {str(e)}"
                )

    async def get_task_logs(self, dag_id: str, run_id: str, task_id: str) -> str:
        """Get task logs"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
                    auth=self.auth
                )
                response.raise_for_status()

                return response.text

            except Exception as e:
                logger.error(f"Error getting task logs: {e}")
                return f"Error retrieving logs: {str(e)}"

# Initialize Airflow client
airflow_client = AirflowClient()

@router.get("/pipeline/status")
async def get_pipeline_status(current_user: Any = Depends(get_current_user)):
    """Get comprehensive pipeline status"""
    try:
        # Main DSS pipeline
        main_pipeline = await airflow_client.get_dag_status("comprehensive_ecommerce_dss_pipeline")

        # Additional pipelines if any
        pipelines = {
            "main_pipeline": main_pipeline,
            "status": "running" if not main_pipeline["is_paused"] else "paused",
            "last_update": datetime.utcnow().isoformat()
        }

        return JSONResponse(content=pipelines)

    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to retrieve pipeline status"
        )

@router.post("/pipeline/trigger")
async def trigger_pipeline(
    dag_id: str = "comprehensive_ecommerce_dss_pipeline",
    force_refresh: bool = False,
    current_user: Any = Depends(get_current_user)
):
    """Trigger data pipeline manually"""
    try:
        conf = {
            "force_refresh": force_refresh,
            "triggered_by": "web_interface",
            "trigger_time": datetime.utcnow().isoformat()
        }

        result = await airflow_client.trigger_dag(dag_id, conf)

        return JSONResponse(content={
            "message": "Pipeline triggered successfully",
            "dag_run_id": result.get("dag_run_id"),
            "execution_date": result.get("execution_date"),
            "state": result.get("state")
        })

    except Exception as e:
        logger.error(f"Error triggering pipeline: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger pipeline: {str(e)}"
        )

@router.get("/pipeline/runs")
async def get_pipeline_runs(
    dag_id: str = "comprehensive_ecommerce_dss_pipeline",
    limit: int = 10,
    current_user: Any = Depends(get_current_user)
):
    """Get recent pipeline runs"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns",
                params={"limit": limit, "order_by": "-execution_date"},
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
            )
            response.raise_for_status()

            runs = response.json()

            # Process runs data for frontend
            processed_runs = []
            for run in runs.get("dag_runs", []):
                processed_runs.append({
                    "run_id": run.get("dag_run_id"),
                    "execution_date": run.get("execution_date"),
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "state": run.get("state"),
                    "external_trigger": run.get("external_trigger"),
                    "conf": run.get("conf", {})
                })

            return JSONResponse(content={
                "runs": processed_runs,
                "total_entries": runs.get("total_entries", 0)
            })

    except Exception as e:
        logger.error(f"Error getting pipeline runs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to retrieve pipeline runs"
        )

@router.get("/pipeline/metrics")
async def get_pipeline_metrics(current_user: Any = Depends(get_current_user)):
    """Get pipeline performance metrics"""
    try:
        # Get recent runs for metrics calculation
        dag_status = await airflow_client.get_dag_status("comprehensive_ecommerce_dss_pipeline")
        recent_runs = dag_status.get("recent_runs", [])

        # Calculate metrics
        total_runs = len(recent_runs)
        successful_runs = len([r for r in recent_runs if r.get("state") == "success"])
        failed_runs = len([r for r in recent_runs if r.get("state") == "failed"])

        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0

        # Calculate average duration
        durations = []
        for run in recent_runs:
            if run.get("start_date") and run.get("end_date"):
                start = datetime.fromisoformat(run["start_date"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(run["end_date"].replace("Z", "+00:00"))
                durations.append((end - start).total_seconds())

        avg_duration = sum(durations) / len(durations) if durations else 0

        return JSONResponse(content={
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "success_rate": round(success_rate, 2),
            "average_duration_seconds": round(avg_duration, 2),
            "pipeline_health": "healthy" if success_rate > 80 else "needs_attention",
            "last_calculated": datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Error calculating pipeline metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to calculate pipeline metrics"
        )

@router.get("/pipeline/logs/{run_id}/{task_id}")
async def get_task_logs(
    run_id: str,
    task_id: str,
    dag_id: str = "comprehensive_ecommerce_dss_pipeline",
    current_user: Any = Depends(get_current_user)
):
    """Get logs for a specific task"""
    try:
        logs = await airflow_client.get_task_logs(dag_id, run_id, task_id)

        return JSONResponse(content={
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "logs": logs
        })

    except Exception as e:
        logger.error(f"Error getting task logs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to retrieve task logs"
        )

@router.post("/pipeline/pause")
async def pause_pipeline(
    dag_id: str = "comprehensive_ecommerce_dss_pipeline",
    current_user: Any = Depends(get_current_user)
):
    """Pause the pipeline"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}",
                json={"is_paused": True},
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
            )
            response.raise_for_status()

            return JSONResponse(content={
                "message": f"Pipeline {dag_id} paused successfully"
            })

    except Exception as e:
        logger.error(f"Error pausing pipeline: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to pause pipeline"
        )

@router.post("/pipeline/resume")
async def resume_pipeline(
    dag_id: str = "comprehensive_ecommerce_dss_pipeline",
    current_user: Any = Depends(get_current_user)
):
    """Resume the pipeline"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}",
                json={"is_paused": False},
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
            )
            response.raise_for_status()

            return JSONResponse(content={
                "message": f"Pipeline {dag_id} resumed successfully"
            })

    except Exception as e:
        logger.error(f"Error resuming pipeline: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to resume pipeline"
        )