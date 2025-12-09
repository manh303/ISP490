import os
from datetime import date
from typing import List, Optional
from contextlib import asynccontextmanager
from app.api.dependencies import require_role
import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from app.db_config import DATABASE_URL
from app.schemas.ml import (
    MLModelCreate,
    MLModelUpdate,
    MLModelResponse,
    PricePredictionHistoryResponse,
    OnlinePricePredictionRequest,
    OnlinePricePredictionResponse,
    RecommendationResponse,
    SentimentSummaryResponse,
    OnlineSentimentRequest,
    OnlineSentimentResponse,
    MLStatusSummary,
)
from app.services.ml_service import MLService
import logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ml", tags=["Machine Learning"])

# --------- DB dependency (asyncpg connection) ---------
async def get_db():
    """
    Tạo 1 kết nối asyncpg cho mỗi request ML.
    MLService đang dùng self.db.fetch / fetchrow / execute theo style asyncpg.
    """
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()


async def get_ml_service(db=Depends(get_db)) -> MLService:
    return MLService(db)


# --------- MODEL REGISTRY ENDPOINTS ---------

@router.get("/models", response_model=List[MLModelResponse],dependencies=[Depends(require_role("ML"))])
async def list_models(
    model_type: Optional[str] = Query(None, alias="type"),
    status: Optional[str] = None,
    service: MLService = Depends(get_ml_service),
):
    return await service.list_models(model_type=model_type, status=status)


@router.post("/models", response_model=MLModelResponse,dependencies=[Depends(require_role("ML"))])
async def create_model(
    payload: MLModelCreate,
    service: MLService = Depends(get_ml_service),
):
    return await service.create_model(payload)


@router.get("/models/{model_sk}", response_model=MLModelResponse,dependencies=[Depends(require_role("ML"))])
async def get_model(
    model_sk: int,
    service: MLService = Depends(get_ml_service),
):
    model = await service.get_model(model_sk)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


@router.patch("/models/{model_sk}", response_model=MLModelResponse,dependencies=[Depends(require_role("ML"))])
async def update_model(
    model_sk: int,
    payload: MLModelUpdate,
    service: MLService = Depends(get_ml_service),
):
    model = await service.update_model(model_sk, payload)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


# --------- PRICE PREDICTIONS ---------

@router.get("/price-predictions/history", response_model=PricePredictionHistoryResponse,dependencies=[Depends(require_role("ML"))])
async def get_price_prediction_history(
    product_key: str = Query(..., description="global_product_id_synced / product_key"),
    platform_code: str = Query(..., description="tiki / lazada"),
    from_date: date = Query(...),
    to_date: date = Query(...),
    model_name: Optional[str] = None,
    model_version: Optional[str] = None,
    service: MLService = Depends(get_ml_service),
):
    return await service.get_price_prediction_history(
        product_key=product_key,
        platform_code=platform_code,
        from_date=from_date,
        to_date=to_date,
        model_name=model_name,
        model_version=model_version,
    )


@router.post("/price-predictions/online", response_model=OnlinePricePredictionResponse,dependencies=[Depends(require_role("ML"))])
async def online_price_prediction(
    payload: OnlinePricePredictionRequest,
    service: MLService = Depends(get_ml_service),
):
    return await service.online_price_prediction(payload)


# --------- RECOMMENDATIONS ---------

@router.get("/recommendations", response_model=RecommendationResponse,dependencies=[Depends(require_role("ML"))])
async def get_recommendations(
    source_product_key: str = Query(..., description="VD: 'tiki_123456'"),
    platform_code: str = Query(..., description="tiki hoặc lazada"),
    model_name: Optional[str] = None,
    model_version: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    service: MLService = Depends(get_ml_service),
):
    rec = await service.get_recommendations(
        source_product_key=source_product_key,
        platform_code=platform_code,
        model_name=model_name,
        model_version=model_version,
        limit=limit,
    )
    if not rec:
        raise HTTPException(status_code=404, detail="No recommendations found")
    return rec

# --------- SENTIMENT (CLASSIFICATION) ---------

@router.get("/sentiment/summary", response_model=SentimentSummaryResponse,dependencies=[Depends(require_role("ML"))])
async def get_sentiment_summary(
    product_key: str = Query(...),
    platform_code: str = Query(...),
    from_date: date = Query(...),
    to_date: date = Query(...),
    model_name: Optional[str] = None,
    model_version: Optional[str] = None,
    service: MLService = Depends(get_ml_service),
):
    result = await service.get_sentiment_summary(
        product_key=product_key,
        platform_code=platform_code,
        from_date=from_date,
        to_date=to_date,
        model_name=model_name,
        model_version=model_version,
    )
    if not result:
        raise HTTPException(status_code=404, detail="No sentiment data found")
    return result


@router.post("/sentiment/online", response_model=OnlineSentimentResponse,dependencies=[Depends(require_role("ML"))])
async def online_sentiment(
    payload: OnlineSentimentRequest,
    service: MLService = Depends(get_ml_service),
):
    return await service.online_sentiment(payload)



# --------- STATUS SUMMARY ---------

@router.get("/status/summary", response_model=MLStatusSummary,dependencies=[Depends(require_role("ML"))])
async def get_status_summary(
    service: MLService = Depends(get_ml_service),
):
    return await service.get_status_summary()
