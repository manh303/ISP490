# ✅ Checklist Cải Thiện API Analyst

**Mục tiêu:** Nâng điểm từ 71/100 → 95/100  
**Timeline:** 4-6 tuần

---

## 🔥 PHASE 1: CRITICAL (Tuần 1-2)

### Task 1: Export APIs (Excel/PDF)
**File mới:** `backend/app/api/v1/analytics_export.py`

#### [ ] 1.1. Tạo Export Router
```python
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
import pandas as pd
from openpyxl import Workbook
from reportlab.pdfgen import canvas

export_router = APIRouter(prefix="/analytics/export", tags=["Analytics Export"])
```

#### [ ] 1.2. Implement Excel Export
```python
@export_router.get("/overview/excel")
async def export_overview_excel(
    from_date: date,
    to_date: date,
    platform_code: Optional[str] = None,
    category_key: Optional[str] = None,
    service: AnalyticsService = Depends(get_analytics_service),
):
    # Get data from report API
    data = await service.get_overview_report(...)
    
    # Create Excel with multiple sheets
    wb = Workbook()
    
    # Sheet 1: KPIs
    ws_kpis = wb.active
    ws_kpis.title = "KPIs"
    ws_kpis.append(["Metric", "Value"])
    ws_kpis.append(["Total Revenue", data.kpis.total_revenue])
    ws_kpis.append(["Total Products", data.kpis.total_products])
    # ... more KPIs
    
    # Sheet 2: Trends
    ws_trends = wb.create_sheet("Trends")
    ws_trends.append(["Date", "Revenue", "Orders", "Avg Price", "Rating"])
    for point in data.trends.points:
        ws_trends.append([
            str(point.date),
            point.revenue,
            point.total_orders,
            point.avg_price,
            point.avg_rating
        ])
    
    # Sheet 3: Platform Comparison
    ws_platforms = wb.create_sheet("Platforms")
    ws_platforms.append(["Platform", "Revenue", "Products", "Rating"])
    for platform in data.platform_comparison:
        ws_platforms.append([
            platform.platform_name,
            platform.total_revenue,
            platform.total_products,
            platform.avg_rating
        ])
    
    # Save to BytesIO
    from io import BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    
    # Return as download
    filename = f"analytics_overview_{from_date}_{to_date}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
```

#### [ ] 1.3. Implement PDF Export
```python
@export_router.get("/overview/pdf")
async def export_overview_pdf(
    from_date: date,
    to_date: date,
    platform_code: Optional[str] = None,
    category_key: Optional[str] = None,
    service: AnalyticsService = Depends(get_analytics_service),
):
    data = await service.get_overview_report(...)
    
    # Create PDF
    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Table, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()
    
    # Title
    title = Paragraph(f"Analytics Report: {from_date} to {to_date}", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 20))
    
    # KPIs Table
    kpi_data = [
        ["Metric", "Value"],
        ["Total Revenue", f"{data.kpis.total_revenue:,.0f} ₫"],
        ["Total Products", str(data.kpis.total_products)],
        ["Total Reviews", str(data.kpis.total_reviews)],
        ["Avg Price", f"{data.kpis.avg_price:,.0f} ₫" if data.kpis.avg_price else "N/A"],
        ["Avg Rating", f"{data.kpis.avg_rating:.2f} ⭐" if data.kpis.avg_rating else "N/A"],
    ]
    kpi_table = Table(kpi_data)
    elements.append(kpi_table)
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    
    filename = f"analytics_overview_{from_date}_{to_date}.pdf"
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
```

#### [ ] 1.4. Add to main.py
```python
from api.v1.analytics_export import export_router
app.include_router(export_router, prefix=f"{settings.API_V1_PREFIX}")
```

#### [ ] 1.5. Install Dependencies
```bash
pip install openpyxl reportlab pandas
# Add to requirements.txt:
# openpyxl==3.1.2
# reportlab==4.0.7
# pandas==2.1.4
```

#### [ ] 1.6. Test Export
```bash
curl "http://localhost:8000/api/v1/analytics/export/overview/excel?from_date=2025-01-01&to_date=2025-01-31" -o report.xlsx
curl "http://localhost:8000/api/v1/analytics/export/overview/pdf?from_date=2025-01-01&to_date=2025-01-31" -o report.pdf
```

---

### Task 2: Period Comparison
**File sửa:** `backend/app/schemas/analytics.py`, `backend/app/services/analytics_service.py`

#### [ ] 2.1. Update Schema - OverviewKPIResponse
```python
# backend/app/schemas/analytics.py

class OverviewKPIResponse(BaseModel):
    from_date: date
    to_date: date
    platform_code: Optional[str] = None
    category_key: Optional[str] = None
    category_name: Optional[str] = None

    # Current period
    total_revenue: float
    total_products: int
    total_reviews: int
    avg_price: Optional[float] = None
    avg_rating: Optional[float] = None
    
    # NEW: Previous period
    total_revenue_previous: Optional[float] = None
    total_products_previous: Optional[int] = None
    total_reviews_previous: Optional[int] = None
    avg_price_previous: Optional[float] = None
    avg_rating_previous: Optional[float] = None
    
    # NEW: Changes
    total_revenue_change_pct: Optional[float] = None  # Percentage change
    total_products_change_pct: Optional[float] = None
    total_reviews_change_pct: Optional[float] = None
    avg_price_change_pct: Optional[float] = None
    avg_rating_change_pct: Optional[float] = None
    
    # NEW: Trends
    total_revenue_trend: Optional[str] = None  # "up" / "down" / "stable"
    total_products_trend: Optional[str] = None
    total_reviews_trend: Optional[str] = None
    avg_price_trend: Optional[str] = None
    avg_rating_trend: Optional[str] = None
```

#### [ ] 2.2. Update Service - get_overview_kpis
```python
# backend/app/services/analytics_service.py

async def get_overview_kpis(
    self,
    from_date: date,
    to_date: date,
    platform_code: Optional[str] = None,
    category_key: Optional[str] = None,
) -> OverviewKPIResponse:
    # Existing query for current period
    current_kpis = await self._query_kpis(from_date, to_date, platform_code, category_key)
    
    # NEW: Calculate previous period dates
    period_days = (to_date - from_date).days + 1
    previous_to_date = from_date - timedelta(days=1)
    previous_from_date = previous_to_date - timedelta(days=period_days - 1)
    
    # NEW: Query for previous period
    previous_kpis = await self._query_kpis(
        previous_from_date, 
        previous_to_date, 
        platform_code, 
        category_key
    )
    
    # NEW: Calculate changes
    def calc_change_pct(current, previous):
        if previous is None or previous == 0:
            return None
        return ((current - previous) / previous) * 100
    
    def calc_trend(change_pct):
        if change_pct is None:
            return None
        if change_pct > 5:
            return "up"
        elif change_pct < -5:
            return "down"
        else:
            return "stable"
    
    revenue_change = calc_change_pct(
        current_kpis.total_revenue, 
        previous_kpis.total_revenue
    )
    
    return OverviewKPIResponse(
        from_date=from_date,
        to_date=to_date,
        platform_code=platform_code,
        category_key=category_key,
        category_name=current_kpis.category_name,
        
        # Current
        total_revenue=current_kpis.total_revenue,
        total_products=current_kpis.total_products,
        total_reviews=current_kpis.total_reviews,
        avg_price=current_kpis.avg_price,
        avg_rating=current_kpis.avg_rating,
        
        # Previous
        total_revenue_previous=previous_kpis.total_revenue,
        total_products_previous=previous_kpis.total_products,
        total_reviews_previous=previous_kpis.total_reviews,
        avg_price_previous=previous_kpis.avg_price,
        avg_rating_previous=previous_kpis.avg_rating,
        
        # Changes
        total_revenue_change_pct=revenue_change,
        total_products_change_pct=calc_change_pct(
            current_kpis.total_products, 
            previous_kpis.total_products
        ),
        # ... more changes
        
        # Trends
        total_revenue_trend=calc_trend(revenue_change),
        # ... more trends
    )
```

#### [ ] 2.3. Test Period Comparison
```bash
curl "http://localhost:8000/api/v1/analytics/overview/kpis?from_date=2025-01-01&to_date=2025-01-31"
# Should return previous period data and change percentages
```

---

### Task 3: Data Quality Integration
**File sửa:** `backend/app/schemas/analytics.py`, tất cả analytics endpoints

#### [ ] 3.1. Create Data Quality Schema
```python
# backend/app/schemas/analytics.py

class DataQualityWarning(BaseModel):
    has_issues: bool
    severity: Optional[str] = None  # "low" / "medium" / "high"
    message: Optional[str] = None
    affected_period: Optional[str] = None
    affected_metrics: Optional[List[str]] = None
    recommendation: Optional[str] = None
    details_url: Optional[str] = None  # Link to data engineer dashboard
```

#### [ ] 3.2. Update All Response Schemas
```python
# Add to OverviewKPIResponse, PlatformComparisonResponse, etc.

class OverviewKPIResponse(BaseModel):
    # ... existing fields ...
    
    # NEW: Data quality
    data_quality: Optional[DataQualityWarning] = None
```

#### [ ] 3.3. Create Data Quality Service
```python
# backend/app/services/data_quality_checker.py

class DataQualityChecker:
    def __init__(self, db):
        self.db = db
    
    async def check_quality(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ) -> DataQualityWarning:
        """Check data quality for given period and filters"""
        
        # Query data engineer APIs or directly check tables
        issues = await self._get_data_quality_issues(
            from_date, to_date, platform_code, category_key
        )
        
        if not issues:
            return DataQualityWarning(has_issues=False)
        
        # Analyze issues
        critical_count = sum(1 for i in issues if i["severity"] == "critical")
        
        if critical_count > 0:
            severity = "high"
            message = f"{critical_count} critical data quality issues detected"
            recommendation = "Use data with extreme caution. Contact data team."
        elif len(issues) > 5:
            severity = "medium"
            message = f"{len(issues)} data quality issues detected"
            recommendation = "Some metrics may be incomplete. Review details."
        else:
            severity = "low"
            message = f"{len(issues)} minor data quality issues"
            recommendation = "Data is mostly reliable."
        
        affected_metrics = list(set([
            metric 
            for issue in issues 
            for metric in issue.get("affected_metrics", [])
        ]))
        
        return DataQualityWarning(
            has_issues=True,
            severity=severity,
            message=message,
            affected_period=f"{from_date} to {to_date}",
            affected_metrics=affected_metrics,
            recommendation=recommendation,
            details_url="/api/v1/data-engineer/data-quality/summary"
        )
    
    async def _get_data_quality_issues(self, from_date, to_date, platform_code, category_key):
        # Implementation: Query dwh.data_quality_checks table
        # or call Data Engineer API
        pass
```

#### [ ] 3.4. Integrate into Analytics Service
```python
# backend/app/services/analytics_service.py

class AnalyticsService:
    def __init__(self, db):
        self.db = db
        self.dq_checker = DataQualityChecker(db)  # NEW
    
    async def get_overview_kpis(self, ...):
        # ... existing logic ...
        
        # NEW: Check data quality
        dq_warning = await self.dq_checker.check_quality(
            from_date, to_date, platform_code, category_key
        )
        
        return OverviewKPIResponse(
            # ... existing fields ...
            data_quality=dq_warning  # NEW
        )
```

#### [ ] 3.5. Test Data Quality
```bash
curl "http://localhost:8000/api/v1/analytics/overview/kpis?from_date=2025-01-01&to_date=2025-01-31"
# Should include data_quality field
```

---

## 🟡 PHASE 2: IMPORTANT (Tuần 3-4)

### Task 4: Alert System
**File mới:** `backend/app/api/v1/alerts.py`, `backend/app/services/alert_service.py`

#### [ ] 4.1. Create Alerts Table
```sql
-- migrations/add_alerts_table.sql

CREATE TABLE IF NOT EXISTS analytics.alerts (
    alert_id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,  -- 'revenue_drop', 'rating_drop', 'spike_detected'
    severity VARCHAR(20) NOT NULL,    -- 'low', 'medium', 'high', 'critical'
    
    metric_name VARCHAR(50),
    metric_value DECIMAL,
    threshold_value DECIMAL,
    
    message TEXT NOT NULL,
    description TEXT,
    
    affected_entity_type VARCHAR(50),  -- 'platform', 'category', 'product'
    affected_entity_id VARCHAR(100),
    affected_entity_name VARCHAR(255),
    
    detection_date DATE NOT NULL,
    detection_time TIMESTAMP DEFAULT NOW(),
    
    status VARCHAR(20) DEFAULT 'active',  -- 'active', 'acknowledged', 'resolved'
    acknowledged_by INT,
    acknowledged_at TIMESTAMP,
    
    suggested_actions JSONB,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_alerts_detection_date ON analytics.alerts(detection_date);
CREATE INDEX idx_alerts_status ON analytics.alerts(status);
CREATE INDEX idx_alerts_severity ON analytics.alerts(severity);

-- Alert subscriptions
CREATE TABLE IF NOT EXISTS analytics.alert_subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    
    filters JSONB,  -- {"platform_code": "tiki", "category_key": "1"}
    
    notification_channel VARCHAR(50),  -- 'email', 'webhook', 'in_app'
    notification_config JSONB,
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);
```

#### [ ] 4.2. Create Alert Schemas
```python
# backend/app/schemas/alerts.py

from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional, List, Dict

class AlertResponse(BaseModel):
    alert_id: int
    alert_type: str
    severity: str
    
    metric_name: Optional[str]
    metric_value: Optional[float]
    threshold_value: Optional[float]
    
    message: str
    description: Optional[str]
    
    affected_entity_type: Optional[str]
    affected_entity_id: Optional[str]
    affected_entity_name: Optional[str]
    
    detection_date: date
    detection_time: datetime
    
    status: str
    suggested_actions: Optional[List[str]]

class AlertSubscriptionCreate(BaseModel):
    alert_type: str
    filters: Optional[Dict] = None
    notification_channel: str = "in_app"
    notification_config: Optional[Dict] = None

class AlertSubscriptionResponse(BaseModel):
    subscription_id: int
    user_id: int
    alert_type: str
    filters: Optional[Dict]
    notification_channel: str
    is_active: bool
```

#### [ ] 4.3. Create Alert Service
```python
# backend/app/services/alert_service.py

class AlertService:
    def __init__(self, db):
        self.db = db
    
    async def get_active_alerts(
        self,
        severity: Optional[str] = None,
        alert_type: Optional[str] = None,
        limit: int = 50
    ) -> List[AlertResponse]:
        sql = """
            SELECT * FROM analytics.alerts
            WHERE status = 'active'
        """
        if severity:
            sql += f" AND severity = '{severity}'"
        if alert_type:
            sql += f" AND alert_type = '{alert_type}'"
        
        sql += f" ORDER BY detection_time DESC LIMIT {limit}"
        
        rows = await self.db.fetch(sql)
        return [AlertResponse(**dict(row)) for row in rows]
    
    async def create_subscription(
        self,
        user_id: int,
        subscription: AlertSubscriptionCreate
    ) -> AlertSubscriptionResponse:
        sql = """
            INSERT INTO analytics.alert_subscriptions
            (user_id, alert_type, filters, notification_channel, notification_config)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        """
        row = await self.db.fetchrow(
            sql,
            user_id,
            subscription.alert_type,
            json.dumps(subscription.filters) if subscription.filters else None,
            subscription.notification_channel,
            json.dumps(subscription.notification_config) if subscription.notification_config else None,
        )
        return AlertSubscriptionResponse(**dict(row))
    
    async def detect_anomalies(self):
        """Background job to detect anomalies and create alerts"""
        # Implement anomaly detection logic
        # Compare current metrics with historical averages
        # Create alerts if thresholds exceeded
        pass
```

#### [ ] 4.4. Create Alert Router
```python
# backend/app/api/v1/alerts.py

from fastapi import APIRouter, Depends
from app.services.alert_service import AlertService
from app.schemas.alerts import *

alerts_router = APIRouter(prefix="/analytics/alerts", tags=["Analytics Alerts"])

@alerts_router.get("/active", response_model=List[AlertResponse])
async def get_active_alerts(
    severity: Optional[str] = None,
    alert_type: Optional[str] = None,
    service: AlertService = Depends(get_alert_service),
):
    return await service.get_active_alerts(severity, alert_type)

@alerts_router.get("/history", response_model=List[AlertResponse])
async def get_alert_history(
    from_date: date = Query(...),
    to_date: date = Query(...),
    service: AlertService = Depends(get_alert_service),
):
    return await service.get_alert_history(from_date, to_date)

@alerts_router.post("/subscribe", response_model=AlertSubscriptionResponse)
async def subscribe_to_alerts(
    subscription: AlertSubscriptionCreate,
    current_user: dict = Depends(get_current_user),
    service: AlertService = Depends(get_alert_service),
):
    return await service.create_subscription(current_user["user_id"], subscription)

@alerts_router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: int,
    current_user: dict = Depends(get_current_user),
    service: AlertService = Depends(get_alert_service),
):
    return await service.acknowledge_alert(alert_id, current_user["user_id"])
```

#### [ ] 4.5. Add Background Job
```python
# backend/app/background_jobs.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

async def anomaly_detection_job():
    alert_service = AlertService(db)
    await alert_service.detect_anomalies()

# Run every 15 minutes
scheduler.add_job(anomaly_detection_job, 'interval', minutes=15)
scheduler.start()
```

---

### Task 5: Advanced Filtering

#### [ ] 5.1. Update get_top_products to Support Multiple Filters
```python
# backend/app/api/v1/analytics.py

@router.get("/products/top", response_model=List[TopProductItem])
async def get_top_products(
    from_date: date = Query(...),
    to_date: date = Query(...),
    metric: str = Query("revenue"),
    
    # NEW: Multiple filters
    platform_codes: Optional[str] = Query(None, description="Comma-separated: tiki,lazada"),
    category_keys: Optional[str] = Query(None, description="Comma-separated: 1,2,3"),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    rating_min: Optional[float] = Query(None),
    has_reviews: Optional[bool] = Query(None),
    
    limit: int = Query(20, ge=1, le=100),
    service: AnalyticsService = Depends(get_analytics_service),
):
    # Parse comma-separated values
    platform_list = platform_codes.split(",") if platform_codes else None
    category_list = category_keys.split(",") if category_keys else None
    
    return await service.get_top_products_advanced(
        from_date=from_date,
        to_date=to_date,
        metric=metric,
        platform_codes=platform_list,
        category_keys=category_list,
        price_min=price_min,
        price_max=price_max,
        rating_min=rating_min,
        has_reviews=has_reviews,
        limit=limit,
    )
```

#### [ ] 5.2. Implement Advanced Query in Service
```python
# backend/app/services/analytics_service.py

async def get_top_products_advanced(
    self,
    from_date: date,
    to_date: date,
    metric: str,
    platform_codes: Optional[List[str]] = None,
    category_keys: Optional[List[str]] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    rating_min: Optional[float] = None,
    has_reviews: Optional[bool] = None,
    limit: int = 20,
):
    # Build dynamic WHERE clause
    conditions = ["f.snapshot_date BETWEEN $1 AND $2"]
    params = [from_date, to_date]
    param_counter = 3
    
    if platform_codes:
        conditions.append(f"dp.platform_code = ANY(${param_counter})")
        params.append(platform_codes)
        param_counter += 1
    
    if category_keys:
        conditions.append(f"CAST(dc.category_sk AS TEXT) = ANY(${param_counter})")
        params.append(category_keys)
        param_counter += 1
    
    if price_min is not None:
        conditions.append(f"f.current_price >= ${param_counter}")
        params.append(price_min)
        param_counter += 1
    
    if price_max is not None:
        conditions.append(f"f.current_price <= ${param_counter}")
        params.append(price_max)
        param_counter += 1
    
    if rating_min is not None:
        conditions.append(f"f.avg_rating >= ${param_counter}")
        params.append(rating_min)
        param_counter += 1
    
    if has_reviews is not None:
        if has_reviews:
            conditions.append("f.total_reviews > 0")
        else:
            conditions.append("f.total_reviews = 0")
    
    where_clause = " AND ".join(conditions)
    
    # Metric-specific ORDER BY
    order_by = {
        "revenue": "total_revenue DESC",
        "review_count": "total_reviews DESC",
        "avg_rating": "avg_rating DESC NULLS LAST",
        "price_growth": "price_growth DESC NULLS LAST",
    }.get(metric, "total_revenue DESC")
    
    sql = f"""
        SELECT
            dp.product_key,
            dp.product_name,
            dpl.platform_code,
            dc.category_sk AS category_key,
            dc.category_lvl1 || ' > ' || COALESCE(dc.category_lvl2, '') AS category_name,
            SUM(f.current_price * f.total_reviews) AS total_revenue,
            SUM(f.total_reviews) AS total_reviews,
            AVG(f.avg_rating) AS avg_rating,
            AVG(f.current_price) AS avg_price
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_product dp ON f.product_sk = dp.product_sk
        JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
        JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
        WHERE {where_clause}
        GROUP BY dp.product_key, dp.product_name, dpl.platform_code, dc.category_sk, category_name
        ORDER BY {order_by}
        LIMIT {limit}
    """
    
    rows = await self.db.fetch(sql, *params)
    return [TopProductItem(**dict(row)) for row in rows]
```

---

### Task 6: Pagination

#### [ ] 6.1. Create Pagination Schema
```python
# backend/app/schemas/common.py

class PaginationMeta(BaseModel):
    total_items: int
    total_pages: int
    current_page: int
    page_size: int
    has_next: bool
    has_previous: bool

class PaginatedResponse(BaseModel):
    items: List[Any]
    pagination: PaginationMeta
```

#### [ ] 6.2. Update Products API with Pagination
```python
# backend/app/api/v1/analytics.py

@router.get("/products/top")
async def get_top_products(
    from_date: date = Query(...),
    to_date: date = Query(...),
    metric: str = Query("revenue"),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    
    # NEW: Pagination params
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    
    service: AnalyticsService = Depends(get_analytics_service),
):
    result = await service.get_top_products_paginated(
        from_date=from_date,
        to_date=to_date,
        metric=metric,
        platform_code=platform_code,
        category_key=category_key,
        page=page,
        page_size=page_size,
    )
    
    return {
        "items": result["items"],
        "pagination": {
            "total_items": result["total_count"],
            "total_pages": (result["total_count"] + page_size - 1) // page_size,
            "current_page": page,
            "page_size": page_size,
            "has_next": page * page_size < result["total_count"],
            "has_previous": page > 1,
        }
    }
```

#### [ ] 6.3. Implement Paginated Service Method
```python
# backend/app/services/analytics_service.py

async def get_top_products_paginated(
    self,
    from_date: date,
    to_date: date,
    metric: str,
    platform_code: Optional[str] = None,
    category_key: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
) -> Dict[str, Any]:
    """Get top products with pagination support"""
    
    # Build WHERE clause
    conditions = ["f.snapshot_date BETWEEN $1 AND $2"]
    params = [from_date, to_date]
    param_counter = 3
    
    if platform_code:
        conditions.append(f"dpl.platform_code = ${param_counter}")
        params.append(platform_code)
        param_counter += 1
    
    if category_key:
        conditions.append(f"dc.category_sk = ${param_counter}")
        params.append(int(category_key))
        param_counter += 1
    
    where_clause = " AND ".join(conditions)
    
    # Metric-specific ORDER BY
    order_by = {
        "revenue": "total_revenue DESC",
        "review_count": "total_reviews DESC",
        "avg_rating": "avg_rating DESC NULLS LAST",
        "price_growth": "price_growth DESC NULLS LAST",
    }.get(metric, "total_revenue DESC")
    
    # Get total count first
    count_sql = f"""
        SELECT COUNT(DISTINCT dp.product_key)
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_product dp ON f.product_sk = dp.product_sk
        JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
        JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
        WHERE {where_clause}
    """
    total_count = await self.db.fetchval(count_sql, *params)
    
    # Get paginated items
    offset = (page - 1) * page_size
    
    items_sql = f"""
        SELECT
            dp.product_key,
            dp.product_name,
            dpl.platform_code,
            dc.category_sk AS category_key,
            dc.category_lvl1 || ' > ' || COALESCE(dc.category_lvl2, '') AS category_name,
            SUM(f.current_price * f.total_reviews) AS total_revenue,
            SUM(f.total_reviews) AS total_reviews,
            AVG(f.avg_rating) AS avg_rating,
            AVG(f.current_price) AS avg_price
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_product dp ON f.product_sk = dp.product_sk
        JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
        JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
        WHERE {where_clause}
        GROUP BY dp.product_key, dp.product_name, dpl.platform_code, dc.category_sk, category_name
        ORDER BY {order_by}
        LIMIT {page_size} OFFSET {offset}
    """
    
    rows = await self.db.fetch(items_sql, *params)
    items = [TopProductItem(**dict(row)) for row in rows]
    
    return {
        "items": items,
        "total_count": total_count or 0
    }
```

#### [ ] 6.4. Test Pagination
```bash
# Page 1
curl "http://localhost:8000/api/v1/analytics/products/top?from_date=2025-01-01&to_date=2025-01-31&page=1&page_size=20"

# Page 2
curl "http://localhost:8000/api/v1/analytics/products/top?from_date=2025-01-01&to_date=2025-01-31&page=2&page_size=20"

# Verify pagination metadata
```

---

## 🟢 PHASE 3: NICE TO HAVE (Tuần 5-6)

### Task 7: Benchmark APIs
**Mục đích:** Cho phép analyst so sánh performance với market benchmark

#### [ ] 7.1. Create Benchmark Schema
```python
# backend/app/schemas/analytics.py

class BenchmarkMetrics(BaseModel):
    avg_price: Optional[float]
    avg_rating: Optional[float]
    avg_reviews_per_product: Optional[float]
    price_volatility: Optional[float]  # Standard deviation

class BenchmarkPosition(BaseModel):
    metric_name: str
    your_value: float
    market_average: float
    market_median: float
    your_percentile: int  # 0-100: where you stand
    position_label: str   # "Above Average" / "Below Average" / "Average"

class CategoryBenchmarkResponse(BaseModel):
    category_key: str
    category_name: str
    from_date: date
    to_date: date
    
    your_metrics: BenchmarkMetrics
    market_metrics: BenchmarkMetrics
    
    positions: List[BenchmarkPosition]
    
    insights: List[str]  # Human-readable insights
```

#### [ ] 7.2. Implement Benchmark Service
```python
# backend/app/services/benchmark_service.py

class BenchmarkService:
    def __init__(self, db):
        self.db = db
    
    async def get_category_benchmark(
        self,
        category_key: str,
        from_date: date,
        to_date: date,
        your_platform_code: Optional[str] = None,
    ) -> CategoryBenchmarkResponse:
        """
        Compare your performance vs market average in a category
        
        Logic:
        - "Your metrics" = your products in this category
        - "Market metrics" = all products in this category (across all platforms)
        - Calculate percentile position
        """
        
        # Get YOUR metrics
        your_metrics = await self._get_metrics(
            category_key, from_date, to_date, your_platform_code
        )
        
        # Get MARKET metrics (all platforms)
        market_metrics = await self._get_metrics(
            category_key, from_date, to_date, platform_code=None
        )
        
        # Calculate positions
        positions = []
        
        # Price position
        if your_metrics.avg_price and market_metrics.avg_price:
            price_percentile = await self._calculate_percentile(
                category_key, from_date, to_date, 
                metric="avg_price", 
                your_value=your_metrics.avg_price
            )
            positions.append(BenchmarkPosition(
                metric_name="Average Price",
                your_value=your_metrics.avg_price,
                market_average=market_metrics.avg_price,
                market_median=await self._get_median(category_key, from_date, to_date, "price"),
                your_percentile=price_percentile,
                position_label=self._get_position_label(price_percentile, "price")
            ))
        
        # Rating position
        if your_metrics.avg_rating and market_metrics.avg_rating:
            rating_percentile = await self._calculate_percentile(
                category_key, from_date, to_date,
                metric="avg_rating",
                your_value=your_metrics.avg_rating
            )
            positions.append(BenchmarkPosition(
                metric_name="Average Rating",
                your_value=your_metrics.avg_rating,
                market_average=market_metrics.avg_rating,
                market_median=await self._get_median(category_key, from_date, to_date, "rating"),
                your_percentile=rating_percentile,
                position_label=self._get_position_label(rating_percentile, "rating")
            ))
        
        # Generate insights
        insights = self._generate_insights(positions, your_metrics, market_metrics)
        
        return CategoryBenchmarkResponse(
            category_key=category_key,
            category_name=await self._get_category_name(category_key),
            from_date=from_date,
            to_date=to_date,
            your_metrics=your_metrics,
            market_metrics=market_metrics,
            positions=positions,
            insights=insights
        )
    
    async def _get_metrics(
        self, 
        category_key: str, 
        from_date: date, 
        to_date: date,
        platform_code: Optional[str] = None
    ) -> BenchmarkMetrics:
        """Get aggregated metrics for a category"""
        conditions = ["f.snapshot_date BETWEEN $1 AND $2", "dc.category_sk = $3"]
        params = [from_date, to_date, int(category_key)]
        
        if platform_code:
            conditions.append("dpl.platform_code = $4")
            params.append(platform_code)
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            SELECT
                AVG(f.current_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating,
                AVG(f.total_reviews) AS avg_reviews_per_product,
                STDDEV(f.current_price) AS price_volatility
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
            JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
            WHERE {where_clause}
        """
        
        row = await self.db.fetchrow(sql, *params)
        return BenchmarkMetrics(
            avg_price=_safe_float(row['avg_price']),
            avg_rating=_safe_float(row['avg_rating']),
            avg_reviews_per_product=_safe_float(row['avg_reviews_per_product']),
            price_volatility=_safe_float(row['price_volatility'])
        )
    
    async def _calculate_percentile(
        self,
        category_key: str,
        from_date: date,
        to_date: date,
        metric: str,
        your_value: float
    ) -> int:
        """Calculate percentile position (0-100)"""
        
        # Get all values for this metric
        metric_column = {
            "avg_price": "f.current_price",
            "avg_rating": "f.avg_rating"
        }.get(metric, "f.current_price")
        
        sql = f"""
            SELECT COUNT(*) AS total_count,
                   SUM(CASE WHEN {metric_column} <= $4 THEN 1 ELSE 0 END) AS below_count
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
            WHERE f.snapshot_date BETWEEN $1 AND $2
              AND dc.category_sk = $3
              AND {metric_column} IS NOT NULL
        """
        
        row = await self.db.fetchrow(sql, from_date, to_date, int(category_key), your_value)
        
        if row['total_count'] == 0:
            return 50  # Default to median
        
        percentile = int((row['below_count'] / row['total_count']) * 100)
        return percentile
    
    async def _get_median(
        self,
        category_key: str,
        from_date: date,
        to_date: date,
        metric: str
    ) -> float:
        """Get median value for a metric"""
        
        metric_column = {
            "price": "f.current_price",
            "rating": "f.avg_rating"
        }.get(metric, "f.current_price")
        
        sql = f"""
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {metric_column}) AS median_value
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
            WHERE f.snapshot_date BETWEEN $1 AND $2
              AND dc.category_sk = $3
              AND {metric_column} IS NOT NULL
        """
        
        row = await self.db.fetchrow(sql, from_date, to_date, int(category_key))
        return _safe_float(row['median_value']) or 0.0
    
    def _get_position_label(self, percentile: int, metric_type: str) -> str:
        """Convert percentile to human-readable label"""
        
        # For price: lower is better (cheaper)
        # For rating: higher is better
        
        if metric_type == "price":
            if percentile <= 25:
                return "Very Competitive (Low Price)"
            elif percentile <= 50:
                return "Below Average Price"
            elif percentile <= 75:
                return "Above Average Price"
            else:
                return "Premium Pricing"
        else:  # rating
            if percentile >= 75:
                return "Excellent (Top 25%)"
            elif percentile >= 50:
                return "Above Average"
            elif percentile >= 25:
                return "Below Average"
            else:
                return "Needs Improvement"
    
    def _generate_insights(
        self,
        positions: List[BenchmarkPosition],
        your_metrics: BenchmarkMetrics,
        market_metrics: BenchmarkMetrics
    ) -> List[str]:
        """Generate actionable insights"""
        
        insights = []
        
        # Price insight
        price_pos = next((p for p in positions if p.metric_name == "Average Price"), None)
        if price_pos:
            if price_pos.your_percentile < 50:
                insights.append(
                    f"💰 Your prices are {price_pos.your_percentile}th percentile - "
                    f"cheaper than most competitors. Consider if you can increase margins."
                )
            elif price_pos.your_percentile > 75:
                insights.append(
                    f"💰 Your prices are in the top 25% (premium). "
                    f"Ensure your value proposition justifies the premium."
                )
        
        # Rating insight
        rating_pos = next((p for p in positions if p.metric_name == "Average Rating"), None)
        if rating_pos:
            if rating_pos.your_percentile >= 75:
                insights.append(
                    f"⭐ Excellent ratings ({rating_pos.your_value:.2f}) - "
                    f"better than {rating_pos.your_percentile}% of market. Leverage this in marketing!"
                )
            elif rating_pos.your_percentile < 25:
                insights.append(
                    f"⚠️ Ratings below market average. "
                    f"Focus on product quality and customer satisfaction."
                )
        
        # Volatility insight
        if your_metrics.price_volatility and market_metrics.price_volatility:
            if your_metrics.price_volatility > market_metrics.price_volatility * 1.5:
                insights.append(
                    f"📊 Your price volatility is high ({your_metrics.price_volatility:.0f}). "
                    f"Consider more consistent pricing strategy."
                )
        
        return insights
    
    async def _get_category_name(self, category_key: str) -> str:
        sql = """
            SELECT category_lvl1 || ' > ' || COALESCE(category_lvl2, '') AS name
            FROM dwh.dim_category
            WHERE category_sk = $1
        """
        row = await self.db.fetchrow(sql, int(category_key))
        return row['name'] if row else "Unknown Category"
```

#### [ ] 7.3. Create Benchmark Router
```python
# backend/app/api/v1/benchmark.py

from fastapi import APIRouter, Depends, Query
from app.services.benchmark_service import BenchmarkService
from app.schemas.analytics import CategoryBenchmarkResponse

benchmark_router = APIRouter(prefix="/analytics/benchmark", tags=["Analytics Benchmark"])

@benchmark_router.get("/category/{category_key}", response_model=CategoryBenchmarkResponse)
async def get_category_benchmark(
    category_key: str,
    from_date: date = Query(...),
    to_date: date = Query(...),
    your_platform_code: Optional[str] = Query(
        None, 
        description="Your platform to compare against market. If null, compares all your products."
    ),
    service: BenchmarkService = Depends(get_benchmark_service),
):
    """
    Compare your performance vs market benchmark in a category
    
    Example:
    - category_key = "1" (Electronics > Mobile Phones)
    - your_platform_code = "tiki" (compare Tiki vs market)
    
    Returns:
    - Your metrics vs market average
    - Percentile positions (where you rank)
    - Actionable insights
    """
    return await service.get_category_benchmark(
        category_key, from_date, to_date, your_platform_code
    )

@benchmark_router.get("/platform/{platform_code}", response_model=PlatformBenchmarkResponse)
async def get_platform_benchmark(
    platform_code: str,
    from_date: date = Query(...),
    to_date: date = Query(...),
    service: BenchmarkService = Depends(get_benchmark_service),
):
    """Compare platform overall performance vs market"""
    return await service.get_platform_benchmark(platform_code, from_date, to_date)
```

#### [ ] 7.4. Add to main.py
```python
from api.v1.benchmark import benchmark_router
app.include_router(benchmark_router, prefix=f"{settings.API_V1_PREFIX}")
```

#### [ ] 7.5. Test Benchmark API
```bash
curl "http://localhost:8000/api/v1/analytics/benchmark/category/1?from_date=2025-01-01&to_date=2025-01-31&your_platform_code=tiki"
```

---

### Task 8: Drill-down APIs
**Mục đích:** Cho phép analyst drill từ high-level xuống detail level

#### [ ] 8.1. Create Drill-down Schema
```python
# backend/app/schemas/analytics.py

class DrilldownLevel(BaseModel):
    level: int  # 1, 2, 3...
    level_name: str  # "Platform", "Category", "Product"
    entity_key: str
    entity_name: str
    
    metrics: Dict[str, Any]  # KPIs at this level
    
    has_children: bool
    child_count: Optional[int] = None

class DrilldownPath(BaseModel):
    """Breadcrumb trail"""
    levels: List[DrilldownLevel]
    current_level: int

class DrilldownResponse(BaseModel):
    path: DrilldownPath
    current_level_data: List[DrilldownLevel]
    aggregation_type: str  # "platform", "category", "product"
```

#### [ ] 8.2. Implement Drill-down Service
```python
# backend/app/services/drilldown_service.py

class DrilldownService:
    """
    Support hierarchical drill-down analysis:
    
    Level 0: Overall (all platforms, all categories)
    Level 1: By Platform (Tiki, Lazada)
    Level 2: By Category within Platform
    Level 3: By Product within Category
    """
    
    def __init__(self, db):
        self.db = db
    
    async def get_drilldown(
        self,
        from_date: date,
        to_date: date,
        level: int = 0,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ) -> DrilldownResponse:
        """
        Get drill-down data at specified level
        
        Examples:
        - level=0: Overall summary
        - level=1, platform_code=None: List all platforms
        - level=2, platform_code="tiki": List categories in Tiki
        - level=3, platform_code="tiki", category_key="1": List products in Tiki > Category 1
        """
        
        # Build breadcrumb path
        path = await self._build_path(level, platform_code, category_key)
        
        # Get current level data
        if level == 0:
            current_data = await self._get_overall_summary(from_date, to_date)
        elif level == 1:
            current_data = await self._get_platforms(from_date, to_date)
        elif level == 2 and platform_code:
            current_data = await self._get_categories(from_date, to_date, platform_code)
        elif level == 3 and platform_code and category_key:
            current_data = await self._get_products(from_date, to_date, platform_code, category_key)
        else:
            raise ValueError("Invalid drill-down parameters")
        
        return DrilldownResponse(
            path=path,
            current_level_data=current_data,
            aggregation_type=self._get_aggregation_type(level)
        )
    
    async def _get_overall_summary(
        self, 
        from_date: date, 
        to_date: date
    ) -> List[DrilldownLevel]:
        """Level 0: Overall summary"""
        
        sql = """
            SELECT
                SUM(f.current_price * f.total_reviews) AS total_revenue,
                COUNT(DISTINCT f.product_sk) AS total_products,
                SUM(f.total_reviews) AS total_reviews,
                AVG(f.current_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating
            FROM dwh.fact_product_daily f
            WHERE f.snapshot_date BETWEEN $1 AND $2
        """
        
        row = await self.db.fetchrow(sql, from_date, to_date)
        
        return [DrilldownLevel(
            level=0,
            level_name="Overall",
            entity_key="all",
            entity_name="All Platforms & Categories",
            metrics={
                "total_revenue": _safe_float(row['total_revenue']) or 0,
                "total_products": row['total_products'] or 0,
                "total_reviews": row['total_reviews'] or 0,
                "avg_price": _safe_float(row['avg_price']),
                "avg_rating": _safe_float(row['avg_rating']),
            },
            has_children=True,
            child_count=await self._count_platforms()
        )]
    
    async def _get_platforms(
        self, 
        from_date: date, 
        to_date: date
    ) -> List[DrilldownLevel]:
        """Level 1: By Platform"""
        
        sql = """
            SELECT
                dpl.platform_code,
                dpl.platform_name,
                SUM(f.current_price * f.total_reviews) AS total_revenue,
                COUNT(DISTINCT f.product_sk) AS total_products,
                SUM(f.total_reviews) AS total_reviews,
                AVG(f.current_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
            WHERE f.snapshot_date BETWEEN $1 AND $2
            GROUP BY dpl.platform_code, dpl.platform_name
            ORDER BY total_revenue DESC
        """
        
        rows = await self.db.fetch(sql, from_date, to_date)
        
        result = []
        for row in rows:
            child_count = await self._count_categories(row['platform_code'])
            result.append(DrilldownLevel(
                level=1,
                level_name="Platform",
                entity_key=row['platform_code'],
                entity_name=row['platform_name'],
                metrics={
                    "total_revenue": _safe_float(row['total_revenue']) or 0,
                    "total_products": row['total_products'] or 0,
                    "total_reviews": row['total_reviews'] or 0,
                    "avg_price": _safe_float(row['avg_price']),
                    "avg_rating": _safe_float(row['avg_rating']),
                },
                has_children=child_count > 0,
                child_count=child_count
            ))
        
        return result
    
    async def _get_categories(
        self,
        from_date: date,
        to_date: date,
        platform_code: str
    ) -> List[DrilldownLevel]:
        """Level 2: Categories within Platform"""
        
        sql = """
            SELECT
                dc.category_sk,
                dc.category_lvl1 || ' > ' || COALESCE(dc.category_lvl2, '') AS category_name,
                SUM(f.current_price * f.total_reviews) AS total_revenue,
                COUNT(DISTINCT f.product_sk) AS total_products,
                SUM(f.total_reviews) AS total_reviews,
                AVG(f.current_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
            JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
            WHERE f.snapshot_date BETWEEN $1 AND $2
              AND dpl.platform_code = $3
            GROUP BY dc.category_sk, category_name
            ORDER BY total_revenue DESC
        """
        
        rows = await self.db.fetch(sql, from_date, to_date, platform_code)
        
        result = []
        for row in rows:
            child_count = await self._count_products(platform_code, str(row['category_sk']))
            result.append(DrilldownLevel(
                level=2,
                level_name="Category",
                entity_key=str(row['category_sk']),
                entity_name=row['category_name'],
                metrics={
                    "total_revenue": _safe_float(row['total_revenue']) or 0,
                    "total_products": row['total_products'] or 0,
                    "total_reviews": row['total_reviews'] or 0,
                    "avg_price": _safe_float(row['avg_price']),
                    "avg_rating": _safe_float(row['avg_rating']),
                },
                has_children=child_count > 0,
                child_count=child_count
            ))
        
        return result
    
    async def _get_products(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
        category_key: str
    ) -> List[DrilldownLevel]:
        """Level 3: Products within Category"""
        
        sql = """
            SELECT
                dp.product_key,
                dp.product_name,
                SUM(f.current_price * f.total_reviews) AS total_revenue,
                SUM(f.total_reviews) AS total_reviews,
                AVG(f.current_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_product dp ON f.product_sk = dp.product_sk
            JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
            JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
            WHERE f.snapshot_date BETWEEN $1 AND $2
              AND dpl.platform_code = $3
              AND dc.category_sk = $4
            GROUP BY dp.product_key, dp.product_name
            ORDER BY total_revenue DESC
            LIMIT 100
        """
        
        rows = await self.db.fetch(sql, from_date, to_date, platform_code, int(category_key))
        
        result = []
        for row in rows:
            result.append(DrilldownLevel(
                level=3,
                level_name="Product",
                entity_key=row['product_key'],
                entity_name=row['product_name'],
                metrics={
                    "total_revenue": _safe_float(row['total_revenue']) or 0,
                    "total_reviews": row['total_reviews'] or 0,
                    "avg_price": _safe_float(row['avg_price']),
                    "avg_rating": _safe_float(row['avg_rating']),
                },
                has_children=False,
                child_count=0
            ))
        
        return result
    
    async def _build_path(
        self,
        level: int,
        platform_code: Optional[str],
        category_key: Optional[str]
    ) -> DrilldownPath:
        """Build breadcrumb trail"""
        
        levels = []
        
        # Level 0: Always present
        levels.append(DrilldownLevel(
            level=0,
            level_name="Overall",
            entity_key="all",
            entity_name="All",
            metrics={},
            has_children=True
        ))
        
        # Level 1: Platform
        if level >= 1 and platform_code:
            platform_name = await self._get_platform_name(platform_code)
            levels.append(DrilldownLevel(
                level=1,
                level_name="Platform",
                entity_key=platform_code,
                entity_name=platform_name,
                metrics={},
                has_children=True
            ))
        
        # Level 2: Category
        if level >= 2 and category_key:
            category_name = await self._get_category_name(category_key)
            levels.append(DrilldownLevel(
                level=2,
                level_name="Category",
                entity_key=category_key,
                entity_name=category_name,
                metrics={},
                has_children=True
            ))
        
        return DrilldownPath(
            levels=levels,
            current_level=level
        )
    
    def _get_aggregation_type(self, level: int) -> str:
        return {
            0: "overall",
            1: "platform",
            2: "category",
            3: "product"
        }.get(level, "unknown")
    
    async def _count_platforms(self) -> int:
        sql = "SELECT COUNT(*) FROM dwh.dim_platform"
        return await self.db.fetchval(sql)
    
    async def _count_categories(self, platform_code: str) -> int:
        sql = """
            SELECT COUNT(DISTINCT f.category_sk)
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
            WHERE dpl.platform_code = $1
        """
        return await self.db.fetchval(sql, platform_code)
    
    async def _count_products(self, platform_code: str, category_key: str) -> int:
        sql = """
            SELECT COUNT(DISTINCT f.product_sk)
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_platform dpl ON f.platform_sk = dpl.platform_sk
            JOIN dwh.dim_category dc ON f.category_sk = dc.category_sk
            WHERE dpl.platform_code = $1 AND dc.category_sk = $2
        """
        return await self.db.fetchval(sql, platform_code, int(category_key))
    
    async def _get_platform_name(self, platform_code: str) -> str:
        sql = "SELECT platform_name FROM dwh.dim_platform WHERE platform_code = $1"
        row = await self.db.fetchrow(sql, platform_code)
        return row['platform_name'] if row else platform_code
    
    async def _get_category_name(self, category_key: str) -> str:
        sql = """
            SELECT category_lvl1 || ' > ' || COALESCE(category_lvl2, '') AS name
            FROM dwh.dim_category WHERE category_sk = $1
        """
        row = await self.db.fetchrow(sql, int(category_key))
        return row['name'] if row else "Unknown"
```

#### [ ] 8.3. Create Drill-down Router
```python
# backend/app/api/v1/drilldown.py

from fastapi import APIRouter, Depends, Query
from app.services.drilldown_service import DrilldownService
from app.schemas.analytics import DrilldownResponse

drilldown_router = APIRouter(prefix="/analytics/drilldown", tags=["Analytics Drill-down"])

@drilldown_router.get("", response_model=DrilldownResponse)
async def get_drilldown(
    from_date: date = Query(...),
    to_date: date = Query(...),
    level: int = Query(0, ge=0, le=3, description="0=Overall, 1=Platform, 2=Category, 3=Product"),
    platform_code: Optional[str] = Query(None, description="Required for level 2+"),
    category_key: Optional[str] = Query(None, description="Required for level 3"),
    service: DrilldownService = Depends(get_drilldown_service),
):
    """
    Hierarchical drill-down analysis
    
    Examples:
    1. Get overall summary:
       GET /drilldown?from_date=2025-01-01&to_date=2025-01-31&level=0
    
    2. Drill into platforms:
       GET /drilldown?from_date=2025-01-01&to_date=2025-01-31&level=1
    
    3. Drill into Tiki categories:
       GET /drilldown?from_date=2025-01-01&to_date=2025-01-31&level=2&platform_code=tiki
    
    4. Drill into Tiki > Category 1 products:
       GET /drilldown?from_date=2025-01-01&to_date=2025-01-31&level=3&platform_code=tiki&category_key=1
    """
    return await service.get_drilldown(from_date, to_date, level, platform_code, category_key)
```

#### [ ] 8.4. Add to main.py
```python
from api.v1.drilldown import drilldown_router
app.include_router(drilldown_router, prefix=f"{settings.API_V1_PREFIX}")
```

#### [ ] 8.5. Test Drill-down API
```bash
# Level 0: Overall
curl "http://localhost:8000/api/v1/analytics/drilldown?from_date=2025-01-01&to_date=2025-01-31&level=0"

# Level 1: Platforms
curl "http://localhost:8000/api/v1/analytics/drilldown?from_date=2025-01-01&to_date=2025-01-31&level=1"

# Level 2: Tiki Categories
curl "http://localhost:8000/api/v1/analytics/drilldown?from_date=2025-01-01&to_date=2025-01-31&level=2&platform_code=tiki"

# Level 3: Tiki > Electronics > Products
curl "http://localhost:8000/api/v1/analytics/drilldown?from_date=2025-01-01&to_date=2025-01-31&level=3&platform_code=tiki&category_key=1"
```

---

## ✅ TESTING CHECKLIST

### Unit Tests

#### [ ] Test Export Functionality
```python
# tests/test_analytics_export.py

import pytest
from datetime import date
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_export_excel_success():
    """Test Excel export returns valid file"""
    response = client.get(
        "/api/v1/analytics/export/overview/excel",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31"
        }
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert len(response.content) > 0
    
    # Save and validate Excel file
    import openpyxl
    from io import BytesIO
    wb = openpyxl.load_workbook(BytesIO(response.content))
    assert "KPIs" in wb.sheetnames
    assert "Trends" in wb.sheetnames
    assert "Platforms" in wb.sheetnames

def test_export_pdf_success():
    """Test PDF export returns valid file"""
    response = client.get(
        "/api/v1/analytics/export/overview/pdf",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31"
        }
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert response.content[:4] == b'%PDF'  # PDF magic number

def test_export_with_filters():
    """Test export with platform and category filters"""
    response = client.get(
        "/api/v1/analytics/export/overview/excel",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31",
            "platform_code": "tiki",
            "category_key": "1"
        }
    )
    assert response.status_code == 200
```

#### [ ] Test Period Comparison
```python
# tests/test_period_comparison.py

def test_period_comparison_calculation():
    """Test period-over-period calculation"""
    from app.services.analytics_service import AnalyticsService
    
    service = AnalyticsService(db)
    result = await service.get_overview_kpis(
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 31)
    )
    
    # Should have previous period data
    assert result.total_revenue_previous is not None
    assert result.total_revenue_change_pct is not None
    assert result.total_revenue_trend in ["up", "down", "stable"]
    
    # Validate calculation
    if result.total_revenue_previous and result.total_revenue_previous > 0:
        expected_change = (
            (result.total_revenue - result.total_revenue_previous) 
            / result.total_revenue_previous 
            * 100
        )
        assert abs(result.total_revenue_change_pct - expected_change) < 0.01

def test_trend_label_logic():
    """Test trend determination (up/down/stable)"""
    # +10% should be "up"
    # -10% should be "down"
    # +3% should be "stable"
    pass
```

#### [ ] Test Data Quality Integration
```python
# tests/test_data_quality.py

def test_data_quality_warning_present():
    """Test data quality warning is included in response"""
    response = client.get(
        "/api/v1/analytics/overview/kpis",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31"
        }
    )
    assert response.status_code == 200
    data = response.json()
    
    assert "data_quality" in data
    assert "has_issues" in data["data_quality"]

def test_data_quality_severity_levels():
    """Test different severity levels"""
    # Mock data with issues
    # Verify severity is calculated correctly
    pass
```

#### [ ] Test Alert System
```python
# tests/test_alerts.py

def test_get_active_alerts():
    """Test getting active alerts"""
    response = client.get("/api/v1/analytics/alerts/active")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

def test_alert_subscription():
    """Test creating alert subscription"""
    response = client.post(
        "/api/v1/analytics/alerts/subscribe",
        json={
            "alert_type": "revenue_drop",
            "filters": {"platform_code": "tiki"},
            "notification_channel": "email"
        },
        headers={"Authorization": "Bearer test_token"}
    )
    assert response.status_code == 200
    assert response.json()["subscription_id"] is not None

def test_anomaly_detection():
    """Test anomaly detection logic"""
    from app.services.alert_service import AlertService
    service = AlertService(db)
    
    # Mock: Revenue drops 30% suddenly
    # Should create HIGH severity alert
    await service.detect_anomalies()
    
    alerts = await service.get_active_alerts()
    assert any(a.alert_type == "revenue_drop" for a in alerts)
```

#### [ ] Test Advanced Filtering
```python
# tests/test_advanced_filtering.py

def test_multiple_platforms_filter():
    """Test filtering by multiple platforms"""
    response = client.get(
        "/api/v1/analytics/products/top",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31",
            "platform_codes": "tiki,lazada"
        }
    )
    assert response.status_code == 200
    products = response.json()
    
    # All products should be from tiki or lazada
    assert all(p["platform_code"] in ["tiki", "lazada"] for p in products)

def test_price_range_filter():
    """Test price range filtering"""
    response = client.get(
        "/api/v1/analytics/products/top",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31",
            "price_min": 1000000,
            "price_max": 10000000
        }
    )
    assert response.status_code == 200
    products = response.json()
    
    # All products should be within price range
    assert all(1000000 <= p["avg_price"] <= 10000000 for p in products)
```

#### [ ] Test Pagination
```python
# tests/test_pagination.py

def test_pagination_first_page():
    """Test first page pagination"""
    response = client.get(
        "/api/v1/analytics/products/top",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31",
            "page": 1,
            "page_size": 20
        }
    )
    assert response.status_code == 200
    data = response.json()
    
    assert len(data["items"]) <= 20
    assert data["pagination"]["current_page"] == 1
    assert data["pagination"]["has_previous"] == False

def test_pagination_consistency():
    """Test pagination returns consistent results"""
    # Get page 1 and page 2
    page1 = client.get("...?page=1&page_size=10").json()
    page2 = client.get("...?page=2&page_size=10").json()
    
    # Items should not overlap
    page1_keys = [p["product_key"] for p in page1["items"]]
    page2_keys = [p["product_key"] for p in page2["items"]]
    assert len(set(page1_keys) & set(page2_keys)) == 0
```

### Integration Tests

#### [ ] Test Full Export Workflow
```python
def test_full_export_workflow():
    """Test complete export workflow from API to file"""
    # 1. Request export
    response = client.get("/api/v1/analytics/export/overview/excel?...")
    
    # 2. Save file
    with open("test_report.xlsx", "wb") as f:
        f.write(response.content)
    
    # 3. Verify file can be opened
    import openpyxl
    wb = openpyxl.load_workbook("test_report.xlsx")
    
    # 4. Verify data completeness
    ws = wb["KPIs"]
    assert ws["A1"].value == "Metric"
    assert ws["B1"].value == "Value"
    
    # Cleanup
    os.remove("test_report.xlsx")
```

#### [ ] Test Alert Notification Flow
```python
@pytest.mark.asyncio
async def test_alert_notification_flow():
    """Test end-to-end alert flow"""
    # 1. Create subscription
    # 2. Trigger anomaly
    # 3. Verify alert is created
    # 4. Verify notification is sent (mock)
    # 5. Acknowledge alert
    # 6. Verify status updated
    pass
```

#### [ ] Test Data Quality Warning Display
```python
def test_data_quality_affects_all_endpoints():
    """Test data quality warning shows in all analytics endpoints"""
    endpoints = [
        "/api/v1/analytics/overview/kpis",
        "/api/v1/analytics/platforms/comparison",
        "/api/v1/analytics/products/top",
    ]
    
    for endpoint in endpoints:
        response = client.get(f"{endpoint}?from_date=2025-01-01&to_date=2025-01-31")
        data = response.json()
        
        # All should have data_quality field
        assert "data_quality" in data or "data_quality" in data.get("kpis", {})
```

#### [ ] Test Filter Combinations
```python
def test_complex_filter_combination():
    """Test multiple filters working together"""
    response = client.get(
        "/api/v1/analytics/products/top",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31",
            "platform_codes": "tiki,lazada",
            "category_keys": "1,2",
            "price_min": 1000000,
            "price_max": 10000000,
            "rating_min": 4.0,
            "has_reviews": True,
            "page": 1,
            "page_size": 20
        }
    )
    
    assert response.status_code == 200
    products = response.json()["items"]
    
    # Verify all filters applied
    for product in products:
        assert product["platform_code"] in ["tiki", "lazada"]
        assert product["category_key"] in ["1", "2"]
        assert 1000000 <= product["avg_price"] <= 10000000
        assert product["avg_rating"] >= 4.0
        assert product["total_reviews"] > 0
```

### Performance Tests

#### [ ] Benchmark Export Speed
```python
import time

def test_export_performance():
    """Export should complete in <3 seconds for 1 month data"""
    start = time.time()
    
    response = client.get(
        "/api/v1/analytics/export/overview/excel",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31"
        }
    )
    
    duration = time.time() - start
    
    assert response.status_code == 200
    assert duration < 3.0, f"Export took {duration}s, expected <3s"
```

#### [ ] Benchmark Alert Detection
```python
@pytest.mark.performance
async def test_alert_detection_performance():
    """Alert detection should complete in <5 seconds"""
    service = AlertService(db)
    
    start = time.time()
    await service.detect_anomalies()
    duration = time.time() - start
    
    assert duration < 5.0, f"Alert detection took {duration}s, expected <5s"
```

#### [ ] Benchmark Advanced Filtering
```python
def test_complex_query_performance():
    """Complex filtered queries should complete in <2 seconds"""
    start = time.time()
    
    response = client.get(
        "/api/v1/analytics/products/top",
        params={
            "from_date": "2025-01-01",
            "to_date": "2025-01-31",
            "platform_codes": "tiki,lazada",
            "category_keys": "1,2,3",
            "price_min": 1000000,
            "price_max": 10000000,
            "rating_min": 4.0
        }
    )
    
    duration = time.time() - start
    
    assert response.status_code == 200
    assert duration < 2.0, f"Query took {duration}s, expected <2s"
```

### Load Tests

#### [ ] Load Test Export API
```bash
# Use Apache Bench or Locust
ab -n 100 -c 10 "http://localhost:8000/api/v1/analytics/export/overview/excel?from_date=2025-01-01&to_date=2025-01-31"

# Expected:
# - 95th percentile: <5s
# - No errors
# - Consistent response times
```

#### [ ] Load Test Analytics APIs
```python
# tests/load/test_analytics_load.py

from locust import HttpUser, task, between

class AnalystUser(HttpUser):
    wait_time = between(1, 3)
    
    @task(3)
    def get_overview(self):
        self.client.get(
            "/api/v1/analytics/overview/kpis",
            params={
                "from_date": "2025-01-01",
                "to_date": "2025-01-31"
            }
        )
    
    @task(2)
    def get_platform_comparison(self):
        self.client.get(
            "/api/v1/analytics/platforms/comparison",
            params={
                "from_date": "2025-01-01",
                "to_date": "2025-01-31"
            }
        )
    
    @task(1)
    def get_top_products(self):
        self.client.get(
            "/api/v1/analytics/products/top",
            params={
                "from_date": "2025-01-01",
                "to_date": "2025-01-31",
                "metric": "revenue",
                "limit": 20
            }
        )

# Run: locust -f tests/load/test_analytics_load.py
# Target: 50 concurrent users, <2s average response time
```

---

## 📊 SUCCESS METRICS

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Overall API Score | 71/100 | 95/100 | ⏳ In Progress |
| Export Support | 0/10 | 10/10 | ⏳ Phase 1 |
| Period Comparison | 5/10 | 10/10 | ⏳ Phase 1 |
| Data Quality | 6/10 | 10/10 | ⏳ Phase 1 |
| Alert System | 0/10 | 9/10 | ⏳ Phase 2 |
| Advanced Filtering | 8/10 | 10/10 | ⏳ Phase 2 |
| Pagination | 7/10 | 10/10 | ⏳ Phase 2 |

---

## 🚀 DEPLOYMENT CONSIDERATIONS

### Database Optimization

#### [ ] Add Indexes for New Queries
```sql
-- For period comparison queries
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_date_range 
ON dwh.fact_product_daily(snapshot_date, platform_sk, category_sk);

-- For benchmark queries
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_category_price 
ON dwh.fact_product_daily(category_sk, current_price) 
WHERE current_price IS NOT NULL;

-- For alert detection queries
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_recent 
ON dwh.fact_product_daily(snapshot_date DESC, platform_sk, product_sk);

-- For drill-down queries
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_hierarchy 
ON dwh.fact_product_daily(platform_sk, category_sk, product_sk, snapshot_date);
```

#### [ ] Add Materialized Views for Common Aggregations
```sql
-- Daily aggregated metrics (refresh nightly)
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_daily_platform_metrics AS
SELECT
    snapshot_date,
    platform_sk,
    SUM(current_price * total_reviews) AS total_revenue,
    COUNT(DISTINCT product_sk) AS total_products,
    SUM(total_reviews) AS total_reviews,
    AVG(current_price) AS avg_price,
    AVG(avg_rating) AS avg_rating
FROM dwh.fact_product_daily
GROUP BY snapshot_date, platform_sk;

CREATE INDEX ON dwh.mv_daily_platform_metrics(snapshot_date, platform_sk);

-- Refresh schedule
-- Add to cron or Airflow: REFRESH MATERIALIZED VIEW dwh.mv_daily_platform_metrics;
```

### Caching Strategy

#### [ ] Implement Redis Caching
```python
# backend/app/services/cached_analytics_service.py

import redis
import json
import hashlib
from datetime import timedelta

class CachedAnalyticsService(AnalyticsService):
    """Analytics service with Redis caching"""
    
    def __init__(self, db):
        super().__init__(db)
        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )
        self.cache_ttl = 3600  # 1 hour
    
    def _make_cache_key(self, method: str, **kwargs) -> str:
        """Generate cache key from method and parameters"""
        params_str = json.dumps(kwargs, sort_keys=True, default=str)
        hash_str = hashlib.md5(params_str.encode()).hexdigest()
        return f"analytics:{method}:{hash_str}"
    
    async def get_overview_kpis(self, **kwargs):
        """Get KPIs with caching"""
        cache_key = self._make_cache_key("overview_kpis", **kwargs)
        
        # Try cache first
        cached = self.redis.get(cache_key)
        if cached:
            return OverviewKPIResponse(**json.loads(cached))
        
        # Cache miss: compute
        result = await super().get_overview_kpis(**kwargs)
        
        # Store in cache
        self.redis.setex(
            cache_key,
            self.cache_ttl,
            result.model_dump_json()
        )
        
        return result
    
    async def invalidate_cache(self, pattern: str = "*"):
        """Invalidate cache entries matching pattern"""
        keys = self.redis.keys(f"analytics:{pattern}")
        if keys:
            self.redis.delete(*keys)
```

#### [ ] Cache Invalidation Strategy
```python
# Invalidate cache when new data arrives
# backend/airflow/dags/etl_dag.py (at end of ETL)

from app.services.cached_analytics_service import CachedAnalyticsService

def invalidate_analytics_cache():
    """Invalidate cache after ETL completes"""
    redis_client = redis.Redis(...)
    
    # Invalidate all analytics cache
    keys = redis_client.keys("analytics:*")
    if keys:
        redis_client.delete(*keys)
    
    logging.info(f"Invalidated {len(keys)} cache entries")

# Add as last task in ETL DAG
invalidate_cache_task = PythonOperator(
    task_id="invalidate_analytics_cache",
    python_callable=invalidate_analytics_cache,
    dag=dag
)
```

### API Rate Limiting

#### [ ] Implement Rate Limiting
```python
# backend/app/middleware/rate_limit.py

from fastapi import Request, HTTPException
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)

# Apply to expensive endpoints
@router.get("/export/overview/excel")
@limiter.limit("5/minute")  # Max 5 exports per minute per IP
async def export_overview_excel(request: Request, ...):
    pass

@router.get("/products/top")
@limiter.limit("30/minute")  # Max 30 queries per minute
async def get_top_products(request: Request, ...):
    pass
```

### Monitoring & Alerting

#### [ ] Add Prometheus Metrics
```python
# backend/app/monitoring/metrics.py

from prometheus_client import Counter, Histogram, Gauge

# Request metrics
analytics_requests_total = Counter(
    'analytics_requests_total',
    'Total analytics API requests',
    ['endpoint', 'status']
)

analytics_request_duration = Histogram(
    'analytics_request_duration_seconds',
    'Analytics API request duration',
    ['endpoint']
)

# Export metrics
export_requests_total = Counter(
    'export_requests_total',
    'Total export requests',
    ['format', 'status']
)

export_file_size_bytes = Histogram(
    'export_file_size_bytes',
    'Export file size in bytes',
    ['format']
)

# Cache metrics
cache_hits_total = Counter('cache_hits_total', 'Cache hits')
cache_misses_total = Counter('cache_misses_total', 'Cache misses')

# Data quality metrics
data_quality_issues_gauge = Gauge(
    'data_quality_issues',
    'Current data quality issues',
    ['severity']
)
```

#### [ ] Configure Grafana Dashboards
```yaml
# monitoring/grafana/analytics_dashboard.json
{
  "dashboard": {
    "title": "Analytics API Monitoring",
    "panels": [
      {
        "title": "Request Rate",
        "targets": [
          "rate(analytics_requests_total[5m])"
        ]
      },
      {
        "title": "P95 Latency",
        "targets": [
          "histogram_quantile(0.95, analytics_request_duration_seconds)"
        ]
      },
      {
        "title": "Cache Hit Rate",
        "targets": [
          "rate(cache_hits_total[5m]) / (rate(cache_hits_total[5m]) + rate(cache_misses_total[5m]))"
        ]
      },
      {
        "title": "Export Volume",
        "targets": [
          "sum(rate(export_requests_total[1h])) by (format)"
        ]
      }
    ]
  }
}
```

### Documentation

#### [ ] Update API Documentation
```python
# Add comprehensive docstrings with examples

@router.get("/export/overview/excel")
async def export_overview_excel(
    from_date: date = Query(..., example="2025-01-01"),
    to_date: date = Query(..., example="2025-01-31"),
    platform_code: Optional[str] = Query(None, example="tiki"),
):
    """
    Export analytics overview report to Excel format
    
    Returns a multi-sheet Excel file with:
    - **KPIs Sheet**: Key metrics (revenue, products, reviews, etc.)
    - **Trends Sheet**: Daily time series data
    - **Platforms Sheet**: Platform comparison
    - **Categories Sheet**: Category breakdown
    
    **Response:**
    - Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
    - File name: analytics_overview_{from_date}_{to_date}.xlsx
    
    **Example:**
    ```bash
    curl "http://api.example.com/api/v1/analytics/export/overview/excel?from_date=2025-01-01&to_date=2025-01-31" -o report.xlsx
    ```
    
    **Rate Limit:** 5 exports per minute per IP
    
    **Estimated Time:** 2-5 seconds for 1 month of data
    """
    pass
```

#### [ ] Create Postman Collection
```json
{
  "info": {
    "name": "Analyst APIs v2",
    "description": "Complete collection of Analyst APIs with examples"
  },
  "item": [
    {
      "name": "Export - Overview Excel",
      "request": {
        "method": "GET",
        "url": {
          "raw": "{{base_url}}/api/v1/analytics/export/overview/excel?from_date=2025-01-01&to_date=2025-01-31",
          "host": ["{{base_url}}"],
          "path": ["api", "v1", "analytics", "export", "overview", "excel"],
          "query": [
            {"key": "from_date", "value": "2025-01-01"},
            {"key": "to_date", "value": "2025-01-31"}
          ]
        }
      }
    }
  ]
}
```

---

## 📊 FINAL CHECKLIST SUMMARY

### Phase 1 (Tuần 1-2) - CRITICAL ✅
- [ ] Task 1: Export APIs (Excel/PDF) - 5 subtasks
- [ ] Task 2: Period Comparison - 3 subtasks  
- [ ] Task 3: Data Quality Integration - 5 subtasks

### Phase 2 (Tuần 3-4) - IMPORTANT ✅
- [ ] Task 4: Alert System - 5 subtasks
- [ ] Task 5: Advanced Filtering - 2 subtasks
- [ ] Task 6: Pagination - 4 subtasks

### Phase 3 (Tuần 5-6) - NICE TO HAVE ✅
- [ ] Task 7: Benchmark APIs - 5 subtasks
- [ ] Task 8: Drill-down APIs - 5 subtasks

### Testing & QA ✅
- [ ] Unit Tests - 6 test suites
- [ ] Integration Tests - 4 test scenarios
- [ ] Performance Tests - 3 benchmarks
- [ ] Load Tests - 2 load scenarios

### Deployment ✅
- [ ] Database optimization - indexes & materialized views
- [ ] Caching strategy - Redis implementation
- [ ] Rate limiting - API throttling
- [ ] Monitoring - Prometheus + Grafana
- [ ] Documentation - API docs + Postman

---

## 🎯 SUCCESS CRITERIA

### Functional Requirements
- ✅ All Phase 1-3 features implemented and tested
- ✅ No breaking changes to existing APIs
- ✅ Backward compatible with current frontend
- ✅ 95%+ test coverage for new features

### Performance Requirements
- ✅ Export APIs: <3s for 1 month data
- ✅ Analytics queries: <2s with filters
- ✅ Alert detection: <5s for all checks
- ✅ Cache hit rate: >80% for common queries

### Quality Requirements
- ✅ All tests passing
- ✅ No critical bugs
- ✅ API documentation complete
- ✅ Monitoring dashboards configured

---

**Timeline:** 4-6 tuần  
**Estimated Effort:** ~120-150 hours (updated with testing & deployment)  
**Team Size:** 2 backend developers + 1 QA engineer

---

📅 **Created:** 2025-11-25  
✍️ **Author:** AI Assistant  
📝 **Version:** 2.0 (Complete)

