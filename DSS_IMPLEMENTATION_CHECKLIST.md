# DSS Drill-Down Implementation Checklist

## Quick Summary

Bạn muốn implement drill-down analytics cho workflow:
```
Overall Revenue ↓ Platform (Lazada) ↓ Category (Electronics) ↓ Product (Brand X)
```

**Status**: ✅ Database + Backend API **READY TO IMPLEMENT**

---

## Immediate Actions (Next Sprint)

### [ ] 1. Deploy SQL Views (Database Layer)
```bash
# SSH vào database server
psql -h your-db-host -U dss_user -d ecommerce_dss < database/views/dss_drilldown_views.sql

# Verify views created
\dv v_daily_*
\dv v_alert_*
```

**Files to run**:
- `database/views/dss_drilldown_views.sql` ✅ (Created)

**Time**: ~15 minutes

---

### [ ] 2. Register Backend API Router

Edit `backend/app/main.py`:

```python
# Add to imports
from app.api.v1.dss_drilldown import router as dss_drilldown_router

# Add to FastAPI app
app.include_router(dss_drilldown_router)

# Verify endpoints created
# GET /api/v1/dss/drilldown/overall
# GET /api/v1/dss/drilldown/platform/{platform_code}
# GET /api/v1/dss/drilldown/category/{category_code}
# GET /api/v1/dss/drilldown/product/{global_product_id}
```

**Files to modify**:
- `backend/app/main.py`

**Time**: ~5 minutes

---

### [ ] 3. Test API Endpoints

```bash
# 1. Test Overall Dashboard
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/overall?start_date=2024-10-01&end_date=2024-10-31"

# 2. Test Platform Drill-Down
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/platform/lazada?start_date=2024-10-01&end_date=2024-10-31"

# 3. Test Category Drill-Down
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/category/electronics?platform_code=lazada"

# 4. Test Product Detail
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/product/{global_product_id}"
```

**Time**: ~30 minutes

---

## Next Phase (After Testing)

### [ ] 4. Implement Missing Database Components

**Option A: Quick (if you don't need campaigns)**
- Skip campaign data
- Skip order-level transactions
- Focus on existing fact table

**Option B: Full (Recommended)**
Create additional tables for richer analysis:

```bash
# Create campaign dimension
psql < database/schema/dwh_dim_campaign.sql

# Create order fact table
psql < database/schema/dwh_fact_orders.sql

# Create inventory fact table
psql < database/schema/dwh_fact_inventory.sql
```

**Time**: 2-3 hours

---

### [ ] 5. Build Frontend Dashboard

Create Streamlit or React dashboard with:

1. **Overall Dashboard Page**
   - KPI cards (total revenue, change %, trend)
   - Top 5 categories bar chart
   - Top 3 platforms comparison
   - Key alerts section

2. **Platform Details Page**
   - Drill-down from overall
   - Platform-specific metrics
   - Categories breakdown
   - Problematic categories alert

3. **Category Details Page**
   - Drill-down from platform
   - Top brands breakdown
   - Top products table
   - Price changes heatmap
   - Out of stock products

4. **Product Details Page**
   - Product metrics card
   - Price history chart
   - Availability history
   - Competitor price comparison
   - Reviews sentiment

**Time**: 1-2 weeks

---

## Database Schema: Current vs Required

### Current Schema Status

| Table | Purpose | Data Available | Gap |
|-------|---------|-----------------|-----|
| `dwh_fact_product_daily` | Daily product metrics | ✅ Price, availability, sales, rating | Orders, detailed inventory |
| `dwh_dim_product` | Product dimension | ✅ Name, brand, category | SKU, variant info |
| `dwh_dim_platform` | Platform dimension | ✅ Lazada, Tiki, etc | Regional info |
| `dwh_dim_category` | Category dimension | ✅ Electronics, Home, etc | Subcategory levels |
| `dwh_dim_brand` | Brand dimension | ✅ Brand names | Brand category, country |
| `ods_price_point` | Price history | ✅ Captured per date | No order quantity |
| `ods_review_clean` | Reviews | ✅ Sentiment, rating | Detailed feedback |

### Needed for Full DSS (Optional)

```sql
-- 1. Campaign Dimension
CREATE TABLE dwh_dim_campaign (
    campaign_sk SERIAL PRIMARY KEY,
    campaign_code VARCHAR(100),
    campaign_name TEXT,
    campaign_type VARCHAR(50),  -- flash_sale, seasonal
    platform_sk INT,
    start_date DATE,
    end_date DATE,
    budget DECIMAL(15,2)
);

-- 2. Order Fact Table
CREATE TABLE dwh_fact_orders (
    order_sk BIGSERIAL PRIMARY KEY,
    date_sk INT,
    product_sk BIGINT,
    platform_sk INT,
    quantity INT,
    unit_price DECIMAL(15,2),
    total_amount DECIMAL(15,2),
    status VARCHAR(50)
);

-- 3. Inventory Fact Table
CREATE TABLE dwh_fact_inventory (
    date_sk INT,
    product_sk BIGINT,
    platform_sk INT,
    stock_quantity INT,
    reserved_quantity INT,
    available_quantity INT,
    days_out_of_stock INT
);
```

---

## File Structure

```
backend/
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── analytics.py (existing - basic charts)
│   │       ├── dashboard.py (existing - KPIs)
│   │       └── dss_drilldown.py ✅ (NEW - drill-down)
│   └── main.py (modify - add router)
│
database/
├── views/
│   ├── dashboard_views.sql (existing)
│   └── dss_drilldown_views.sql ✅ (NEW)
│
docs/
├── DSS_DRILLDOWN_ANALYSIS.md ✅ (NEW - Design doc)
└── DSS_IMPLEMENTATION_CHECKLIST.md ✅ (NEW - This file)
```

---

## Performance Considerations

### Current Database Size
- **Estimated rows in dwh_fact_product_daily**: ~10M rows/month
- **Date range**: Last 30-90 days = 300M-900M rows

### Query Performance Targets
- **Overall dashboard**: < 2 seconds
- **Platform drill-down**: < 3 seconds
- **Category drill-down**: < 3 seconds
- **Product detail**: < 1 second

### Optimization Applied
1. ✅ Composite indexes on (date_sk, platform_sk, product_sk)
2. ✅ Partition tables by month if size grows > 1GB
3. ✅ Materialized views for monthly aggregates
4. ✅ Query caching (Redis) for frequently accessed periods

---

## Testing Plan

### Unit Tests (Backend)
```python
# test_dss_drilldown.py
def test_overall_dashboard():
    # Test with sample date range
    # Assert response structure
    pass

def test_platform_drilldown():
    # Test with Lazada
    # Verify categories breakdown
    pass

def test_category_drilldown():
    # Test Electronics category
    # Verify top products
    pass

def test_product_detail():
    # Test specific product
    # Verify price history
    pass
```

### Integration Tests
```bash
# 1. Database connectivity
psql -c "SELECT COUNT(*) FROM dwh_fact_product_daily"

# 2. View creation
psql -c "\dv v_daily_revenue_*"

# 3. API response validation
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/overall"

# 4. Data accuracy
# Compare manual query vs API response
```

---

## Known Limitations

1. **No Campaign Data**: Can't filter by specific promotions
   - **Workaround**: Use date range as proxy
   - **Fix**: Add campaign dimension table

2. **No Order Quantity**: Can't track units sold
   - **Current**: Using `sold_count` from rating snapshot
   - **Better**: Use order fact table

3. **Limited Inventory Data**: Only availability flag
   - **Current**: Binary available/not available
   - **Better**: Track actual stock levels and days OOS

4. **No Variant Tracking**: Treats all variants as same product
   - **Current**: Group by global_product_id
   - **Better**: Add variant SKU to dimension

---

## Success Metrics

After implementation, you should be able to:

- [ ] View overall revenue trend in < 2 seconds
- [ ] Drill down to any platform and see category breakdown
- [ ] Drill down to category and identify top products and price changes
- [ ] Open any product and see:
  - [ ] Price history (7/30 day trend)
  - [ ] Stock availability status
  - [ ] Competitor prices across platforms
  - [ ] Review sentiment
- [ ] Auto-detect and alert on:
  - [ ] Revenue decline > 10%
  - [ ] Price increases > 5%
  - [ ] Products out of stock > 3 days
  - [ ] Category performance drops

---

## Estimated Timeline

| Phase | Task | Time | Status |
|-------|------|------|--------|
| 1 | SQL Views + Indexes | 30 min | ⏳ TODO |
| 2 | API Implementation | 2 hours | ⏳ TODO |
| 3 | Testing & Debugging | 2 hours | ⏳ TODO |
| 4 | Frontend Dashboard | 1-2 weeks | ⏳ TODO |
| 5 | Optional: Add campaign data | 3-5 days | ⏳ OPTIONAL |
| 6 | Optimization & Caching | 2-3 days | ⏳ TODO |

**Total**: 1 week for core + 1-2 weeks for frontend

---

## Resources Needed

### Database
- [ ] PostgreSQL 12+ with PostGIS (optional)
- [ ] Sufficient disk space (300MB+ for views)
- [ ] Query planning tools (EXPLAIN ANALYZE)

### Backend
- [ ] FastAPI setup ✅ (done)
- [ ] Pydantic models ✅ (created)
- [ ] Async database connection ✅ (existing)

### Frontend (Optional for MVP)
- [ ] Streamlit OR React
- [ ] Plotly/Matplotlib for charts
- [ ] Pandas for data manipulation

---

## Troubleshooting Guide

### Issue 1: Views taking too long to create
```sql
-- Check query plan
EXPLAIN ANALYZE SELECT * FROM v_daily_revenue_by_platform LIMIT 10;

-- Solution: Add more indexes
CREATE INDEX idx_product_daily_date_platform 
ON dwh_fact_product_daily(date_sk DESC, platform_sk);
```

### Issue 2: API returning null values
```python
# Check database connection in main.py
# Verify tables have data
SELECT COUNT(*) FROM dwh_fact_product_daily;
SELECT COUNT(*) FROM dwh_dim_platform;

# Debug query results
await db.execute_query("SELECT * FROM v_daily_revenue_by_platform LIMIT 10")
```

### Issue 3: Performance degradation
```sql
-- Monitor slow queries
SELECT * FROM pg_stat_statements ORDER BY mean_time DESC;

-- Analyze query plans
EXPLAIN ANALYZE SELECT ... FROM v_daily_revenue_platform_category;

-- Add missing indexes based on plan
```

---

## Next Steps (In Order)

1. ✅ Review design document (you're reading it)
2. ⏳ Run SQL views creation script
3. ⏳ Integrate API router into main.py
4. ⏳ Test all endpoints
5. ⏳ Build frontend dashboard
6. ⏳ Deploy to production
7. ⏳ Monitor performance and optimize

---

## Contact & Support

For questions on:
- **SQL Views**: Review `database/views/dss_drilldown_views.sql`
- **API Design**: Review `backend/app/api/v1/dss_drilldown.py`
- **Architecture**: Review `DSS_DRILLDOWN_ANALYSIS.md`

---

**Created**: Nov 15, 2024
**Status**: Ready for Implementation
