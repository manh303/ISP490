**Tổng Quan**
- **Mục tiêu:** Tài liệu này giải thích chi tiết các API client hiện có trong `src/services/*` — mục đích mỗi endpoint, dữ liệu trả về (response schema), luồng dữ liệu trong ứng dụng và lý do vì sao các trường dữ liệu được dùng để vẽ các biểu đồ hay bảng trong UI.
- **Các file chính:** `analyticsApi.ts`, `DSSApi.ts`, `machineLearningApi.ts`, `reportApi.ts`, `businessMetadataApi.ts`, `dataEngineerApi.ts`.

-------------------------
**Hướng dẫn đọc nhanh**
- **Endpoint:** đường dẫn được gọi (ví dụ `/v1/analytics/overview/kpis`).
- **Method:** `GET` / `POST`.
- **Params / Body:** những tham số cần truyền (query params hoặc body JSON).
- **Response:** cấu trúc trả về quan trọng (chỉ liệt kê trường dùng bởi UI).
- **UI mapping:** component/biểu đồ sử dụng response và cách ánh xạ trường -> chart.

-------------------------
**1. `analyticsApi.ts` — Analytics / Dashboard**

Mục đích: Cung cấp dữ liệu cho dashboard tổng quan, các biểu đồ phân tích theo platform/category/product và các báo cáo sản phẩm.

- `getPlatforms()`
  - Method: GET `/v1/analytics/filters/platforms`
  - Response: danh sách platform objects `{ platform_code, platform_name }`.
  - UI use: dropdown filter, legend cho biểu đồ so sánh platform.

- `getCategories(params?)`
  - Method: GET `/v1/analytics/filters/categories` (params: `platform_code`, `parent_category_key`)
  - Response: `{ category_key, category_name, level, parent_key, platform_code }`
  - UI use: cây category, filter selector.

- `getProducts(params)`
  - Method: GET `/v1/analytics/filters/products` (params: `q`, `platform_code`, `category_key`, `limit`)
  - Response: `{ product_key, product_name, platform_code, category_key }[]`
  - UI use: autocomplete product search, selection for product report.

- `getOverviewKPIs(params)`
  - Method: GET `/v1/analytics/overview/kpis` (params: `from_date`, `to_date`, optional `platform_code`, `category_key`)
  - Response fields used: `total_revenue`, `total_products`, `total_reviews`, `avg_price`, `avg_rating`.
  - UI mapping: KPI cards (numeric tiles) showing totals/averages.

- `getOverviewTrends(params)`
  - Method: GET `/v1/analytics/overview/trends`
  - Response: `points[]` where each point: `{ date, revenue, total_orders, avg_price, avg_rating, total_reviews }`.
  - UI mapping: line/area charts (x=date):
    - Line for `revenue` over time.
    - Secondary axis for `total_orders` or `avg_rating`.
    - Bars for `total_reviews` (stacked chart or combo chart).
  - Rationale: trends cho thấy thay đổi theo thời gian; date-indexed series là input tự nhiên cho biểu đồ tuyến tính.

- `getPlatformComparison(params)`
  - Method: GET `/v1/analytics/platforms/comparison`
  - Response: array mỗi phần tử có `platform_code`, `platform_name`, `total_revenue`, `total_products`, `avg_price`, `avg_rating`, `total_reviews`.
  - UI mapping: grouped bar chart hoặc horizontal bars (so sánh tổng doanh thu, số sản phẩm, avg_rating giữa các platform).
  - Rationale: so sánh theo platform giúp xác định platform hiệu quả nhất.

- `getCategoryShare(params)`
  - Method: GET `/v1/analytics/platforms/category-share`
  - Response: `category_key, category_name, platform_code, revenue, revenue_share`.
  - UI mapping: pie chart hoặc stacked bar (tỷ lệ doanh thu theo category).

- `getTopProducts(params)`
  - Method: GET `/v1/analytics/products/top`
  - Response: list `TopProduct` (has `total_revenue`, `avg_rating`, `total_reviews`, `avg_price`).
  - UI mapping: table + bar chart for top-N revenue; sparkline for each product if timeseries included.

- `getProductTimeseries(params)`
  - Method: GET `/v1/analytics/product/timeseries`
  - Response: `points[]` each `{ date, avg_price, min_price, max_price, total_reviews, avg_rating, revenue }`.
  - UI mapping: line chart for price history, area between `min_price` and `max_price`, bar for `total_reviews`.
  - Rationale: bật lên biến động giá và tác động đến doanh thu/đánh giá.

- `getProductReviewSummary(params)`
  - Method: GET `/v1/analytics/product/reviews/summary`
  - Response: `total_reviews`, `avg_rating`, `rating_breakdown.by_rating`, `top_helpful_reviews`.
  - UI mapping: donut for rating breakdown, list of reviews.

- `getPriceDistribution(params)`
  - Method: GET `/v1/analytics/pricing/distribution`
  - Response: `{ min_price, p25_price, median_price, p75_price, max_price }` per platform/category.
  - UI mapping: boxplot (min,p25,median,p75,max) or violin; histogram if raw price buckets available.
  - Rationale: Phân phối giá lý giải vị trí trung vị và biến động.

- `getPriceVsRevenue(params)`
  - Method: GET `/v1/analytics/pricing/price-vs-revenue`
  - Response: list `{ product_key, product_name, avg_price, total_revenue, avg_rating, total_reviews }`.
  - UI mapping: scatter plot (x=avg_price, y=total_revenue), point size = total_reviews, color = avg_rating.
  - Rationale: phát hiện mối quan hệ giữa giá trung bình và doanh thu.

- `getOverviewReport`, `getProductReport`
  - Method: GET, trả `OverviewReport` / `ProductReport` (bigger payloads)
  - Response sử dụng để build các trang báo cáo chi tiết.

-------------------------
**2. `DSSApi.ts` — Decision Support System (DSS)**

Mục đích: endpoints phục vụ các tác vụ AI/DSS như dự đoán giá, gợi ý sản phẩm, phân tích sentiment — dùng cho các trang chuyên biệt `DSS` trong app.

- `runPricePredictionDSS(data: PricePredictionRequest)`
  - Method: POST `/v1/dss/price/run`
  - Request: `from_date`, `to_date`, filters (platforms, categories), `scope_mode`, `top_n` hoặc `product_keys`, thresholds như `min_margin_pct`, `min_confidence`.
  - Response: `PricePredictionResponse` gồm `kpi_summary`, `table_data[]`, `total_count`, `ai_summary_insights`, `ai_recommended_actions`, `generated_at`, `ai_model_used`.
  - UI mapping:
    - KPI cards from `kpi_summary` (num_products, projected_revenue, expected_uplift_pct).
    - Table showing `table_data[]` with columns: `product_name`, `current_price`, `predicted_price`, `price_diff`, `confidence`, `projected_revenue`.
    - Charts: histogram of `price_change_pct`, scatter `confidence` vs `price_change_pct`, stacked bar of revenue uplift by category.
  - Rationale: predictions -> đưa ra gợi ý điều chỉnh giá và ước lượng ảnh hưởng doanh thu.

- `runProductRecommendationDSS(data)`
  - Method: POST `/v1/dss/reco/run`
  - Request: `scope_mode` (`by_product`/`by_category`), `source_product_key`, `top_k`, filters.
  - Response: `ProductRecommendationResponse` gồm `kpi_summary`, `table_data[]`, `ai_summary_insights`.
  - UI mapping: recommendation list/cards, similarity score charts (bar), co-purchase network visualization.

- `runReviewSentimentDSS(data)`
  - Method: POST `/v1/dss/review/run`
  - Request: `from_date`, `to_date`, `min_reviews_per_product`, `sentiment_focus`, `negative_threshold`.
  - Response: `ReviewSentimentResponse`:
    - `kpi_summary`: aggregated sentiment KPIs.
    - `table_data[]`: per-product sentiment counts, `avg_rating`, `top_positive_reasons`, `top_negative_reasons`, `is_critical` flag.
    - `ai_summary_insights`, `ai_recommended_actions`.
  - UI mapping: heatmap of negative sentiment by category/platform, bar charts for top negative reasons, sample negative reviews list.
  - Rationale: find sản phẩm cần hành động (critical negative), support triage.

- `getProductReviewDetails(params)`
  - Method: GET `/v1/dss/review/{product_key}/details`
  - Response: `reviews[]` với `review_id`, `rating`, `sentiment_label`, `sentiment_score`, `review_body`, `helpful_votes`, `review_date`.
  - UI mapping: paginated review list, sentiment distribution histogram, helpful vote sorting.

- DSS Decisions endpoints (`saveDSSDecision`, `listDSSDecisions`, `getDSSDecisionDetail`)
  - Purpose: lưu các quyết định (actions) do user chọn ra dựa trên DSS results; dùng cho hồ sơ ra quyết định (audit trail).
  - UI: forms to create/save decision, list view, detail view with action items and status.

-------------------------
**3. `machineLearningApi.ts` — ML models & online inference**

Mục đích: quản lý model, truy vấn lịch sử predictions và thực hiện inferencing online.

- `listModels(params?)`, `createModel`, `getModel`, `updateModel`
  - CRUD model metadata; response `MLModel` có `model_name`, `model_type`, `model_version`, `training_data_until`, `metrics`, `status`.
  - UI mapping: models table, status badges, metrics panel.

- `getPricePredictionHistory(params)`
  - Method: GET `/v1/ml/price-predictions/history`
  - Response: `PricePredictionHistory` với `points[]` chứa `date`, `predicted_price`, `ci_lower`, `ci_upper`, `run_id`.
  - UI mapping: ribbon plot or line with confidence interval (ci_lower/ci_upper displayed as shaded area), useful for model performance visualization.

- `onlinePricePrediction(data)`
  - Method: POST `/v1/ml/price-predictions/online`
  - Response: `predicted_price`, `ci_lower`, `ci_upper`, `latency_ms`.
  - UI mapping: one-off prediction card (used in product quick-preview hoặc admin tool for testing model outputs).

- `getRecommendations(params)`
  - Method: GET `/v1/ml/recommendations`
  - Response: `Recommendations` object with `recommendations[]` each `{ rank, recommended_product_key, similarity_score }`.
  - UI mapping: recommendation carousel or list.

- `getSentimentSummary` & `onlineSentiment`
  - `getSentimentSummary`: time-series summarizing sentiment (used to draw stacked area of positive/neutral/negative ratios).
  - `onlineSentiment`: single text scored by sentiment model, used in review moderation UI.

- `getStatusSummary()`
  - Method: GET `/v1/ml/status/summary`
  - Response: aggregate counts such as `models_total`, `models_active`, `predictions_last_7_days`.
  - UI mapping: small KPI tiles used on ML status dashboard.

-------------------------
**4. `reportApi.ts` — Export / Reports**

Mục đích: trả về file báo cáo (binary/Blob, e.g., Excel/PDF).

- Endpoints: `exportOverviewReport`, `exportProductsReport`, `exportReviewsReport`, `exportReviewsDetailsReport`, `exportProductReviewsDetails`, `exportProductsByCategory`, `exportProductsByCategoryAllPlatforms`.
  - Method: GET, `responseType: 'blob'`
  - UI mapping: Download button in report pages — server trả file đã format sẵn.

-------------------------
**5. `businessMetadataApi.ts` — Metadata / Data Catalog**

Mục đích: danh mục nguồn dữ liệu, dataset, schema, business glossary, expectations (data quality rules).

- Key endpoints: `getAllSourceSystems`, `getSourceSystemDetails`, `getAllDatasets`, `getDatasetDetails`, `searchDataCatalog`, `getAllSchemas`, `getTablesInSchema`, `getAllBusinessTerms`, `createBusinessTerm`, `getBusinessTermDetail`, `getAllExpectations`, `createExpectation`, `getExpectationResults`, `getAllJobs`, `getJobDetails`.
  - Response objects: `SourceSystem`, `DatasetDetail`, `Schema`, `TableInSchema`, `BusinessTerm`, `Expectation`, `ExpectationResult`, `Job`.
  - UI mapping: data catalog pages, search, dataset detail pages, data quality dashboards showing expectation results and frequencies.
  - Rationale: metadata giúp truy vết nguồn dữ liệu, kiểm tra PII, retention, và liên kết đến dashboard/biểu đồ khi cần biết dữ liệu nào tạo ra metric.

-------------------------
**6. `dataEngineerApi.ts` — Data engineering / ETL / Quality**

Mục đích: cung cấp health checks, ETL jobs list/history, table health, data-quality issues và pipeline stats.

- Key endpoints & mapping:
  - `getHealth()` -> overall health indicator (badge)
  - `getETLJobs()` -> table of ETL jobs với last run status
  - `getETLRunHistory(jobCode)` -> run timeline (use để vẽ Gantt/sparkline of statuses)
  - `getETLRunLogs(runId)` -> raw logs (text viewer)
  - `getTableHealth(schemaName?, staleHours)` -> table list with `row_count`, `size_mb`, `freshness_hours` (used to color-code stale tables)
  - `getDataQualityIssues()` -> table and summary charts (issue counts by severity/type)
  - `getDataQualitySummary()` -> KPI tiles; stacked bars by severity
  - `getDatabaseHealth()` -> DB KPIs (active connections, avg_query_time) shown as small tiles/alerts
  - `getTableLineage(schema, table)` -> draws lineage graph (nodes/edges)
  - `getAlertSummary()` & `getAlertHistory()` -> alert dashboard, histogram of triggers over time
  - `getPipelinePerformanceStats(days)` -> line charts of throughput/latency per pipeline
  - `getDataVolumeTrends(days)` -> area/line chart showing data volume growth

-------------------------
**Cách ánh xạ dữ liệu -> biểu đồ (quick reference)**
- Time series (x = date): dùng `points[].date` hoặc `generated_at`. Thông dụng: `revenue`, `total_orders`, `avg_price`, `avg_rating`, `total_reviews`.
- KPI cards: numeric summary fields như `total_revenue`, `num_products`, `avg_rating`, `num_with_recommendation`.
- Boxplot / distribution: API trả `min`, `p25`, `median`, `p75`, `max` → vẽ boxplot.
- Scatter: two numeric axes, ví dụ `avg_price` (x) vs `total_revenue` (y); thêm `total_reviews` -> size.
- Pie/Share: `revenue_share` hoặc `%` fields → pie or stacked bars.
- Confidence intervals: `ci_lower` / `ci_upper` -> shaded area around predicted line.

-------------------------
**Sample minimal response snippets**
- OverviewTrends (example point):
```json
{ "date": "2025-11-01", "revenue": 12345.6, "total_orders": 456, "avg_price": 27.1, "avg_rating": 4.2, "total_reviews": 120 }
```
- PricePrediction `table_data` item (example):
```json
{ "product_key":"p-123","product_name":"Sản phẩm A","platform":"PLT","category_name":"Electronics","current_price":100.0,"predicted_price":90.0,"price_diff":-10.0,"price_change_pct":-10.0,"projected_revenue":11000.0,"confidence":0.93 }
```

-------------------------
**Cheat-sheet: câu hỏi giáo viên có thể hỏi & cách trả lời ngắn**
- Q: "API trả về `total_revenue` được tính như thế nào?"
  - A: `total_revenue` là tổng doanh thu trong khoảng `from_date`→`to_date` tùy filter (platform/category) — server chịu trách nhiệm tính tổng bằng aggregations trên fact table.
- Q: "Tại sao dùng median thay vì mean trong phân phối giá?"
  - A: Median bền hơn nhiễu/outlier; phản ánh điểm giữa phân phối giá khi có outliers.
- Q: "Confidence trong price prediction có ý nghĩa gì?"
  - A: Giá trị xác suất/độ tin cậy do model trả về (0-1) — dùng để lọc khuyến nghị chỉ khi `confidence` cao.
- Q: "Tại sao vẽ scatter `price vs revenue`?"
  - A: Để kiểm tra mối quan hệ giữa giá trung bình và doanh thu; có thể cho thấy sản phẩm đắt hơn không nhất thiết doanh thu cao hơn.
- Q: "Nếu `is_critical` trong sentiment = true thì phải làm gì?"
  - A: Prioritize investigation: đọc `sample_negative_reviews`, kiểm tra category/platform, tạo DSS Decision/Action.

-------------------------
