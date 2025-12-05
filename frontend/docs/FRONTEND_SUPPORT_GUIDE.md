**Chart Selection Guide — Công thức nhanh**

Mục tiêu: tài liệu này giúp một người hỗ trợ (supporter) nhanh hiểu phần Frontend bạn đã phát triển, biết cách demo, trả lời câu hỏi và chứng minh họ đã tự code (không phải thuê người khác). File tập trung vào: cấu trúc code, các file quan trọng, luồng dữ liệu (API → service → component → chart), checklist demo, mẫu câu trả lời và cách debug nhanh.
**Frontend Support Guide**

Mục tiêu: tài liệu này giúp một người hỗ trợ (supporter) nhanh hiểu phần Frontend bạn đã phát triển, biết cách demo, trả lời câu hỏi và chứng minh họ đã tự code (không phải thuê người khác). File tập trung vào: cấu trúc code, các file quan trọng, luồng dữ liệu (API → service → component → chart), checklist demo, mẫu câu trả lời và cách debug nhanh.

**1) Tóm tắt ngắn (1 phút)**
- Frontend: React + Vite, code ở `src/`.
- API clients: `src/services/*.ts` — đây là contract giữa frontend và backend.
- UI chính: dashboard, reports, DSS pages, ML pages, data-catalog, data-engineer pages.
- Mục tiêu khi support: đọc `docs/API_DOCUMENTATION.md`, mở file service và component liên quan, chạy demo network để minh họa mapping response → UI.

**2) Các file / thư mục quan trọng**
- `src/services/` — client gọi API (analyticsApi.ts, DSSApi.ts, machineLearningApi.ts, reportApi.ts, businessMetadataApi.ts, dataEngineerApi.ts). Mỗi file chứa: axios instance, interceptors, type interfaces, và functions để gọi endpoint.
- `src/components/` — các component UI (charts, tables, filters). Tìm component dùng dữ liệu của API bằng cách search tên function từ `services`.
- `src/layout/` & `src/pages/` — chứa layout và pages; entry point `src/main.tsx`.
- `docs/API_DOCUMENTATION.md` — tài liệu endpoint và mapping (đã tạo).
- `docs/FRONTEND_SUPPORT_GUIDE.md` — file này (hướng dẫn support).

**3) Luồng dữ liệu ngắn gọn**
1. Component (ví dụ `OverviewDashboard`) gọi function ở `src/services/analyticsApi.ts` (ví dụ `getOverviewTrends`).
2. Service trả JSON (response.data) — service thường chỉ forward dữ liệu hoặc thực hiện small transform.
3. Component nhận data, chuyển thành props cho chart component (x,y mapping, options) và render.
4. User tương tác → component cập nhật query params → gọi lại service.

**4) Checklist demo (step-by-step, 3–5 phút demo)**
1. Mở `docs/API_DOCUMENTATION.md` — show endpoint mapping cho page demo (ví dụ `getOverviewTrends`).
2. Mở file service (`src/services/analyticsApi.ts`) — highlight function gọi endpoint.
3. Mở component sử dụng (vd: Dashboard component) — show nơi gọi `getOverviewTrends`, nơi transform data.
4. Mở browser DevTools → Network tab. Refresh page → show request URL, query params và response JSON.
5. Show chart on page and point to which response fields map vào trục/series.
6. Simulate change: change date filter or platform → show new request and updated chart.
7. Simulate error: (disable backend or mock 500) → show error UI (toast/banner/retry).

**Charts used in system — Biểu đồ trong hệ thống (tóm tắt ngắn)**

Dưới đây chỉ liệt kê các kiểu biểu đồ thực tế đang dùng trong frontend của dự án, kèm **API function** tương ứng và component nơi sử dụng.

- **Line (custom SVG) — Review trends**
  - Component: `src/components/analytics/ReviewTrendsChart.tsx`
  - Page: `src/pages/Analyst/ReviewAnalytics.tsx`
  - API: `getOverviewTrends(params)` → `GET /v1/analytics/overview/trends`
  - Mục đích: hiển thị xu hướng đánh giá (`avg_rating`) theo thời gian, kèm KPI nhỏ (avg_rating, total_reviews, total_orders).

- **Timeseries (Recharts LineChart, multi-series)**
  - Component: `src/components/analytics/ProductTimeseriesChart.tsx`
  - Page: `src/pages/Analyst/ProductAnalytics.tsx` (và product detail)
  - API: `getProductTimeseries(params)` → `GET /v1/analytics/products/{product_key}/timeseries`
  - Mục đích: hiển thị nhiều chỉ số theo thời gian (Doanh thu, Giá TB, Rating TB, Tổng đánh giá) với trục Y đôi.

- **Horizontal progress bars / Ranked list**
  - Component: `src/components/analytics/TopRatedProductsChart.tsx`
  - Page: `src/pages/Analyst/AnalyticsDashboard.tsx`
  - API: `getTopProducts(params)` → `GET /v1/analytics/products/top`
  - Mục đích: hiển thị top sản phẩm theo chỉ số (avg_rating / total_reviews) dưới dạng thanh ngang (progress-like) để dễ đọc ranking.

- **Category performance (compact bars + rating bars)**
  - Component: `src/components/analytics/CategoryPerformanceChart.tsx`
  - Page: `src/pages/Analyst/AnalyticsDashboard.tsx` (dữ liệu từ `getOverviewReport`)
  - API: `getCategoryShare(params)` → `GET /v1/analytics/platforms/category-share`  (hoặc `getOverviewReport()` → `category_share`)
  - Mục đích: so sánh hiệu suất theo danh mục (số sản phẩm, rating, reviews) — dạng bar list, dễ scan.

- **Platform comparison (grouped bars / progress bars)**
  - Component: `src/components/analytics/PlatformComparisonChart.tsx`
  - Page: `src/pages/Analyst/PlatformAnalytics.tsx`
  - API: `getPlatformComparison(params)` → `GET /v1/analytics/platforms/comparison`
  - Mục đích: so sánh nền tảng (total_products, total_reviews, avg_rating, total_revenue) theo từng nền tảng.

- **Scatter plot (custom SVG) — Price vs Rating**
  - Component: `src/components/analytics/PriceVsRatingChart.tsx`
  - Page: `src/pages/Analyst/PricingAnalytics.tsx`
  - API: `getPriceVsRevenue(params)` → `GET /v1/analytics/pricing/price-vs-revenue`
  - Mục đích: phân tích tương quan giá/đánh giá giữa sản phẩm (price on X, rating on Y).

- **Review summary (Recharts Bar + Pie)**
  - Component: `src/components/analytics/ReviewSummaryChart.tsx`
  - Page: `src/pages/Analyst/ProductAnalytics.tsx` (product detail)
  - API: `getProductReviewSummary(params)` → `GET /v1/analytics/products/{product_key}/reviews/summary`
  - Mục đích: phân phối số sao (rating breakdown) bằng bar + pie; hiển thị các thống kê tổng quan.

- **KPI cards / Single metrics**
  - Used across dashboards (AnalyticsDashboard, ProductAnalytics, etc.)
  - API: `getOverviewKPIs(params)` → `GET /v1/analytics/overview/kpis` or `getOverviewReport()` → `kpis`
  - Mục đích: hiển thị các chỉ số headline (Total revenue, Total products, Avg rating...) trên KPI cards.

Ghi chú nhanh:
- Những components trên dùng data trực tiếp từ `src/services/analyticsApi.ts` — nếu bạn demo, mở file service để show function name và endpoint.
- Một số trang chứa các chart mẫu (ví dụ `src/pages/Charts/*` dùng `apexcharts`) — đó là demo UI, còn phần phân tích chính dùng các component trong `src/components/analytics/`.

- **Line chart**: Xu hướng theo thời gian (`x = date/time`), ít series (≤5). Dùng để nhìn slope, seasonality, trend.
- **Area chart**: Giống line nhưng nhấn magnitude/tổng; phù hợp khi muốn thấy khối lượng theo thời gian.
- **Bar chart (vertical)**: So sánh giữa các category, ranking; dùng khi `x` là categorical hoặc discrete.
- **Grouped bar**: So sánh nhiều series trong cùng category (side-by-side).
- **Stacked bar / Stacked area**: Hiển thị composition (part-to-whole) theo thời gian hoặc category; chỉ dùng khi số segments ≤4–6.
- **Pie / Donut**: Thành phần đơn giản ≤6 phần; tránh khi slices gần bằng nhau hoặc cần so sánh chính xác.
- **Histogram**: Phân bố một biến liên tục (frequency / density).
- **Boxplot**: Phân bố theo nhóm, hiển thị median, IQR, outliers.
- **Scatter plot**: Tương quan giữa hai biến (x,y); thêm trendline khi cần phân tích mối quan hệ.
- **Heatmap**: Hiển thị pattern trên ma trận (ví dụ time-of-day × day-of-week).
- **Map (choropleth / bubble)**: Dữ liệu địa lý — choropleth cho tỷ lệ (rate), bubble cho counts/volume.
- **Table**: Khi cần giá trị chính xác, nhiều cột, hoặc sort/filter; kết hợp sparkline nếu cần visual nhỏ.
- **KPI card / Single metric**: Headline numbers + delta so với kỳ trước; dùng cho báo cáo nhanh.
- **Funnel**: Các bước chuyển đổi, thể hiện drop-off giữa các bước.
- **Waterfall**: Giải thích các thành phần làm thay đổi tổng (breakdown of delta).

- **Quy trình chọn nhanh**:
  1) X là thời gian? → Line / Area.
  2) X là category và cần so sánh? → Bar (grouped nếu nhiều series).
  3) Cần composition? → Stacked bar (hoặc Pie nếu đơn giản và ít phần).
  4) Cần phân bố? → Histogram / Boxplot.
  5) Cần tương quan? → Scatter.
  6) Dữ liệu có location? → Map.
  7) Muốn headline metric? → KPI card hoặc Table.

- **Thuật ngữ (Glossary) — ngắn & ví dụ**:
  - **Composition (thành phần)**: cách một tổng (total) được cấu thành bởi các phần. Dùng khi bạn muốn biết mỗi phần đóng góp bao nhiêu vào tổng.
    - Ví dụ: "Tổng doanh thu = Online + Cửa hàng + Đối tác" → dùng `stacked bar` hoặc `stacked area` để thấy tỉ lệ đóng góp theo thời gian.
  - **Headline (headline metric / KPI)**: một chỉ số chính, con số nổi bật để nắm nhanh trạng thái (single-number summary) thường kèm delta so sánh kỳ trước.
    - Ví dụ: `Total Revenue: 1,234,567 VND (Δ +5% so với tháng trước)` → hiển thị bằng `KPI card` hoặc `single metric`.

- **Lưu ý trực quan & performance**:
  - Hạn chế số series/points trên client (agg server-side nếu >1000 điểm).
  - Dùng palette tương thích color-blind và đủ contrast cho labels.
  - Ghi rõ axis labels, unit, và tooltip có nguồn/period.
  - Luôn có empty / loading / error state.
  - Tránh Pie khi các phần gần bằng nhau; dùng Bar để dễ so sánh giá trị.

**5) Mẫu câu trả lời để chứng minh bạn hiểu và tự code**
- "Tôi code phần frontend: gọi `getOverviewTrends` trong `src/services/analyticsApi.ts`, map `points[].date` → x và `points[].revenue` → y để vẽ line chart." 
- "Tôi xử lý token với axios interceptor trong mỗi file service: cookie `access_token` được attach vào header Authorization." 
- "Khi backend trả `total_revenue`, frontend chỉ hiển thị; tính toán aggregation do backend đảm nhiệm. Nếu cần thay đổi aggregate, tôi sẽ yêu cầu backend thay đổi hoặc viết adapter phía frontend nếu muốn tạm thời." 
- "Để test, tôi mock axios responses trong unit tests hoặc dùng Postman với sample JSON." 

**6) Câu trả lời mẫu cho các câu hỏi thường gặp**
- Q: "Tại sao biểu đồ này hiển thị như vậy?"
  - A: "Biểu đồ hiển thị trường X từ endpoint Y vì chúng ta muốn thể hiện [mục đích phân tích]. Trục X là `date`, trục Y là `revenue`, dùng để thấy xu hướng theo thời gian." 
- Q: "Ai tính `total_revenue`?"
  - A: "Backend tính (aggregation). Frontend chỉ hiển thị và có thể format số/locale." 
- Q: "Nếu contract API thay đổi thì sao?"
  - A: "Tôi sẽ cập nhật mapper ở `src/services/*` hoặc component nếu đổi nhỏ; với breaking change sẽ phối hợp với backend dev để version API hoặc tạo adapter." 

**7) Cách chứng minh bạn là người code frontend (kỹ thuật / bằng chứng)**
- Show Git history: `git log -- src/components/...` và `git diff` files bạn đã chỉnh sửa.
- Open key commits (message + diff) that implement a feature (filters, charts, or mapping). Nếu chưa commit, show local branches. (Lưu ý: commit history phụ thuộc repo state.)
- Show direct edits in editor: open `src/services/<file>` and `src/pages/<page>` to show code you wrote (lines with mapping / transform). Point out small but specific choices (date formatting, debounce time, cache). 
- Show unit tests or snapshots if any (tests folder) proving component behavior.

**8) Troubleshooting nhanh (common issues & fixes)**
- Issue: 401 / missing token → Check cookie `access_token` and axios interceptor. Fix: re-login or set cookie.
- Issue: slow API → Check network tab, enable caching, or request smaller `page_size`.
- Issue: chart empty → Verify response.data points array length and date range; check JS errors in console.
- Issue: wrong numbers → Confirm filters (`from_date`/`to_date`/`platform`) passed in query params.

**9) Useful commands (PowerShell) to show during demo**
```powershell
# show git history for a file
git log --pretty=oneline -- src/services/analyticsApi.ts

# show last commit diff for a file
git show HEAD:src/services/analyticsApi.ts | more
```

**10) Next steps you can offer the requester (optional)**
- Add `docs/examples/` with sample JSON responses for quick offline demo.
- Create an OpenAPI/Swagger spec for backend (or ask backend team) to auto-generate client docs.
- Add a small mock server (json-server or MSW) to let the requester demo without backend.

---