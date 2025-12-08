PS C:\DoAn_FPT_FALL2025\ecommerce-dss-project> python db_migration_dss_improvements.py

======================================================================
🚀 BẮT ĐẦU DATABASE MIGRATION - DSS IMPROVEMENTS
======================================================================
Thời gian: 2025-12-05 14:27:04
✅ Kết nối database thành công

======================================================================
1. Migration: dwh.fact_product_daily
======================================================================
✅ Thêm cột total_orders vào dwh.fact_product_daily
✅ Thêm cột total_revenue vào dwh.fact_product_daily

💡 Lưu ý: Cần cập nhật ETL job để fill dữ liệu cho 2 cột mới này

======================================================================
2. Migration: dwh.product_metrics_global
======================================================================
✅ Thêm cột total_revenue vào dwh.product_metrics_global
✅ Thêm cột avg_cost vào dwh.product_metrics_global
✅ Thêm cột avg_margin_pct vào dwh.product_metrics_global

💡 Lưu ý: Cần cập nhật ETL job để tính toán:
   - total_revenue: SUM từ fact_product_daily.total_revenue
   - avg_cost: từ bảng cost (nếu có)
   - avg_margin_pct: (avg_price - avg_cost) / avg_price

======================================================================
3. Migration: ml.fact_price_prediction
======================================================================
✅ Thêm cột prediction_confidence vào ml.fact_price_prediction
✅ Thêm comment cho cột prediction_confidence

💡 Lưu ý: Cần cập nhật ML pipeline để tính và lưu prediction_confidence

======================================================================
4. Migration: ml.fact_product_recommendation
======================================================================
✅ Thêm cột co_purchase_count vào ml.fact_product_recommendation
✅ Thêm cột co_purchase_rate vào ml.fact_product_recommendation
✅ Thêm cột avg_bundle_revenue vào ml.fact_product_recommendation
✅ Thêm cột window_days vào ml.fact_product_recommendation
✅ Thêm comment cho cột co_purchase_count
✅ Thêm comment cho cột co_purchase_rate
✅ Thêm comment cho cột avg_bundle_revenue
✅ Thêm comment cho cột window_days

💡 Lưu ý: Cần cập nhật recommendation pipeline để tính co-purchase metrics

======================================================================
7. Migration: dss.dss_action_item (BẮT BUỘC)
======================================================================
ℹ️  Bảng dss.dss_action_item đã tồn tại

======================================================================
5. Migration: ml.fact_review_sentiment (OPTIONAL)
======================================================================
✅ Thêm cột language_code vào ml.fact_review_sentiment
✅ Thêm cột sentiment_source vào ml.fact_review_sentiment

💡 Migration này là optional, không bắt buộc cho DSS API

======================================================================
6. Migration: dwh.dim_product (OPTIONAL)
======================================================================
✅ Thêm cột image_url vào dwh.dim_product
✅ Thêm cột product_url vào dwh.dim_product

💡 Migration này là optional, giúp UI đẹp hơn nhưng không bắt buộc cho DSS API

======================================================================
✅ HOÀN THÀNH TẤT CẢ MIGRATIONS!
======================================================================

📋 TÓM TẮT:
   ✅ Đã thêm cột cho các bảng fact/dim
   ✅ Đã tạo bảng dss.dss_action_item

📝 BƯỚC TIẾP THEO:
   1. Cập nhật ETL jobs để fill dữ liệu cho các cột mới
   2. Cập nhật ML pipelines (price prediction, recommendation)
   3. Chạy lại ETL/ML jobs để có dữ liệu mẫu
   4. Test lại 3 API: /dss/price/run, /dss/reco/run, /dss/review/run
✅ Đã ngắt kết nối database