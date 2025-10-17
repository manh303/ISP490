#!/usr/bin/env python3
"""
Simple Data Flow Guide
Hướng dẫn luồng dữ liệu rõ ràng cho Vietnamese E-commerce DSS
"""

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def explain_data_flow():
    """Explain data flow step by step"""

    logger.info("🔄 LUỒNG DỮ LIỆU VIETNAMESE E-COMMERCE DSS")
    logger.info("=" * 80)

    # Step 1: Data Sources
    logger.info("\n📊 BƯỚC 1: NGUỒN DỮ LIỆU (Data Sources)")
    logger.info("=" * 60)
    logger.info("🔹 PostgreSQL Raw Data:")
    logger.info("   • raw_data.orders: 15 đơn hàng lịch sử")
    logger.info("   • raw_data.customers: 1,500 khách hàng")
    logger.info("   • raw_data.products: 5,000 sản phẩm")
    logger.info("   • Ví dụ: order_id='ORD_001', total_amount=2500000 VND, payment_method='vnpay'")

    logger.info("\n🔹 MongoDB Streaming:")
    logger.info("   • processed_orders_stream: Đơn hàng real-time từ website/mobile")
    logger.info("   • Ví dụ: iPhone 15 Pro Max, platform='mobile', location='Ho Chi Minh'")

    logger.info("\n🔹 Generated Big Data:")
    logger.info("   • 100,000 orders + 50,000 customers được sinh tự động")
    logger.info("   • Mô phỏng traffic cao cho Vietnamese e-commerce")
    logger.info("   • Products: iPhone, MacBook, AirPods, etc.")
    logger.info("   • Cities: Ho Chi Minh, Hanoi, Da Nang, etc.")
    logger.info("   • Payments: VNPay, MoMo, ZaloPay, Banking, COD")

    # Step 2: Kafka Layer
    logger.info("\n⚡ BƯỚC 2: KAFKA STREAMING LAYER")
    logger.info("=" * 60)
    logger.info("🔹 Kafka Topics:")
    logger.info("   • ecommerce.orders.stream (3 partitions)")
    logger.info("     → Nhận TẤT CẢ orders từ PostgreSQL + MongoDB + Generated data")
    logger.info("   • ecommerce.customers.stream (3 partitions)")
    logger.info("     → Nhận customer data từ tất cả nguồn")
    logger.info("   • ecommerce.products.stream (3 partitions)")
    logger.info("     → Nhận product data từ tất cả nguồn")

    logger.info("\n🔹 Kafka Workflow:")
    logger.info("   1️⃣ Producers đẩy data vào topics")
    logger.info("   2️⃣ Kafka phân chia data theo partitions")
    logger.info("   3️⃣ Messages được queue để consumer xử lý")
    logger.info("   4️⃣ Spark consumers đọc data theo batches")

    # Step 3: Spark Processing
    logger.info("\n🔥 BƯỚC 3: SPARK STREAMING PROCESSING")
    logger.info("=" * 60)
    logger.info("🔹 Data Ingestion:")
    logger.info("   • Đọc từ 3 Kafka topics cùng lúc")
    logger.info("   • Deserialize JSON messages")
    logger.info("   • Process theo micro-batches")

    logger.info("\n🔹 Data Cleaning:")
    logger.info("   • Loại bỏ records null/invalid")
    logger.info("   • Chuẩn hóa Vietnamese text: 'Hồ Chí Minh' → 'Ho Chi Minh'")
    logger.info("   • Normalize payment methods: 'VN Pay' → 'vnpay'")
    logger.info("   • Validate business rules")

    logger.info("\n🔹 Data Transformation:")
    logger.info("   • Schema mapping cho datawarehouse")
    logger.info("   • Data type conversion")
    logger.info("   • Calculate derived fields")

    # Step 4: Datawarehouse Layers
    logger.info("\n💾 BƯỚC 4: DATAWAREHOUSE LAYERS (Medallion Architecture)")
    logger.info("=" * 60)
    logger.info("🔵 Bronze Layer:")
    logger.info("   • Table: bronze.orders_raw")
    logger.info("   • Purpose: Lưu data thô từ Spark")
    logger.info("   • Contains: Original JSON + metadata + lineage info")
    logger.info("   • Example: order_id, original_data (JSONB), kafka_metadata, processed_at")

    logger.info("\n🥈 Silver Layer:")
    logger.info("   • Table: silver.orders_clean")
    logger.info("   • Purpose: Data đã cleaned và validated")
    logger.info("   • Schema: Structured columns với data quality scores")
    logger.info("   • Example: order_id, customer_id, product_name, total_amount, payment_method, city")

    logger.info("\n🥇 Gold Layer:")
    logger.info("   • Tables: gold.daily_sales_summary, gold.customer_summary")
    logger.info("   • Purpose: Business metrics và KPIs")
    logger.info("   • Example: total_revenue, avg_order_value, top_city, customer_segments")

    logger.info("\n⭐ DW_Core (Star Schema):")
    logger.info("   • Tables: fact_orders, dim_customers, dim_products, dim_time")
    logger.info("   • Purpose: Optimized cho BI tools")
    logger.info("   • Foreign key relationships giữa fact và dimensions")

    # Step 5: Business Intelligence
    logger.info("\n📊 BƯỚC 5: BUSINESS INTELLIGENCE")
    logger.info("=" * 60)
    logger.info("🔹 Dashboard Capabilities:")
    logger.info("   • Real-time sales monitoring")
    logger.info("   • Customer behavior analysis")
    logger.info("   • Vietnamese market insights")
    logger.info("   • Payment method preferences (VNPay vs MoMo vs Banking)")

    logger.info("\n🔹 Analytics Examples:")
    logger.info("   • Revenue by Vietnamese cities")
    logger.info("   • Mobile vs Web platform usage")
    logger.info("   • Customer segmentation (Basic/Standard/Premium)")
    logger.info("   • Product performance tracking")

    # Current Status
    logger.info("\n📈 BƯỚC 6: CURRENT STATUS")
    logger.info("=" * 60)
    logger.info("✅ Data Generated: 150,000 records in Kafka")
    logger.info("✅ Kafka Topics: Operational với 3 partitions each")
    logger.info("✅ Datawarehouse: Bronze→Silver→Gold→DW_Core schemas ready")
    logger.info("⚠️ Processing: Cần run consumer để process Kafka data vào datawarehouse")

    # Summary
    logger.info("\n🎯 TÓM TẮT LUỒNG DỮ LIỆU")
    logger.info("=" * 80)
    logger.info("📊 Sources → ⚡ Kafka → 🔥 Spark → 🔵 Bronze → 🥈 Silver → 🥇 Gold → ⭐ DW_Core → 📊 BI")
    logger.info("")
    logger.info("🔄 WORKFLOW HOÀN CHỈNH:")
    logger.info("1️⃣ Producers extract data từ PostgreSQL, MongoDB, generators")
    logger.info("2️⃣ Data được push vào Kafka topics")
    logger.info("3️⃣ Spark consumers process data theo batches")
    logger.info("4️⃣ Cleaned data flow qua Bronze → Silver → Gold layers")
    logger.info("5️⃣ Star schema trong DW_Core ready cho BI tools")
    logger.info("6️⃣ Dashboard và analytics consume từ datawarehouse")

    logger.info("\n🇻🇳 VIETNAMESE E-COMMERCE SPECIFICS:")
    logger.info("• Currency: VND formatting")
    logger.info("• Payments: VNPay, MoMo, ZaloPay, Banking, COD")
    logger.info("• Geography: Vietnamese cities and districts")
    logger.info("• Products: Localized Vietnamese market products")
    logger.info("• Language: Vietnamese text processing")

    logger.info("\n🎉 STATUS: PIPELINE SẴN SÀNG CHO BIG DATA PROCESSING!")

def main():
    """Main function"""
    explain_data_flow()

if __name__ == "__main__":
    main()