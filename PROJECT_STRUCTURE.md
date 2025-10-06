# E-commerce DSS Project - Clean Structure

## 🎯 Vietnam-Only E-commerce Decision Support System

This is a streamlined version focused solely on Vietnamese e-commerce market analysis.

## 📁 Project Structure

```
ecommerce-dss-project/
├── README.md                               # Main project documentation
├── docker-compose.yml                     # Docker services configuration
├── .gitignore                             # Git ignore rules
│
├── backend/                               # FastAPI backend
│   ├── app/main.py                       # Main application
│   ├── requirements.txt                  # Python dependencies
│   └── Dockerfile                        # Backend container
│
├── database/                             # Database configurations
│   └── docker/
│       ├── mongodb/Dockerfile            # MongoDB setup
│       └── postgres/Dockerfile           # PostgreSQL setup
│
├── airflow/                              # Data pipeline orchestration
│   ├── dags/dss_etl_dag.py              # Main ETL workflow
│   ├── requirements.txt                 # Airflow dependencies
│   └── Dockerfile                       # Airflow container
│
├── scripts/                              # Data processing scripts
│   ├── vietnam_only_data_collector.py   # Vietnam data collector
│   ├── run_vietnam_data.bat            # Windows data collection
│   ├── streaming_data_generator.py      # Real-time data streams
│   ├── data_validation.py               # Data quality validation
│   └── requirements.txt                 # Script dependencies
│
├── data/                                # Data storage
│   └── vietnam_only/                    # Vietnamese market data
│       ├── san_pham_dien_tu_vn_*.csv    # Vietnamese products
│       ├── khach_hang_vn_*.csv          # Vietnamese customers
│       ├── don_hang_vn_*.csv            # Vietnamese orders
│       └── thong_ke_thi_truong_vn_*.json # Market statistics
│
├── monitoring/                          # System monitoring
│   └── prometheus/prometheus.yml        # Metrics configuration
│
└── docs/                                # Documentation
    └── deployment/
        ├── STREAMING_ARCHITECTURE_GUIDE.md
        └── TROUBLESHOOTING.md
```

## 🚀 Quick Start

1. **Generate Vietnam Data**:
   ```cmd
   cd scripts
   run_vietnam_data.bat
   ```

2. **Start Services**:
   ```cmd
   docker-compose up -d
   ```

3. **Validate Data**:
   ```cmd
   cd scripts
   python data_validation.py
   ```

## 🇻🇳 Vietnam Market Focus

- **Language**: Vietnamese (Tiếng Việt)
- **Currency**: VND (Vietnamese Dong)
- **Market**: 63 Vietnamese provinces
- **Payment**: MoMo, ZaloPay, Bank Transfer, COD
- **Data**: Based on Vietnam E-commerce Report 2023-2024

## 📊 Data Analytics

- Sales Management (Quản lý bán hàng)
- Procurement Management (Quản lý mua hàng)
- Inventory Management (Quản lý kho)
- Business Operations (Quản lý kinh doanh)

## ⚡ Real-time Features

- Streaming data pipeline with Kafka
- Real-time analytics dashboard
- Live monitoring with Prometheus + Grafana
- WebSocket API for live updates

## 🛠️ Technology Stack

- **Backend**: FastAPI + Python
- **Databases**: PostgreSQL + MongoDB
- **Streaming**: Apache Kafka + Spark
- **Orchestration**: Apache Airflow
- **Monitoring**: Prometheus + Grafana
- **Containerization**: Docker + Docker Compose

## 📈 Business Intelligence

Focus on Vietnamese e-commerce market with real market data and local business patterns for accurate decision support system analytics.
