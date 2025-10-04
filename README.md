# E-commerce Decision Support System (DSS)

A comprehensive, production-ready e-commerce analytics and decision support system built with modern big data technologies and real-time streaming capabilities.

## <× Architecture Overview

This system provides real-time analytics, machine learning insights, and decision support for e-commerce operations using:

- **Real-time Data Processing**: Apache Kafka + Spark Streaming
- **Data Orchestration**: Apache Airflow
- **Databases**: PostgreSQL + MongoDB
- **Backend API**: FastAPI with WebSocket support
- **ML Pipeline**: Integrated machine learning for customer analytics
- **Monitoring**: Prometheus + Grafana dashboards
- **Containerization**: Full Docker deployment

## =€ Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- 8GB+ RAM recommended

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd ecommerce-dss-project
```

2. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. **Start the system**
```bash
docker-compose up -d
```

4. **Verify deployment**
```bash
docker-compose ps
```

## =æ System Components

### Core Services

- **Backend API** (`backend/`) - FastAPI application with ML endpoints
- **Data Pipeline** (`data-pipeline/`) - Spark streaming and ML pipeline
- **Airflow** (`airflow/`) - Data orchestration and ETL workflows
- **Databases** (`database/`) - PostgreSQL and MongoDB configurations
- **Monitoring** (`monitoring/`) - Prometheus metrics and Grafana dashboards

### Key Features

 Real-time customer behavior tracking
 Product recommendation engine
 Sales forecasting and analytics
 Customer lifetime value (CLV) prediction
 Inventory optimization insights
 Real-time dashboards and alerts
 WebSocket streaming for live updates
 RESTful APIs for integration

## =' Development Setup

### Backend Development
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python app/main.py
```

### Data Pipeline Development
```bash
cd data-pipeline
pip install -r requirements.txt
python ml_pipeline_manager.py
```

### Frontend Development (if applicable)
```bash
cd frontend
npm install
npm run dev
```

## =Ê Monitoring & Analytics

### Access Points

- **API Documentation**: http://localhost:8000/docs
- **Grafana Dashboards**: http://localhost:3000 (admin/admin)
- **Prometheus Metrics**: http://localhost:9090
- **Airflow UI**: http://localhost:8080

### Available Dashboards

1. **E-commerce Analytics Dashboard** - Sales, conversion, product performance
2. **Customer Behavior Dashboard** - User journey, segmentation, CLV
3. **System Monitoring Dashboard** - Infrastructure health, performance metrics

## = Data Pipeline

### ETL Workflows

1. **Data Collection**: Real-time event capture from e-commerce platform
2. **Stream Processing**: Kafka + Spark for real-time analytics
3. **ML Pipeline**: Customer segmentation, recommendation, forecasting
4. **Data Storage**: Processed data in PostgreSQL, raw data in MongoDB

### Pipeline Components

```
Data Sources ’ Kafka ’ Spark Streaming ’ ML Models ’ Databases ’ APIs ’ Dashboards
```

## > Machine Learning Features

- **Customer Segmentation**: RFM analysis and clustering
- **Recommendation System**: Collaborative and content-based filtering
- **Sales Forecasting**: Time series prediction models
- **Churn Prediction**: Customer retention insights
- **Dynamic Pricing**: Price optimization recommendations

## = Security & Configuration

### Environment Variables

Key configuration in `.env`:
```bash
# Database
DB_HOST=postgres
DB_NAME=ecommerce_dss
DB_USER=postgres
DB_PASSWORD=your_password

# MongoDB
MONGO_ROOT_USER=admin
MONGO_ROOT_PASSWORD=your_mongo_password

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Redis
REDIS_URL=redis://redis:6379
```

### Security Features

- JWT authentication for API access
- Environment-based configuration
- Docker network isolation
- Health checks for all services

## =È Performance & Scaling

### Recommended Resources

- **Development**: 8GB RAM, 4 CPU cores
- **Production**: 16GB+ RAM, 8+ CPU cores, SSD storage
- **Database**: Separate PostgreSQL and MongoDB instances for production

### Scaling Considerations

- Kafka partitioning for high-throughput data ingestion
- Spark cluster mode for large-scale processing
- Redis clustering for session management
- Load balancer for API endpoints

## >ê Testing

Run the test suite:
```bash
# Backend tests
cd backend
python -m pytest tests/

# Data pipeline tests
cd data-pipeline
python -m pytest tests/

# Integration tests
docker-compose -f docker-compose.test.yml up --abort-on-container-exit
```

## =Ú Documentation

Detailed documentation is available in the `docs/` directory:

- [System Architecture](docs/design/system_architecture.md)
- [API Documentation](docs/requirements/api_documentation.md)
- [Deployment Guide](docs/deployment/deployment_guide.md)
- [Streaming Architecture](docs/deployment/STREAMING_ARCHITECTURE_GUIDE.md)
- [Troubleshooting Guide](docs/deployment/TROUBLESHOOTING.md)

## > Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## =Ë Project Status

-  Core backend API
-  Real-time data pipeline
-  ML model integration
-  Monitoring setup
- = Frontend dashboard (in progress)
- =Ë Advanced ML features (planned)

## =Þ Support

For questions and support:
- Check the [Troubleshooting Guide](docs/deployment/TROUBLESHOOTING.md)
- Review system logs: `docker-compose logs [service-name]`
- Create an issue in the repository

## =Ä License

This project is part of a Final Project (Ó Án) for FPT Fall 2025.

---

**Built with d for modern e-commerce analytics**