# Vietnam E-commerce Streaming Pipeline - Deployment Guide

## Overview
Complete deployment guide for the Vietnam e-commerce real-time streaming pipeline that feeds data into the Vietnam data warehouse.

## Architecture
```
Vietnam Data Sources → Kafka Producer → Kafka Topics → Spark Streaming → PostgreSQL DW → Streaming API → Frontend
```

## Prerequisites
- Docker & Docker Compose
- Python 3.8+
- At least 8GB RAM
- 50GB free disk space

## Quick Start

### 1. Start Infrastructure
```bash
# Start all Docker services
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 2. Deploy Data Warehouse
```bash
# Deploy Vietnam data warehouse schema
docker exec -i postgres psql -U dss_user -d ecommerce_dss < database/schema/vietnam_datawarehouse_schema.sql
docker exec -i postgres psql -U dss_user -d ecommerce_dss < database/schema/deploy_vietnam_datawarehouse.sql
```

### 3. Start Streaming Pipeline
```bash
# Make script executable
chmod +x scripts/start_vietnam_streaming.sh

# Start all streaming components
./scripts/start_vietnam_streaming.sh all
```

### 4. Monitor Pipeline
```bash
# Check component status
./scripts/start_vietnam_streaming.sh status

# View logs
./scripts/start_vietnam_streaming.sh logs producer
./scripts/start_vietnam_streaming.sh logs consumer
./scripts/start_vietnam_streaming.sh logs api
```

## Component Details

### Kafka Producer (Vietnam Data Generator)
- **File**: `streaming/kafka_producer_vietnam.py`
- **Topics**: `vietnam-customers`, `vietnam-products`, `vietnam-sales`, `vietnam-activities`
- **Data Rate**: 78 events/second total
- **Features**: Realistic Vietnamese e-commerce data with VND pricing, COD payments, regional distribution

### Spark Streaming Consumer
- **File**: `data-pipeline/spark-streaming/vietnam_spark_consumer.py`
- **Function**: Processes Kafka streams and writes to Vietnam data warehouse
- **Batch Interval**: 10 seconds
- **Target**: PostgreSQL `vietnam_dw` schema

### Streaming API
- **File**: `backend/app/streaming_api.py`
- **Port**: 8001
- **Endpoints**:
  - `GET /health` - Health check
  - `GET /metrics` - Real-time metrics
  - `WebSocket /ws/stream` - Live data stream

### Data Warehouse Schema
- **Schema**: `vietnam_dw`
- **Tables**: 5 dimensions + 3 facts + 2 aggregates
- **Features**: Vietnamese business logic, VND currency, regional data

## Environment Variables

### Core Configuration
```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=ecommerce_dss
DB_USER=dss_user
DB_PASSWORD=dss_password_123

# Streaming API
STREAMING_API_HOST=0.0.0.0
STREAMING_API_PORT=8001

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=2g
```

### Data Generation Rates
```bash
# Events per second
CUSTOMER_RATE=5      # New customer registrations
PRODUCT_RATE=3       # Product updates
SALES_RATE=20        # Sales transactions
ACTIVITY_RATE=50     # User activity events
```

## Monitoring & Operations

### Health Checks
```bash
# Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Database connectivity
docker exec postgres pg_isready -U dss_user

# API health
curl http://localhost:8001/health
```

### Log Locations
- Producer: `logs/streaming/kafka_producer.log`
- Consumer: `logs/streaming/spark_consumer.log`
- API: `logs/streaming/streaming_api.log`

### Process Management
```bash
# Stop all components
./scripts/start_vietnam_streaming.sh stop

# Restart pipeline
./scripts/start_vietnam_streaming.sh restart

# Start individual components
./scripts/start_vietnam_streaming.sh producer
./scripts/start_vietnam_streaming.sh consumer
./scripts/start_vietnam_streaming.sh api
```

## Data Verification

### Check Data Flow
```sql
-- Connect to PostgreSQL
docker exec -it postgres psql -U dss_user -d ecommerce_dss

-- Check recent data
SELECT COUNT(*) FROM vietnam_dw.fact_sales_vn WHERE created_at > NOW() - INTERVAL '1 hour';
SELECT COUNT(*) FROM vietnam_dw.dim_customer_vn WHERE created_at > NOW() - INTERVAL '1 hour';

-- Check real-time metrics
SELECT
    DATE_TRUNC('minute', sale_date) as minute,
    COUNT(*) as sales_count,
    SUM(total_amount_vnd) as total_revenue_vnd
FROM vietnam_dw.fact_sales_vn
WHERE sale_date > NOW() - INTERVAL '1 hour'
GROUP BY DATE_TRUNC('minute', sale_date)
ORDER BY minute DESC;
```

### WebSocket Testing
```javascript
// Connect to real-time stream
const ws = new WebSocket('ws://localhost:8001/ws/stream');
ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log('Live data:', data);
};
```

## Performance Tuning

### Kafka Optimization
```properties
# In docker-compose.yml kafka environment
KAFKA_NUM_PARTITIONS=6
KAFKA_REPLICATION_FACTOR=1
KAFKA_LOG_RETENTION_HOURS=24
```

### Spark Optimization
```bash
# Increase resources for higher throughput
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_EXECUTOR_CORES=2
```

### Database Optimization
```sql
-- Monitor table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'vietnam_dw'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Vacuum and analyze regularly
VACUUM ANALYZE vietnam_dw.fact_sales_vn;
```

## Troubleshooting

### Common Issues

#### 1. Kafka Connection Failed
```bash
# Check Kafka container
docker logs kafka

# Verify network connectivity
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

#### 2. Spark Consumer Not Processing
```bash
# Check Spark master
curl http://localhost:8080

# Verify Spark application
docker logs spark-master
```

#### 3. Database Connection Issues
```bash
# Test database connection
docker exec postgres psql -U dss_user -d ecommerce_dss -c "SELECT version();"

# Check database logs
docker logs postgres
```

#### 4. Memory Issues
```bash
# Monitor Docker resource usage
docker stats

# Increase Docker memory limits in docker-compose.yml
```

### Log Analysis
```bash
# Search for errors in logs
grep -i error logs/streaming/*.log

# Monitor real-time logs
tail -f logs/streaming/kafka_producer.log
tail -f logs/streaming/spark_consumer.log
tail -f logs/streaming/streaming_api.log
```

## Scaling Considerations

### Horizontal Scaling
- Increase Kafka partitions for higher throughput
- Add more Spark worker nodes
- Use database read replicas for API queries

### Vertical Scaling
- Increase memory allocation for Spark
- Use faster SSD storage for Kafka logs
- Optimize PostgreSQL configuration

## Security Notes

### Database Security
- Change default passwords in production
- Use SSL connections for database access
- Implement proper user permissions

### API Security
- Add authentication middleware
- Implement rate limiting
- Use HTTPS in production

### Network Security
- Configure proper firewall rules
- Use private networks for internal communication
- Monitor access logs

## Production Deployment

### Infrastructure Requirements
- Kubernetes cluster or Docker Swarm
- Load balancer for API endpoints
- Persistent volumes for data storage
- Monitoring stack (Prometheus + Grafana)

### CI/CD Pipeline
- Automated testing for schema changes
- Blue-green deployment for zero downtime
- Database migration scripts
- Rollback procedures

### Backup Strategy
- Daily database backups
- Kafka topic snapshots
- Configuration file versioning
- Disaster recovery procedures

## Support

### Getting Help
1. Check logs in `logs/streaming/` directory
2. Review component status with status command
3. Verify environment variables
4. Test individual components separately

### Common Commands
```bash
# Full pipeline restart
./scripts/start_vietnam_streaming.sh restart

# Component-specific restart
./scripts/start_vietnam_streaming.sh stop
./scripts/start_vietnam_streaming.sh producer
sleep 10
./scripts/start_vietnam_streaming.sh consumer
sleep 5
./scripts/start_vietnam_streaming.sh api

# Health verification
curl http://localhost:8001/health
./scripts/start_vietnam_streaming.sh status
```