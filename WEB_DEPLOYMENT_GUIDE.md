# 🚀 HƯỚNG DẪN DEPLOY TOÀN BỘ HỆ THỐNG LÊN WEB

**Hệ thống:** E-commerce DSS với Vietnam Electronics Pipeline
**Kiến trúc:** Full-stack Web Application với Data Pipeline
**Deployment:** Docker Compose + Nginx Reverse Proxy

---

## 🎯 **TỔNG QUAN HỆ THỐNG WEB**

### 📊 **Các thành phần chính:**
```
🌐 FRONTEND (React)     → Port 3000 → http://localhost:3000
🔧 BACKEND API (FastAPI) → Port 8000 → http://localhost:8000
📊 AIRFLOW UI           → Port 8080 → http://localhost:8080
🔄 KAFKA UI             → Port 8090 → http://localhost:8090
📈 GRAFANA MONITORING   → Port 3001 → http://localhost:3001
🔍 PROMETHEUS           → Port 9090 → http://localhost:9090
```

### 🏗️ **Load Balancer (Nginx):**
```
http://localhost/        → Frontend React App
http://localhost/api/    → Backend FastAPI
http://localhost/airflow → Airflow WebUI
http://localhost/kafka   → Kafka UI
http://localhost/grafana → Grafana Dashboard
```

---

## 🚀 **BƯỚC 1: CHUẨN BỊ HỆ THỐNG**

### ✅ **Prerequisites:**
```bash
# 1. Cài đặt Docker & Docker Compose
# Windows: Docker Desktop
# Linux:
sudo apt update
sudo apt install docker.io docker-compose

# 2. Verify installation
docker --version
docker-compose --version

# 3. Clone và cd vào project
cd C:\DoAn_FPT_FALL2025\ecommerce-dss-project
```

### 🔧 **Check configuration files:**
```bash
# Verify key files exist
ls -la docker-compose.yml     # ✅ Main deployment file (673 lines)
ls -la .env                   # ✅ Environment config (249 lines)
ls -la deployment/nginx/      # ✅ Load balancer config
ls -la frontend/Dockerfile    # ✅ Frontend container
ls -la backend/Dockerfile     # ✅ Backend container
```

---

## 🚀 **BƯỚC 2: BUILD & DEPLOY TOÀN BỘ HỆ THỐNG**

### 🏗️ **Build all services:**
```bash
# Build tất cả containers
docker-compose build

# Or build specific services if needed
docker-compose build frontend backend data-pipeline airflow
```

### 🚀 **Deploy complete system:**
```bash
# Deploy toàn bộ hệ thống (19 services)
docker-compose up -d

# Monitor deployment progress
docker-compose logs -f
```

### 📊 **Services sẽ được deploy:**

#### **🗄️ Core Databases:**
- `postgres` - PostgreSQL warehouse (Port 5432)
- `mongodb` - Document store (Port 27017)
- `redis` - Cache & sessions (Port 6379)

#### **🔄 Streaming Infrastructure:**
- `zookeeper` - Coordination service (Port 2181)
- `kafka` - Message streaming (Port 9092, 29092)
- `kafka-ui` - Kafka management UI (Port 8090)

#### **⚡ Spark Cluster:**
- `spark-master` - Spark master (Port 7077, 8081)
- `spark-worker-1` - Worker node 1 (Port 8082)
- `spark-worker-2` - Worker node 2 (Port 8083)
- `spark-history` - History server (Port 18080)

#### **🔄 Airflow Orchestration:**
- `airflow-db` - Airflow database
- `airflow-init` - Database initialization
- `airflow-webserver` - Web UI (Port 8080)
- `airflow-scheduler` - Task scheduler
- `airflow-worker` - Task executor

#### **💻 Application Services:**
- `data-pipeline` - Data processing (Port 8888)
- `backend` - FastAPI server (Port 8000)
- `frontend` - React application (Port 3000)

#### **📊 Monitoring:**
- `prometheus` - Metrics collection (Port 9090)
- `grafana` - Dashboards (Port 3001)
- `node-exporter` - System metrics (Port 9100)
- Various database exporters

#### **🌐 Load Balancer:**
- `nginx` - Reverse proxy (Port 80, 443)

---

## 🚀 **BƯỚC 3: VERIFY DEPLOYMENT**

### ✅ **Check all services status:**
```bash
# Check containers status
docker-compose ps

# Should show all services as "Up" or "healthy"
```

### 🌐 **Access web interfaces:**

#### **🎯 Main Application:**
```bash
# Frontend React App
http://localhost:3000

# Backend API Documentation
http://localhost:8000/docs

# Full system via Load Balancer
http://localhost/
```

#### **📊 Management UIs:**
```bash
# Airflow Pipeline Management
http://localhost:8080
# Login: admin / admin123

# Kafka Streaming UI
http://localhost:8090

# Grafana Monitoring
http://localhost:3001
# Login: admin / admin123

# Prometheus Metrics
http://localhost:9090
```

---

## 🚀 **BƯỚC 4: ENABLE VIETNAM ELECTRONICS PIPELINE**

### 🇻🇳 **Activate Direct Pipeline:**
```bash
# Access Airflow UI
open http://localhost:8080

# Or via command line:
docker-compose exec airflow-webserver airflow dags unpause vietnam_electronics_direct

# Trigger first run
docker-compose exec airflow-webserver airflow dags trigger vietnam_electronics_direct
```

### 📊 **Monitor Pipeline:**
```bash
# View DAG execution
http://localhost:8080/admin/airflow/dag/vietnam_electronics_direct

# Check logs
docker-compose exec airflow-webserver airflow tasks logs vietnam_electronics_direct crawl_vietnam_electronics_direct
```

---

## 🚀 **BƯỚC 5: PRODUCTION OPTIMIZATION**

### 🔒 **Security Setup:**
```bash
# 1. Change default passwords in .env:
JWT_SECRET_KEY=your-production-secret-key
DB_PASSWORD=strong-database-password
GRAFANA_ADMIN_PASSWORD=strong-grafana-password

# 2. Enable HTTPS (optional)
# Update nginx config for SSL certificates
```

### 🚀 **Performance Tuning:**
```bash
# 1. Scale services if needed
docker-compose up -d --scale spark-worker=4

# 2. Adjust resource limits in docker-compose.yml
# 3. Enable monitoring alerts in Grafana
```

### 🔄 **Auto-restart:**
```bash
# All services have restart: unless-stopped
# System will auto-recover on reboot
```

---

## 📊 **BƯỚC 6: MONITORING & MANAGEMENT**

### 📈 **Dashboard Access:**
```bash
# System Health
http://localhost:3001 (Grafana)

# Pipeline Status
http://localhost:8080 (Airflow)

# Data Flow
http://localhost:8090 (Kafka UI)

# API Performance
http://localhost:8000/metrics (FastAPI)
```

### 🔍 **Logs & Debugging:**
```bash
# View all logs
docker-compose logs -f

# Specific service logs
docker-compose logs -f frontend
docker-compose logs -f backend
docker-compose logs -f airflow-webserver

# Container health status
docker-compose ps
```

---

## 🚀 **QUICK START COMMANDS**

### 🎯 **Full deployment in one command:**
```bash
# Deploy everything
docker-compose up -d && echo "🚀 System deployed! Access: http://localhost"
```

### 🔄 **Daily operations:**
```bash
# Start system
docker-compose start

# Stop system
docker-compose stop

# Restart system
docker-compose restart

# Update system
docker-compose pull && docker-compose up -d
```

### 🔧 **Development mode:**
```bash
# Hot-reload enabled for frontend/backend
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

---

## 🎯 **EXPECTED RESULTS**

### ✅ **After successful deployment:**

1. **🌐 Frontend:** React app running at http://localhost:3000
2. **🔧 Backend:** FastAPI serving at http://localhost:8000
3. **📊 Airflow:** Pipeline management at http://localhost:8080
4. **🚀 Load Balancer:** Everything accessible via http://localhost/
5. **📈 Monitoring:** Grafana dashboards at http://localhost:3001

### 📊 **System Performance:**
- **Total Services:** 19 containers
- **Memory Usage:** ~8-12GB RAM recommended
- **Storage:** ~10-20GB for data volumes
- **Network:** All services in isolated dss_network

### 🎯 **Data Pipeline:**
- **Vietnam Electronics:** Auto-running every 6 hours
- **Data Sources:** 5 Vietnamese e-commerce platforms
- **Processing:** Direct pipeline (no Kafka complexity)
- **Warehouse:** PostgreSQL with structured data
- **Monitoring:** Real-time dashboards

---

## 🎉 **SUCCESS VERIFICATION**

### ✅ **System is successfully deployed if:**

1. **All containers healthy:** `docker-compose ps` shows all "Up"
2. **Frontend accessible:** http://localhost:3000 loads React app
3. **Backend responsive:** http://localhost:8000/docs shows FastAPI
4. **Airflow running:** http://localhost:8080 shows pipeline management
5. **Load balancer working:** http://localhost/ serves the application
6. **Pipeline active:** `vietnam_electronics_direct` DAG enabled and running
7. **Data flowing:** PostgreSQL warehouse receiving processed data

---

**🚀 Your complete Vietnam Electronics DSS system is now live on the web!**

**Main Access Points:**
- **Application:** http://localhost/
- **Management:** http://localhost/airflow
- **Monitoring:** http://localhost/grafana
- **API:** http://localhost/api/docs

**🎯 The system provides end-to-end data processing from Vietnamese e-commerce platforms to warehouse with full web-based management and monitoring capabilities!**