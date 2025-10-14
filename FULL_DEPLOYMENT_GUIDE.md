# 🚀 Complete Data Pipeline Deployment Guide

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │ -> │   Airflow DAGs   │ -> │   Databases     │
│   (APIs, Files) │    │   (Processing)   │    │ (Postgres/Mongo)│
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              v
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend      │ <- │   Backend API    │ <- │   Processed     │
│   (React)       │    │   (FastAPI)      │    │   Data          │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Deployment Steps

### 1. 🗄️ Setup Cloud Databases

#### PostgreSQL (Supabase)
```bash
# 1. Go to https://supabase.com
# 2. Create new project: "ecommerce-dss"
# 3. Copy connection string:
postgresql://postgres:[password]@[host]:5432/postgres
```

#### MongoDB (Atlas)
```bash
# 1. Go to https://cloud.mongodb.com
# 2. Create M0 Free cluster
# 3. Copy connection string:
mongodb+srv://[user]:[password]@[cluster].mongodb.net/ecommerce_dss
```

### 2. ⚙️ Deploy Airflow

#### Option A: Railway (Recommended)
```bash
# 1. Create new Railway project
# 2. Connect GitHub repo: manh303/ISP490
# 3. Set root directory: /airflow
# 4. Railway will auto-detect Docker
# 5. Set environment variables:

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://...
AIRFLOW_VAR_MONGODB_URI=mongodb+srv://...
AIRFLOW_VAR_KAFKA_SERVERS=kafka:29092
AIRFLOW_VAR_REDIS_URL=redis://redis:6379/0
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key
```

#### Option B: Google Cloud Composer
```bash
gcloud composer environments create ecommerce-dss-airflow \
    --location=asia-southeast1 \
    --python-version=3 \
    --node-count=3
```

### 3. 🔧 Deploy Backend API

#### Railway Backend
```bash
# Already configured with:
# - main.py (entry point)
# - Procfile (start command)
# - nixpacks.toml (build config)

# Set environment variables:
DATABASE_URL=postgresql://...
MONGO_URL=mongodb+srv://...
AIRFLOW_URL=https://your-airflow.railway.app
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=your-airflow-password
SECRET_KEY=your-backend-secret
```

### 4. 🌐 Deploy Frontend

#### Vercel Frontend
```bash
# 1. Connect GitHub repo to Vercel
# 2. Set root directory: /frontend
# 3. Set environment variables:

VITE_API_BASE_URL=https://your-backend.railway.app
VITE_AIRFLOW_URL=https://your-airflow.railway.app
```

### 5. 🔗 Connect All Services

#### Update Backend API Routes
```python
# Add to backend/app/main.py
from app.api.v1.airflow_integration import router as airflow_router

app.include_router(
    airflow_router,
    prefix="/api/v1",
    tags=["pipeline"]
)
```

#### Add Frontend Pipeline Dashboard
```javascript
// Add to frontend routing
import PipelineMonitor from './components/pipeline/PipelineMonitor';

// Route: /pipeline
<Route path="/pipeline" component={PipelineMonitor} />
```

## 📊 Complete Data Flow

### 1. Data Collection
```
Airflow DAG → Vietnam E-commerce APIs → Raw Data Collection
```

### 2. Data Processing
```
Raw Data → Pandas/NumPy Processing → Feature Engineering → ML Models
```

### 3. Data Storage
```
Processed Data → PostgreSQL (structured) + MongoDB (documents)
```

### 4. API Layer
```
FastAPI → Database Queries → JSON Responses → Frontend Consumption
```

### 5. Frontend Visualization
```
React Dashboard → Charts/Tables → Real-time Updates → User Interaction
```

## 🔧 Environment Variables Summary

### Airflow (Railway)
```env
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://...
AIRFLOW_VAR_MONGODB_URI=mongodb+srv://...
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
```

### Backend (Railway)
```env
DATABASE_URL=postgresql://...
MONGO_URL=mongodb+srv://...
AIRFLOW_URL=https://your-airflow.railway.app
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=your-password
SECRET_KEY=your-secret
```

### Frontend (Vercel)
```env
VITE_API_BASE_URL=https://your-backend.railway.app
VITE_AIRFLOW_URL=https://your-airflow.railway.app
```

## 🧪 Testing the Complete Flow

### 1. Test Airflow Pipeline
```bash
# Trigger DAG manually
curl -X POST https://your-airflow.railway.app/api/v1/dags/comprehensive_ecommerce_dss_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:password \
  -d '{"execution_date":"2024-10-08T00:00:00Z"}'
```

### 2. Test Backend API
```bash
# Test pipeline status
curl https://your-backend.railway.app/api/v1/pipeline/status

# Test data endpoints
curl https://your-backend.railway.app/api/v1/analytics/summary
```

### 3. Test Frontend
```bash
# Visit your Vercel app
https://your-app.vercel.app/pipeline

# Should show:
# - Pipeline status
# - Recent runs
# - Control buttons
# - Real-time metrics
```

## 🎯 Final URLs

After deployment, you'll have:

- **Frontend**: `https://your-app.vercel.app`
- **Backend API**: `https://your-backend.railway.app`
- **Airflow UI**: `https://your-airflow.railway.app`
- **Databases**: Cloud PostgreSQL + MongoDB

## 📈 Monitoring & Maintenance

### Daily Operations
1. Check pipeline runs in Airflow UI
2. Monitor data quality in backend logs
3. Review performance metrics in frontend dashboard

### Weekly Reviews
1. Database performance optimization
2. Pipeline scheduling adjustments
3. Cost optimization review

## 🔒 Security Considerations

1. **Environment Variables**: Never commit secrets to Git
2. **Database Access**: Use read-only users for analytics
3. **API Authentication**: Implement proper JWT tokens
4. **CORS**: Restrict to specific domains in production
5. **Airflow**: Enable authentication and HTTPS

## ✅ Success Checklist

- [ ] All databases connected and accessible
- [ ] Airflow DAG running successfully every 4 hours
- [ ] Backend API responding to all endpoints
- [ ] Frontend dashboard displaying real-time data
- [ ] Pipeline can be triggered manually from web interface
- [ ] Data flows from collection → processing → visualization
- [ ] All environment variables configured
- [ ] Monitoring and alerting set up