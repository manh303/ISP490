# Ecommerce DSS Project

This is a full-stack scaffold for an e-commerce Decision Support System (DSS) with a big data pipeline, backend API, and frontend dashboard.

## Quick start (dev)
```bash
# 1) Copy .env.example -> .env (adjust as needed)
cp .env.example .env

# 2) Build & run
docker compose up -d --build

# 3) Open services
# Backend API: http://localhost:8000/docs
# Frontend:    http://localhost:5173
# Postgres:    postgresql://admin:admin@localhost:5432/ecom
```
