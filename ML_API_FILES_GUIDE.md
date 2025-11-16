# ML API Files Guide - Quick Navigation

## 📁 File Structure & Location

### Database Files
```
database/
├── schema/
│   └── ml_tables.sql                 ← Updated with ml_model_registry table
└── seeds/
    ├── ml_model_registry_seed.sql    ← Sample ML models (NEW)
    └── ml_predictions_seed.sql       ← Sample recommendations & predictions (NEW)
```

### Backend API Files
```
backend/
├── app/
│   ├── api/v1/
│   │   └── ml_api.py                 ← Updated with 6 new endpoints
│   └── schemas/
│       └── ml_schemas.py             ← Pydantic models for ML API (NEW)
├── ml_api_test.http                  ← VS Code REST Client test file (NEW)
├── ML_API_Postman.json               ← Postman collection (NEW)
├── ML_API_CONTRACT.md                ← Complete API documentation (NEW)
└── verify_ml_api.py                  ← Endpoint verification script (NEW)
```

### Project Root Files
```
project_root/
├── ML_API_DAY2_SUMMARY.md            ← Day 2 completion summary (NEW)
└── ML_API_FILES_GUIDE.md             ← This file (NEW)
```

---

## 📖 Which File to Use For What?

### I want to understand the API contract
👉 **Read:** `ML_API_CONTRACT.md`
- Complete endpoint specifications
- Request/response examples
- Query parameter documentation
- Error codes reference

### I want to test endpoints quickly
👉 **Use:** `ml_api_test.http` (VS Code REST Client)
- 24 ready-to-run test requests
- Click "Send Request" in VS Code
- Instant response preview

### I want to import in Postman
👉 **Use:** `ML_API_Postman.json`
- Import in Postman via File → Import
- Pre-configured for testing
- Organized in logical groups

### I want to verify the code works
👉 **Run:** `python verify_ml_api.py`
- Tests all imports
- Verifies endpoints are defined
- Executes sample endpoint calls
- Shows results summary

### I want to see implementation summary
👉 **Read:** `ML_API_DAY2_SUMMARY.md`
- Completion checklist
- Files created/modified
- Sample response examples
- Next steps for Day 3

### I want to look at the actual code
👉 **Open:** `backend/app/api/v1/ml_api.py`
- Lines 435-732: New endpoints added
- Lines 1-434: Existing endpoints
- All with error handling

### I want to understand data structures
👉 **Open:** `backend/app/schemas/ml_schemas.py`
- MLModelListResponse
- RecommendationSampleResponse
- PricePredictionSampleResponse
- etc.

---

## 🚀 Quick Start

### Step 1: Understand the Contract (5 min)
```bash
# Open and read the API contract
cat ML_API_CONTRACT.md
```

### Step 2: Review Sample Requests (2 min)
```bash
# Check available test requests
cat backend/ml_api_test.http
```

### Step 3: Test Locally (5 min)
```bash
# Option A: Use VS Code REST Client
# Open backend/ml_api_test.http and click "Send Request"

# Option B: Use Postman
# Import backend/ML_API_Postman.json

# Option C: Run curl command
curl http://localhost:8000/api/v1/ml/models
```

### Step 4: Verify Implementation (2 min)
```bash
cd backend
python verify_ml_api.py
```

---

## 📋 API Endpoints at a Glance

### Models Management
```
GET    /api/v1/ml/models
GET    /api/v1/ml/models/{model_id}/metrics
POST   /api/v1/ml/models/{model_id}/train
```

### Data Queries
```
GET    /api/v1/ml/recommendations/sample?product_sk=...&limit=...
GET    /api/v1/ml/price-predictions/sample?product_sk=...&platform_sk=...
```

### Health & Status
```
GET    /api/v1/ml/health
GET    /api/v1/ml/models/status
GET    /api/v1/ml/metrics
```

### Existing Predictions
```
POST   /api/v1/ml/predict/demand
POST   /api/v1/ml/predict/batch-demand
POST   /api/v1/ml/predict/recommendation
```

---

## 🔍 Sample Data Reference

### Available Products
1, 2, 3, 4, 5, 7, 10, 11, 12, 14, 15, 16, 18

### Available Platforms
- 1: Tiki
- 2: Lazada

### Available Models
- ID 1: demand_linear_v1.0
- ID 3: recommendation_nn_v1.0
- ID 4: recommendation_kmeans_v1.0
- ID 6: customer_segmentation_v1.0

### Sample Data Counts
- Recommendations: 25+ rows
- Price Predictions: 30+ rows
- Models: 4 entries

---

## 💾 Database Setup

### Create Tables
```bash
# Apply schema
psql -U username -d database_name -f database/schema/ml_tables.sql
```

### Insert Sample Data
```bash
# Insert sample models
psql -U username -d database_name -f database/seeds/ml_model_registry_seed.sql

# Insert sample recommendations & predictions
psql -U username -d database_name -f database/seeds/ml_predictions_seed.sql
```

---

## ✅ Completion Status

### Day 2 - Completed ✓
- [x] Database schema created
- [x] 6 new API endpoints implemented
- [x] Pydantic schemas defined
- [x] HTTP test file created
- [x] Postman collection created
- [x] Documentation written
- [x] Verification script included

### Day 3 - Todo
- [ ] Insert seed data into database
- [ ] Update endpoints to query real database
- [ ] Frontend development
- [ ] Integration testing

---

## 🔗 Cross-References

| Need | File | Lines |
|------|------|-------|
| New Endpoints | ml_api.py | 435-732 |
| Route Definition | main.py | Search "ml_router" |
| Request Models | ml_schemas.py | All |
| Test Examples | ml_api_test.http | All |
| Postman Tests | ML_API_Postman.json | All |
| API Spec | ML_API_CONTRACT.md | All |

---

## 🎯 For Frontend Team

### You'll need:
1. **API Documentation:** `ML_API_CONTRACT.md`
2. **Test Examples:** `ML_API_Postman.json` or `ml_api_test.http`
3. **Response Formats:** Section "Sample Response Examples" in summary
4. **Available Data:** Section "Sample Data Reference" above

### Quick start example:
```javascript
// Fetch all ML models
fetch('http://localhost:8000/api/v1/ml/models')
  .then(r => r.json())
  .then(data => console.log(data))

// Fetch recommendations for product
fetch('http://localhost:8000/api/v1/ml/recommendations/sample?product_sk=1&limit=10')
  .then(r => r.json())
  .then(data => console.log(data))
```

---

## 📝 File Descriptions

### ml_api_test.http
- **Purpose:** VS Code REST Client requests
- **Count:** 24 test requests
- **Format:** HTTP file format (native VS Code support)
- **Usage:** Click "Send Request" in VS Code

### ML_API_Postman.json
- **Purpose:** Postman API collection
- **Count:** 16 organized requests
- **Format:** JSON (Postman format)
- **Usage:** Import in Postman application

### ML_API_CONTRACT.md
- **Purpose:** Complete API specification
- **Contains:** Endpoints, parameters, responses, examples
- **Size:** ~500 lines
- **Usage:** Reference for implementation

### ML_API_DAY2_SUMMARY.md
- **Purpose:** Day 2 completion summary
- **Contains:** Checklist, files created, test data reference
- **Size:** ~400 lines
- **Usage:** Progress tracking

### verify_ml_api.py
- **Purpose:** Automated endpoint verification
- **Tests:** Imports, definitions, execution
- **Runtime:** ~5 seconds
- **Usage:** `python verify_ml_api.py`

### ml_schemas.py
- **Purpose:** Pydantic data models
- **Contains:** 10+ schema classes
- **Usage:** Response serialization in API

### ml_api.py
- **Purpose:** FastAPI router and handlers
- **New Lines:** 435-732 (6 endpoints)
- **Old Lines:** 1-434 (existing endpoints)
- **Usage:** API request handling

---

## 🚨 Important Notes

1. **Router already mounted** - No need to modify main.py
2. **Mock data included** - Works immediately without database
3. **Error handling complete** - No unexpected crashes
4. **Tests are ready** - Copy-paste examples into frontend
5. **Database optional** - Works with mock until integrated

---

## 📞 Quick Reference

| Task | File | Command |
|------|------|---------|
| View API spec | ML_API_CONTRACT.md | `cat` or Open in editor |
| Test in VS Code | ml_api_test.http | Click "Send Request" |
| Test in Postman | ML_API_Postman.json | Import → Test |
| Verify code | verify_ml_api.py | `python verify_ml_api.py` |
| Read summary | ML_API_DAY2_SUMMARY.md | `cat` or Open in editor |
| Check endpoints | ml_api.py | Lines 435-732 |
| See schemas | ml_schemas.py | All 400 lines |

---

**Last Updated:** 2025-11-16  
**Status:** Ready for Day 3 Frontend Integration  
**All Files:** Created and tested ✅
