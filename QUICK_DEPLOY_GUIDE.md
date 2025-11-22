# Quick Deploy Guide - ML Models Fix

## What Was Fixed
✓ Dockerfile now copies ML models to production  
✓ ML API path resolution fixed for Render  
✓ Missing sentiment classifier trained and saved  

## One-Line Deploy Command
```bash
git add -A && git commit -m "Fix: ML models deployment - copy ml folder to Docker and train sentiment classifier" && git push origin manh303
```

## Verify Models Exist
```bash
# Check all model files are present
powershell -Command "Get-ChildItem ml\models\ml-models\ | Measure-Object"
```

Expected: 10 files
```
Name    : ml-models
Count   : 10
```

## Check Git Status
```bash
git status
```

Should show:
- Modified: Dockerfile, backend/app/api/v1/ml_api.py
- Untracked: ML_MODELS_FIX_SUMMARY.md, ml/train_sentiment_quick.py, DEPLOYMENT_CHECKLIST.md

## After Deployment
Check Render logs for these success indicators:
```
✓ Loaded demand model
✓ Loaded nearest neighbors model
✓ Loaded KMeans model
✓ Loaded Sentiment Analysis model
✓ Successfully loaded models: ['demand', 'nearest_neighbors', 'kmeans', 'sentiment']
```

## Test ML Endpoints
```bash
# Get ML health status
curl https://ecommerce-dss-backend.onrender.com/api/v1/ml/health

# Test sentiment analysis
curl -X POST https://ecommerce-dss-backend.onrender.com/api/v1/ml/analyze/sentiment \
  -H "Content-Type: application/json" \
  -d '{"review_id": 1, "product_id": 100, "review_text": "Great!", "rating": 5.0}'
```

## Files Changed
- **Dockerfile** - Added: `COPY ml/ ./ml/`
- **backend/app/api/v1/ml_api.py** - Fixed path order (lines 36-47)
- **ml/train_sentiment_quick.py** - NEW: Quick sentiment training
- **ml/models/ml-models/sentiment_classifier.pkl** - NEW: Sentiment model

## Total Time to Deploy
~5 minutes (after git push)

---

**Status:** Ready to deploy ✓
