# ML Models Deployment Guide for Render

## Problem
ML models are not found on Render deployment because they're not included in the git repository or deployed.

## Solutions

### Option 1: Include Models in Git (Simple, for small models)

1. **Add models to git** (if they're not too large):
```bash
git add ml/models/ml-models/
git commit -m "Add ML models"
git push
```

2. **No additional setup needed** - the code will find models at relative paths

### Option 2: Deploy Models Separately (Recommended for large models)

1. **Upload models to cloud storage** (AWS S3, MinIO, etc.):
```bash
# Example: Upload to MinIO
mc cp ml/models/ml-models/*.pkl minio/ecommerce-dss/ml-models/
```

2. **Add download script to Render buildCommand**:

Edit `render.yaml`:
```yaml
buildCommand: |
  cd backend &&
  pip install --upgrade pip &&
  pip install -r requirements.txt &&
  mkdir -p ../ml/models/ml-models &&
  # Download models from your storage
  # Example with MinIO/S3:
  # aws s3 cp s3://your-bucket/ml-models/ ../ml/models/ml-models/ --recursive
```

### Option 3: Use Environment Variable (Most Flexible)

1. **Update render.yaml** to set ML_PATH:
```yaml
envVars:
  - key: ML_PATH
    value: /app/ml  # or your mounted path
```

2. **Ensure ML directory structure on Render**:
- Make sure the path exists before the app starts
- Or use the fallback to mock data if models aren't found

### Option 4: Use Mock Data (Current Workaround)

The code currently has fallbacks to mock data if models aren't found:
- Demand predictions will return generated data
- Recommendations will use database fallback
- Sentiment analysis will use placeholder values

**No action needed** - app will work with mock data

## Recommended Approach for Your Project

Since you already have models trained locally:

1. **Add to .gitignore check**: Make sure models aren't in .gitignore
```bash
cat .gitignore | grep "ml/models"
```

2. **If they are ignored, remove that line** and add models:
```bash
# Remove from .gitignore if present:
# !ml/models/ml-models/

# Then add:
git add ml/models/ml-models/
git commit -m "Add ML models for production"
git push
```

3. **Deploy to Render** - models will be included automatically

## Verify Models on Render

After deployment, check the logs:
```
✓ Loaded demand model
✓ Loaded nearest neighbors model
✓ Loaded KMeans model
✓ Loaded Sentiment Analysis model
✓ Successfully loaded models
```

If you see warnings instead:
```
⚠ Demand model not found
⚠ No models loaded. ML endpoints will use mock data.
```

Then use one of the solutions above.

## Current Status

The code has been updated to:
- Try multiple possible paths for ML folder
- Gracefully fall back to mock data if models not found
- Accept ML_PATH environment variable for custom locations

No more hard errors - the app will start and use mock data if needed.
