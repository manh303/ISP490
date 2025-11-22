# Airflow ML Training Pipeline - Permission Fix

## Problem
```
PermissionError: [Errno 13] Permission denied: '/app/ml'
```

Airflow container runs with user `airflow` (uid 50000), but `/app/ml` path doesn't have write permissions for this user.

## Root Cause
- DAG `ml_training_pipeline_dag.py` tried to create directories at `/app/ml`
- Airflow container user can't write to `/app/ml`
- Need to use `/opt/airflow/ml` which is the Airflow home directory (writable)

## Solution Implemented

### 1. Updated DAG Configuration
**File:** `airflow/dags/ml_training_pipeline_dag.py` (lines 61-64)

Changed paths from `/app/ml` to `/opt/airflow/ml`:
```python
# OLD:
ML_PROJECT_PATH = '/app/ml'
MODELS_OUTPUT_DIR = '/app/ml/models/ml-models'
DATA_DIR = '/app/ml/data'
LOGS_DIR = '/app/ml/logs'

# NEW:
ML_PROJECT_PATH = '/opt/airflow/ml'  # Airflow writable location
MODELS_OUTPUT_DIR = '/opt/airflow/ml/models/ml-models'
DATA_DIR = '/opt/airflow/ml/data'
LOGS_DIR = '/opt/airflow/ml/logs'
```

### 2. Updated docker-compose.yml Volumes
Added volume mounts for all Airflow services:

**airflow-webserver** (line 362):
```yaml
- ./ml:/opt/airflow/ml
```

**airflow-scheduler** (line 391):
```yaml
- ./ml:/opt/airflow/ml
```

**airflow-worker** (line 414):
```yaml
- ./ml:/opt/airflow/ml
```

## Directory Structure

### In Airflow Container
```
/opt/airflow/
├── dags/                 # DAG files (read-only)
├── logs/                 # Airflow logs (writable)
├── plugins/              # Custom plugins
└── ml/                   # ML models & data (NEW - writable)
    ├── models/
    │   └── ml-models/    # Model output directory
    ├── data/             # Training data
    └── logs/             # ML training logs
```

### Host Machine
```
./ml/                     # Shared with container at /opt/airflow/ml
├── models/
│   └── ml-models/        # All .pkl files
├── data/                 # Raw and processed data
└── logs/                 # Training logs
```

## How It Works

1. **Initial State**: `./ml/` folder exists on host with pre-trained models
2. **Container Start**: Docker mounts `./ml:/opt/airflow/ml` (read-write)
3. **DAG Execution**: Airflow user can now read/write to `/opt/airflow/ml`
4. **Setup Task**: `setup_directories()` creates subdirectories successfully
5. **Training Tasks**: Can read models and write output

## Volume Mapping
```
Host Machine          Container
./ml/          ←→    /opt/airflow/ml  (read-write)
./ml/          ←→    /app/ml           (read-only)
```

This way:
- Airflow can read the pre-trained models at `/opt/airflow/ml`
- Backend API can read models at `/app/ml`
- Airflow can write training results to `/opt/airflow/ml`

## Benefits

✅ **Permissions Fixed**: Airflow user can now create directories  
✅ **Backwards Compatible**: Both `/app/ml` and `/opt/airflow/ml` have same content  
✅ **Clean Separation**: Airflow uses its own home directory  
✅ **No Root User**: No need to run Airflow as root  

## Testing

### 1. Restart Airflow containers
```bash
docker-compose down
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
```

### 2. Monitor DAG logs
```bash
docker-compose logs -f airflow-scheduler
```

### 3. Expected Success Log
```
✅ Directories created successfully
```

No more:
```
PermissionError: [Errno 13] Permission denied: '/app/ml'
```

## Files Modified

1. **airflow/dags/ml_training_pipeline_dag.py**
   - Lines 61-64: Changed path configuration from `/app/ml` to `/opt/airflow/ml`

2. **docker-compose.yml**
   - Line 362 (airflow-webserver): Added `- ./ml:/opt/airflow/ml`
   - Line 391 (airflow-scheduler): Added `- ./ml:/opt/airflow/ml`
   - Line 414 (airflow-worker): Added `- ./ml:/opt/airflow/ml`

## Summary

The fix ensures Airflow training pipeline can:
1. Create required directories at runtime
2. Read pre-trained ML models
3. Train new models and save outputs
4. All without permission issues

---
**Status**: Fixed ✓
**Affected Services**: Airflow Scheduler, Worker, Webserver
**Downtime**: None (restart required)
