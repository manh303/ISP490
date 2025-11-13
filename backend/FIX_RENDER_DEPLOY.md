# Fix Render Deployment Issues

## Problem
```
ModuleNotFoundError: No module named 'services'
```

## Root Cause
Relative imports không work trên Render vì Python path khác với local.

## Solution Applied

### 1. Fixed `app/middleware/activity_middleware.py`
```python
# Before
from services.activity_logger import ActivityLogger
from core.config import settings
from utils.auth_helpers import decode_access_token

# After
from app.services.activity_logger import ActivityLogger
try:
    from app.core.config import settings
except ImportError:
    from core.config import settings
    
try:
    from app.utils.auth_helpers import decode_access_token
except ImportError:
    from utils.auth_helpers import decode_access_token
```

### 2. Verify Other Files
Check all imports use `app.` prefix:
```bash
cd backend
grep -r "from services" app/ --include="*.py"
grep -r "from utils" app/ --include="*.py"
grep -r "from core" app/ --include="*.py"
```

### 3. Update `start.sh` (if exists)
```bash
#!/bin/bash
cd /opt/render/project/src/backend
export PYTHONPATH="/opt/render/project/src/backend:$PYTHONPATH"
uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1
```

### 4. Update Render Build Command
```bash
cd backend && pip install -r requirements.txt
```

### 5. Update Render Start Command
```bash
cd backend && uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1
```

## Test Locally Before Deploy

```bash
cd backend

# Test import
python -c "from app.main import app; print('✅ Import OK')"

# Test run
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Common Import Patterns to Fix

### Pattern 1: Direct import
```python
# ❌ Wrong
from services.user_service import UserService

# ✅ Correct
from app.services.user_service import UserService
```

### Pattern 2: Try-except fallback
```python
# ✅ Best (works both local and Render)
try:
    from app.services.user_service import UserService
except ImportError:
    from services.user_service import UserService
```

### Pattern 3: Relative import
```python
# ❌ Wrong
from ..services.user_service import UserService

# ✅ Correct
from app.services.user_service import UserService
```

## Files to Check

1. ✅ `app/middleware/activity_middleware.py` - FIXED
2. `app/main.py` - Check imports
3. `app/api/v1/*.py` - Check imports
4. `app/services/*.py` - Check imports
5. `app/utils/*.py` - Check imports

## Deployment Checklist

- [x] Fix import paths in middleware
- [ ] Test locally: `python -c "from app.main import app"`
- [ ] Commit changes: `git add . && git commit -m "Fix imports for Render"`
- [ ] Push to GitHub: `git push origin manh303`
- [ ] Trigger Render redeploy
- [ ] Check Render logs for errors
- [ ] Test API endpoints

## If Still Fails

### Check Render Logs
```
# Look for:
- ModuleNotFoundError
- ImportError
- PYTHONPATH issues
```

### Add to Render Environment Variables
```
PYTHONPATH=/opt/render/project/src/backend
```

### Update requirements.txt
Make sure all dependencies are listed:
```
fastapi
uvicorn[standard]
sqlalchemy
psycopg2-binary
pydantic
python-jose[cryptography]
passlib[bcrypt]
python-multipart
```

## Quick Fix Script

```bash
# Run this to fix all imports
cd backend/app
find . -name "*.py" -type f -exec sed -i 's/from services\./from app.services./g' {} +
find . -name "*.py" -type f -exec sed -i 's/from utils\./from app.utils./g' {} +
find . -name "*.py" -type f -exec sed -i 's/from core\./from app.core./g' {} +
find . -name "*.py" -type f -exec sed -i 's/from models\./from app.models./g' {} +
find . -name "*.py" -type f -exec sed -i 's/from constants\./from app.constants./g' {} +
```

## Verify Fix

```bash
# Test import
cd backend
python -c "
import sys
sys.path.insert(0, '.')
from app.main import app
print('✅ All imports working!')
"
```
