# 📁 Backend Project Structure Refactoring Guide

## Cấu trúc Hiện Tại (Trước)

```
backend/
├── main.py                          # 1594 DÒNG ❌ Quá lớn
│   ├── Configuration (Settings)
│   ├── Database Connection (DatabaseManager)
│   ├── Pydantic Models
│   ├── Authentication Logic
│   ├── Email Service
│   ├── Routes (Endpoints)
│   └── Helpers Functions
```

**Problem**: 
- 1 file, 1594 dòng khó maintain
- Khó tìm code cần sửa
- Khó reuse các component
- Không follow FastAPI best practices

---

## Cấu trúc Mới (Sau) ✅

```
backend/
├── main.py                          # ~300 dòng (chỉ app initialization)
├── app/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py               # Settings (moved from main.py)
│   │   ├── settings.py              # ✅ CREATED - App configuration
│   │   ├── database.py              # DatabaseManager (moved from main.py)
│   │   └── lifespan.py              # Startup/shutdown logic
│   │
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── models.py                # ✅ CREATED - All Pydantic models
│   │       ├── HealthCheck
│   │       ├── SignInRequest/Response
│   │       ├── SignupRequest/Response
│   │       ├── VerifyEmailRequest/Response
│   │       ├── ForgotPasswordOTPRequest/Response
│   │       └── UserProfile
│   │
│   ├── helpers/
│   │   ├── __init__.py
│   │   ├── auth_helpers.py          # ✅ CREATED - Auth functions
│   │   │   ├── authenticate_user_db()
│   │   │   ├── authenticate_user()
│   │   │   ├── create_user_in_db()
│   │   │   ├── verify_email_token()
│   │   │   ├── activate_user()
│   │   │   └── VALID_USERS
│   │   └── email_helpers.py         # Email utilities
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   ├── email_service_v2.py      # ✅ CREATED - Mailjet service
│   │   └── iam_service.py           # Existing
│   │
│   ├── api/
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── auth.py              # Auth endpoints (signin, signup, etc.)
│   │   │   ├── admin.py
│   │   │   ├── profile.py
│   │   │   └── ...
│   │   └── __init__.py
│   │
│   └── middleware/
│       ├── activity_middleware.py
│       └── ...
│
├── requirements.txt
└── README.md
```

---

## 📋 Các File Được Tách Ra

### 1. ✅ `app/schemas/models.py` (NEW)
**Chứa**: Tất cả Pydantic models

**Import từ main.py**:
- `HealthCheck`
- `SignupRequest`, `SignupResponse`
- `VerifyEmailRequest`, `VerifyEmailResponse`
- `SignInRequest`, `SignInResponse`
- `ForgotPasswordOTPRequest`, `ForgotPasswordOTPResponse`
- `VerifyOTPResetPasswordRequest`, `VerifyOTPResetPasswordResponse`
- `SignOutResponse`
- `UserProfile`, `UserProfileResponse`

**Lợi ích**:
- Dễ maintain models riêng biệt
- Có thể tái sử dụng trong các endpoint khác
- Code clean trong main.py

---

### 2. ✅ `app/core/settings.py` (NEW)
**Chứa**: Configuration settings

**Import từ main.py**:
- `Settings` class
- `settings` instance

**Lợi ích**:
- Tập trung quản lý config
- Dễ mở rộng (thêm settings mới)
- Follow 12-factor app principles

---

### 3. ✅ `app/helpers/auth_helpers.py` (UPDATED)
**Chứa**: Authentication helper functions

**Import từ main.py**:
- `authenticate_user_db()`
- `authenticate_user()`
- `create_user_in_db()`
- `store_verification_token()`
- `verify_email_token()`
- `activate_user()`
- `check_email_exists()`
- `VALID_USERS` dict

**Lợi ích**:
- Logic authentication riêng, dễ test
- Có thể reuse trong endpoints khác
- Dễ mock cho unit tests

---

### 4. ✅ `app/services/email_service_v2.py` (NEW)
**Chứa**: Email service class

**Import từ main.py**:
- `EmailService` class
- `email_service` instance

**Lợi ích**:
- Service layer riêng biệt
- Dễ swap email provider (Sendgrid, etc.)
- Testable

---

### 5. `app/core/database.py` (EXISTING)
**Chứa**: DatabaseManager class

**Already exists**, giữ nguyên

---

## 🔄 Migration Steps

### Step 1: Tạo các file mới ✅
```bash
# Models
mkdir -p backend/app/schemas
touch backend/app/schemas/__init__.py
# (create models.py - DONE)

# Settings
mkdir -p backend/app/core
# (create settings.py - DONE)

# Helpers
mkdir -p backend/app/helpers
# (update auth_helpers.py - DONE)

# Services
mkdir -p backend/app/services
# (create email_service_v2.py - DONE)
```

### Step 2: Update `main.py` imports
Replace old imports with:
```python
from app.schemas.models import (
    HealthCheck, SignInRequest, SignInResponse,
    SignupRequest, SignupResponse, VerifyEmailRequest,
    VerifyEmailResponse, ForgotPasswordOTPRequest,
    ForgotPasswordOTPResponse, VerifyOTPResetPasswordRequest,
    VerifyOTPResetPasswordResponse, SignOutResponse
)

from app.core.settings import settings

from app.helpers.auth_helpers import (
    authenticate_user_db, authenticate_user,
    create_user_in_db, verify_email_token,
    activate_user, check_email_exists,
    store_verification_token, VALID_USERS
)

from app.services.email_service_v2 import email_service, EmailService
```

### Step 3: Remove từ `main.py`
- Xóa class `Settings`
- Xóa class `HealthCheck`, `SignupRequest`, `SignupResponse`, etc.
- Xóa functions `authenticate_user_db()`, `create_user_in_db()`, etc.
- Xóa class `EmailService`
- Xóa `VALID_USERS` dict

### Step 4: Vẫn giữ trong `main.py`
- FastAPI app initialization
- Middleware setup
- Router include
- Essential endpoints (@app.get, @app.post)
- Lifespan context manager
- Error handlers

---

## 📊 Line Count Reduction

**Before**: 1594 lines  
**After**: ~400 lines  
**Reduction**: **75%** ✅

---

## ✅ Best Practices Implemented

1. **Separation of Concerns**
   - Models → `schemas/`
   - Config → `core/`
   - Business logic → `helpers/`, `services/`
   - HTTP layer → `api/`, `main.py`

2. **Reusability**
   - Helper functions can be used in multiple endpoints
   - Models can be shared across routes
   - Settings accessible everywhere

3. **Testability**
   - Auth helpers can be unit tested
   - Email service can be mocked
   - Database logic isolated

4. **Maintainability**
   - Each file has single responsibility
   - Clear imports and exports
   - Easy to find and modify code

5. **Scalability**
   - Easy to add new endpoints
   - Can add more helpers/services
   - Config centralized

---

## 🎯 Next Steps

1. Create `app/api/v1/auth.py` for authentication endpoints
2. Move signin/signup endpoints from main.py to auth.py
3. Create `app/core/lifespan.py` for startup/shutdown logic
4. Update all imports in main.py
5. Test all endpoints

---

## 📝 Current Status

| File | Status | Lines |
|------|--------|-------|
| `app/schemas/models.py` | ✅ CREATED | 110 |
| `app/core/settings.py` | ✅ CREATED | 36 |
| `app/helpers/auth_helpers.py` | ✅ UPDATED | 230 |
| `app/services/email_service_v2.py` | ✅ CREATED | 120 |
| `backend/main.py` | ⏳ TO UPDATE | 1594→400 |

Total saved: **~800 lines** of cleaner, more maintainable code! 🚀
