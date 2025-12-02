"""
HƯỚNG DẪN: Thêm code này vào main.py để khởi tạo connection pool

BỔ SUNG VÀO MAIN.PY
"""

# ==========================
# BƯỚC 1: THÊM IMPORTS (near the top of main.py, around line 60-70)
# ==========================
# Thêm 2 dòng này vào phần imports ở đầu file:

from app.db_pool import init_pool, close_pool
from app.db_config import DATABASE_URL


# ==========================
# BƯỚC 2: CẬP NHẬT LIFESPAN FUNCTION (around line 286)
# ==========================
# Tìm function `async def lifespan(app: FastAPI):` và thêm code khởi tạo pool

# TRƯỚC KHI SỬA (OLD):
"""
async def lifespan(app: FastAPI):
    # Your existing startup code here...
    
    yield
    
    # Your existing shutdown code here...
"""

# SAU KHI SỬA (NEW):
"""
async def lifespan(app: FastAPI):
    # STARTUP
    logger.info("Starting application...")
    
    # >>> THÊM CODE NÀY <<<
    # Initialize database connection pool
    logger.info("Initializing database connection pool...")
    try:
        await init_pool(DATABASE_URL, min_size=5, max_size=20)
        logger.info("✅ Database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize connection pool: {e}")
        raise
    # >>> KẾT THÚC CODE THÊM <<<
    
    # Your other existing startup code (if any)...
    
    yield
    
    # SHUTDOWN
    logger.info("Shutting down application...")
    
    # >>> THÊM CODE NÀY <<<
    # Close database connection pool
    try:
        await close_pool()
        logger.info("✅ Database connection pool closed")
    except Exception as e:
        logger.error(f"Error closing pool: {e}")
    # >>> KẾT THÚC CODE THÊM <<<
    
    # Your other existing shutdown code (if any)...
"""


# ==========================
# FULL EXAMPLE (nếu bạn cần tham khảo đầy đủ)
# ==========================

from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan with database pool management"""
    
    # STARTUP
    logger.info("=== Application Starting ===")
    
    # Initialize database connection pool
    logger.info("Initializing database connection pool...")
    try:
        await init_pool(
            database_url=DATABASE_URL,
            min_size=5,   # Minimum connections maintained
            max_size=20   # Maximum connections allowed
        )
        logger.info("✅ Database connection pool ready")
    except Exception as e:
        logger.error(f"❌ Failed to initialize connection pool: {e}")
        raise  # Stop app startup if pool init fails
    
    logger.info("=== Application Ready ===")
    
    yield  # Application runs here
    
    # SHUTDOWN
    logger.info("=== Application Shutting Down ===")
    
    # Close database connection pool
    logger.info("Closing database connection pool...")
    try:
        await close_pool()
        logger.info("✅ Database connection pool closed")
    except Exception as e:
        logger.error(f"Error closing connection pool: {e}")
    
    logger.info("=== Application Stopped ===")


# SAU KHI THÊM CODE, LƯU FILE VÀ SERVER SẼ TỰ ĐỘNG RELOAD
# Kiểm tra logs để xác nhận:
# - "✅ Database connection pool ready" - Pool đã khởi tạo thành công
# - "AI Summarizer initialized with providers: OpenAI, Google Gemini" - AI providers sẵn sàng
