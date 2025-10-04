#!/bin/bash
# ====================================
# PRODUCTION STARTUP SCRIPT FOR BACKEND API
# ====================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="Big Data Streaming API"
APP_DIR="/app"
LOG_DIR="/app/logs"
MODELS_DIR="/app/models"
DATA_DIR="/app/data"
BACKUP_DIR="/app/backups"

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_debug() {
    if [[ "${DEBUG:-false}" == "true" ]]; then
        echo -e "${BLUE}[DEBUG]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
    fi
}

# Create necessary directories
create_directories() {
    log_info "Creating application directories..."
    
    for dir in "$LOG_DIR" "$MODELS_DIR" "$DATA_DIR" "$BACKUP_DIR" "/app/uploads" "/app/static"; do
        if [[ ! -d "$dir" ]]; then
            mkdir -p "$dir"
            log_info "Created directory: $dir"
        fi
    done
    
    # Set permissions
    chmod 755 "$LOG_DIR" "$MODELS_DIR" "$DATA_DIR" "$BACKUP_DIR"
}

# Wait for dependencies
wait_for_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=30
    local attempt=1
    
    log_info "Waiting for $service_name at $host:$port..."
    
    while ! nc -z "$host" "$port" >/dev/null 2>&1; do
        if [[ $attempt -eq $max_attempts ]]; then
            log_error "$service_name is not available after $max_attempts attempts"
            return 1
        fi
        
        log_debug "Attempt $attempt/$max_attempts: $service_name not ready, waiting..."
        sleep 2
        ((attempt++))
    done
    
    log_info "$service_name is ready!"
    return 0
}

# Check environment variables
check_environment() {
    log_info "Checking environment variables..."
    
    local required_vars=(
        "DATABASE_URL"
        "MONGODB_URL" 
        "REDIS_URL"
        "KAFKA_BOOTSTRAP_SERVERS"
        "SECRET_KEY"
    )
    
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            missing_vars+=("$var")
        fi
    done
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        log_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            log_error "  - $var"
        done
        return 1
    fi
    
    log_info "All required environment variables are set"
    return 0
}

# Wait for all dependencies
wait_for_dependencies() {
    log_info "Waiting for service dependencies..."
    
    # Extract connection details from environment variables
    local postgres_host="${DB_HOST:-postgres}"
    local postgres_port="${DB_PORT:-5432}"
    
    local mongo_host="${MONGO_HOST:-mongodb}"
    local mongo_port="${MONGO_PORT:-27017}"
    
    local redis_host="${REDIS_HOST:-redis}"
    local redis_port="${REDIS_PORT:-6379}"
    
    local kafka_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
    local kafka_host=$(echo "$kafka_servers" | cut -d':' -f1)
    local kafka_port=$(echo "$kafka_servers" | cut -d':' -f2)
    
    # Wait for each service
    wait_for_service "PostgreSQL" "$postgres_host" "$postgres_port" || return 1
    wait_for_service "MongoDB" "$mongo_host" "$mongo_port" || return 1
    wait_for_service "Redis" "$redis_host" "$redis_port" || return 1
    wait_for_service "Kafka" "$kafka_host" "$kafka_port" || return 1
    
    # Additional wait for services to be fully ready
    log_info "Waiting additional 10 seconds for services to be fully ready..."
    sleep 10
    
    log_info "All dependencies are ready!"
}

# Run database migrations
run_migrations() {
    log_info "Running database migrations..."
    
    cd "$APP_DIR"
    
    # Run Alembic migrations for PostgreSQL
    if command -v alembic >/dev/null 2>&1; then
        log_info "Running Alembic migrations..."
        python -m alembic upgrade head || {
            log_error "Database migration failed"
            return 1
        }
        log_info "Database migrations completed successfully"
    else
        log_warn "Alembic not found, skipping migrations"
    fi
    
    return 0
}

# Load ML models
load_ml_models() {
    log_info "Loading ML models..."
    
    if [[ -d "$MODELS_DIR" ]]; then
        local model_count=$(find "$MODELS_DIR" -name "*.pkl" | wc -l)
        log_info "Found $model_count ML model files in $MODELS_DIR"
        
        if [[ $model_count -eq 0 ]]; then
            log_warn "No ML models found. Creating sample model..."
            # Create a simple placeholder model for testing
            python3 -c "
import pickle
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_regression

# Create sample model
X, y = make_regression(n_samples=100, n_features=4, noise=0.1, random_state=42)
model = RandomForestRegressor(n_estimators=10, random_state=42)
model.fit(X, y)

# Save model
model_path = os.path.join('$MODELS_DIR', 'sample_regression_model.pkl')
with open(model_path, 'wb') as f:
    pickle.dump(model, f)

print(f'Created sample model: {model_path}')
" || log_warn "Failed to create sample model"
        fi
    else
        log_warn "Models directory not found: $MODELS_DIR"
    fi
}

# Initialize MongoDB collections and indexes
init_mongodb() {
    log_info "Initializing MongoDB collections and indexes..."
    
    python3 -c "
import asyncio
import sys
sys.path.append('$APP_DIR')

from motor.motor_asyncio import AsyncIOMotorClient
from app.models.database_models import MONGODB_COLLECTIONS, MONGODB_INDEXES
import os

async def init_mongo():
    mongo_url = os.getenv('MONGODB_URL')
    client = AsyncIOMotorClient(mongo_url)
    db = client.dss_streaming
    
    # Create collections and indexes
    for collection_name, indexes in MONGODB_INDEXES.items():
        collection = db[collection_name]
        
        # Create indexes
        for index in indexes:
            try:
                await collection.create_index(index)
                print(f'Created index for {collection_name}: {index}')
            except Exception as e:
                print(f'Index creation warning for {collection_name}: {e}')
    
    await client.close()
    print('MongoDB initialization completed')

asyncio.run(init_mongo())
" || log_warn "MongoDB initialization failed"
}

# Health check
health_check() {
    log_info "Performing health check..."
    
    local max_attempts=10
    local attempt=1
    
    # Wait for API to be ready
    while [[ $attempt -le $max_attempts ]]; do
        if curl -f http://localhost:8000/health >/dev/null 2>&1; then
            log_info "Health check passed!"
            return 0
        fi
        
        log_debug "Health check attempt $attempt/$max_attempts failed, retrying..."
        sleep 3
        ((attempt++))
    done
    
    log_error "Health check failed after $max_attempts attempts"
    return 1
}

# Start background services
start_background_services() {
    log_info "Starting background services..."
    
    # Start Celery worker if enabled
    if [[ "${CELERY_BROKER_URL:-}" ]] && [[ "${ENABLE_SCHEDULED_JOBS:-false}" == "true" ]]; then
        log_info "Starting Celery worker..."
        celery -A app.background.celery_app worker --loglevel=info --detach \
            --pidfile=/tmp/celery_worker.pid \
            --logfile="$LOG_DIR/celery_worker.log" || log_warn "Failed to start Celery worker"
        
        # Start Celery beat scheduler
        log_info "Starting Celery beat scheduler..."
        celery -A app.background.celery_app beat --loglevel=info --detach \
            --pidfile=/tmp/celery_beat.pid \
            --logfile="$LOG_DIR/celery_beat.log" || log_warn "Failed to start Celery beat"
    fi
}

# Cleanup function for graceful shutdown
cleanup() {
    log_info "Received shutdown signal, cleaning up..."
    
    # Stop background services
    if [[ -f /tmp/celery_worker.pid ]]; then
        log_info "Stopping Celery worker..."
        kill "$(cat /tmp/celery_worker.pid)" 2>/dev/null || true
        rm -f /tmp/celery_worker.pid
    fi
    
    if [[ -f /tmp/celery_beat.pid ]]; then
        log_info "Stopping Celery beat scheduler..."
        kill "$(cat /tmp/celery_beat.pid)" 2>/dev/null || true
        rm -f /tmp/celery_beat.pid
    fi
    
    log_info "Cleanup completed"
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Main execution
main() {
    log_info "Starting $APP_NAME..."
    log_info "Environment: ${ENVIRONMENT:-development}"
    log_info "Debug mode: ${DEBUG:-false}"
    
    # Create directories
    create_directories || {
        log_error "Failed to create directories"
        exit 1
    }
    
    # Check environment
    check_environment || {
        log_error "Environment check failed"
        exit 1
    }
    
    # Wait for dependencies
    wait_for_dependencies || {
        log_error "Dependencies not ready"
        exit 1
    }
    
    # Run migrations
    run_migrations || {
        log_error "Migration failed"
        exit 1
    }
    
    # Initialize MongoDB
    init_mongodb || log_warn "MongoDB initialization had warnings"
    
    # Load ML models
    load_ml_models || log_warn "ML model loading had warnings"
    
    # Start background services
    start_background_services || log_warn "Some background services failed to start"
    
    log_info "Pre-startup checks completed successfully!"
    log_info "Starting FastAPI application..."
    
    # Determine startup command based on environment
    if [[ "${ENVIRONMENT:-development}" == "production" ]]; then
        # Production: Use Gunicorn with Uvicorn workers
        log_info "Starting in production mode with Gunicorn..."
        exec gunicorn app.main:app \
            --worker-class uvicorn.workers.UvicornWorker \
            --workers "${WORKER_PROCESSES:-4}" \
            --bind 0.0.0.0:8000 \
            --timeout "${REQUEST_TIMEOUT:-300}" \
            --keepalive "${KEEP_ALIVE_TIMEOUT:-65}" \
            --max-requests 1000 \
            --max-requests-jitter 100 \
            --preload \
            --access-logfile "$LOG_DIR/access.log" \
            --error-logfile "$LOG_DIR/error.log" \
            --log-level "${LOG_LEVEL:-info}"
    else
        # Development: Use Uvicorn with reload
        log_info "Starting in development mode with Uvicorn..."
        exec uvicorn app.main:app \
            --host 0.0.0.0 \
            --port 8000 \
            --reload \
            --reload-dir /app/app \
            --log-level "${LOG_LEVEL:-info}"
    fi
}

# Run main function
main "$@"