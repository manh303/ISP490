# Airflow Setup Guide - ML Training Pipeline

## Quick Start (5 minutes)

### 1. Create Network (if not exists)

```bash
docker network create ecommerce-network
```

### 2. Start Airflow Stack

```bash
cd /c/DoAn_FPT_FALL2025/ecommerce-dss-project
docker-compose -f docker-compose.ml-airflow.yml up -d
```

### 3. Initialize Database

```bash
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow db init
```

### 4. Create Admin User

```bash
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow users create \
  --username admin \
  --password admin123 \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@ecommerce.com
```

### 5. Access Web UI

Open browser: http://localhost:8080
- Username: admin
- Password: admin123

---

## Detailed Setup

### Step 1: Prerequisites

```bash
# Check Docker installed
docker --version

# Check docker-compose installed
docker-compose --version

# Check network exists
docker network ls | grep ecommerce-network
```

### Step 2: Configuration

Create `.env` file in project root:

```env
# Airflow
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags

# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Redis
REDIS_HOST=redis-airflow
REDIS_PORT=6379

# Python
PYTHONPATH=/opt/airflow:/app/ml
```

### Step 3: Build & Start

```bash
# Create required directories
mkdir -p airflow/dags airflow/logs airflow/plugins
mkdir -p ml/logs/metrics ml/data

# Start all services
docker-compose -f docker-compose.ml-airflow.yml up -d

# Check status
docker-compose -f docker-compose.ml-airflow.yml ps
```

### Step 4: Verify Services

```bash
# Check Webserver (should return health status)
curl http://localhost:8080/health

# Check Scheduler logs
docker-compose -f docker-compose.ml-airflow.yml logs airflow-scheduler | tail -20

# Check Worker status
docker-compose -f docker-compose.ml-airflow.yml logs airflow-worker | tail -20
```

### Step 5: Configure Connections

#### Option A: Via Web UI
1. Go to Admin → Connections
2. Click "Create" → New Connection
3. Configure PostgreSQL:
   - Connection ID: `postgres_default`
   - Connection Type: PostgreSQL
   - Host: `dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com`
   - Port: 5432
   - Database: `ecommerce_dss_1`
   - User: `dss_user`
   - Password: [your password]

#### Option B: Via CLI
```bash
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow connections add postgres_default \
  --conn-type postgresql \
  --conn-host dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com \
  --conn-login dss_user \
  --conn-password [password] \
  --conn-port 5432 \
  --conn-schema ecommerce_dss_1
```

### Step 6: Verify DAGs

```bash
# List all DAGs
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags list

# Parse specific DAG
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags show ml_training_pipeline

# Validate DAG
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags test ml_training_pipeline 2025-11-16
```

---

## Services

### Airflow Webserver
- **URL**: http://localhost:8080
- **Port**: 8080
- **Purpose**: Web UI for managing DAGs and monitoring runs
- **Health Check**: `curl http://localhost:8080/health`

### Airflow Scheduler
- **Container**: airflow-scheduler
- **Purpose**: Schedules DAG runs and manages task execution
- **Logs**: `docker logs airflow-scheduler`

### Airflow Worker (Celery)
- **Container**: airflow-worker
- **Purpose**: Executes tasks from Celery queue
- **Logs**: `docker logs airflow-worker`

### Flower (Celery Monitoring)
- **URL**: http://localhost:5555
- **Purpose**: Monitor Celery workers and task execution
- **Stats**: Real-time task monitoring

### PostgreSQL
- **Host**: localhost
- **Port**: 5433 (to avoid conflict with DWH)
- **Database**: airflow
- **Purpose**: Stores Airflow metadata and ML results

### Redis
- **Host**: localhost
- **Port**: 6379
- **Purpose**: Celery message broker

---

## DAG Triggers

### Manual Trigger

```bash
# Via CLI
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags trigger ml_training_pipeline

# With config
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags trigger ml_training_pipeline \
  --conf '{"lookback_days": 30}'
```

### Schedule Trigger (Automatic)

DAGs run automatically per schedule:
- `ml_training_pipeline`: 1:00 AM daily
- `ml_monitoring_dag`: 2:00 AM daily

Check "Schedule" column in Airflow UI.

---

## Monitoring

### Web UI Monitoring

1. Open http://localhost:8080
2. Click on DAG name to see:
   - DAG runs (success/fail history)
   - Task details
   - Logs
   - XCom (task communication data)

### Command Line Monitoring

```bash
# List recent runs
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags list-runs ml_training_pipeline

# Get DAG run status
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags list-runs ml_training_pipeline --state success

# View task logs
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow tasks logs ml_training_pipeline extract_data 2025-11-16

# Get XCom values
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow tasks xcom get ml_training_pipeline get_model_metrics model_metrics
```

### Flower Monitoring (Celery)

Open http://localhost:5555 to see:
- Worker status
- Task execution history
- Task queue statistics

---

## Troubleshooting

### Issue: Services won't start

```bash
# Check logs
docker-compose -f docker-compose.ml-airflow.yml logs

# Check specific service
docker-compose -f docker-compose.ml-airflow.yml logs airflow-webserver

# Reset everything
docker-compose -f docker-compose.ml-airflow.yml down -v
docker-compose -f docker-compose.ml-airflow.yml up -d
```

### Issue: DAG not showing

```bash
# Check DAG is in correct folder
ls -la airflow/dags/ml_*.py

# Validate DAG syntax
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  python -m py_compile airflow/dags/ml_training_pipeline_dag.py

# Reload DAGs
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags list
```

### Issue: Task fails - connection error

```bash
# Test connection
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow connections test postgres_default

# Check credentials
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow connections get postgres_default
```

### Issue: High memory usage

```bash
# Limit worker concurrency
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow config set core max_active_tasks_per_dag 2

# Check container stats
docker stats
```

### Issue: Database initialization failed

```bash
# Recreate database
docker-compose -f docker-compose.ml-airflow.yml exec postgres-airflow \
  psql -U airflow -d airflow -c "DROP DATABASE airflow; CREATE DATABASE airflow;"

# Re-init
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow db init
```

---

## Common Commands

```bash
# View all DAGs
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags list

# Pause/unpause DAG
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags pause ml_training_pipeline

docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags unpause ml_training_pipeline

# Delete DAG
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags delete ml_training_pipeline

# Clear task instances
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow tasks clear ml_training_pipeline -sd 2025-11-15

# Backfill historical runs
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow dags backfill ml_training_pipeline \
  --start-date 2025-11-01 \
  --end-date 2025-11-15

# Test individual task
docker-compose -f docker-compose.ml-airflow.yml exec airflow-webserver \
  airflow tasks test ml_training_pipeline extract_data 2025-11-16
```

---

## Stopping Services

```bash
# Stop all services
docker-compose -f docker-compose.ml-airflow.yml stop

# Stop and remove containers
docker-compose -f docker-compose.ml-airflow.yml down

# Stop and remove everything (including volumes)
docker-compose -f docker-compose.ml-airflow.yml down -v
```

---

## Production Considerations

### 1. Security

```yaml
# Update airflow/config/airflow.cfg
[core]
expose_config = False
dags_are_paused_at_creation = True
auth_backend = airflow.contrib.auth.backends.password_auth

[webserver]
authenticate = True
auth_backend = airflow.contrib.auth.backends.password_auth
```

### 2. Persistence

```bash
# Backup databases
docker-compose exec postgres-airflow pg_dump -U airflow airflow > airflow_backup.sql

# Backup DAGs
tar -czf dags_backup.tar.gz airflow/dags/

# Backup logs
tar -czf logs_backup.tar.gz airflow/logs/
```

### 3. Monitoring & Alerts

```yaml
# Configure Slack alerts in DAG:
on_failure_callback = slack_notify_on_failure
on_success_callback = slack_notify_on_success
```

### 4. Resource Limits

```yaml
# In docker-compose.yml
services:
  airflow-worker:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```

---

## Advanced Topics

### Custom Operators

Place in `airflow/plugins/operators/`

```python
from airflow.models.baseoperator import BaseOperator

class MLTrainingOperator(BaseOperator):
    def __init__(self, model_type, **kwargs):
        super().__init__(**kwargs)
        self.model_type = model_type
    
    def execute(self, context):
        # Your training logic here
        pass
```

### Custom Hooks

Place in `airflow/plugins/hooks/`

```python
from airflow.hooks.base import BaseHook

class MLHook(BaseHook):
    def get_trained_model(self, model_name):
        # Load model
        pass
```

### Variables & Secrets

```bash
# Set variable
docker-compose exec airflow-webserver \
  airflow variables set key value

# Get variable
docker-compose exec airflow-webserver \
  airflow variables get key
```

---

## Integration with External Systems

### Slack Notifications

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

slack_alert = SlackWebhookOperator(
    task_id='slack_alert',
    http_conn_id='slack_webhook',
    message='ML Training Complete!'
)
```

### Email Notifications

```python
from airflow.operators.email import EmailOperator

email_alert = EmailOperator(
    task_id='email_alert',
    to='admin@ecommerce.com',
    subject='ML Training Report',
    html_content='<h1>Training Complete</h1>'
)
```

### Database Integration

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

db_log = PostgresOperator(
    task_id='log_to_db',
    postgres_conn_id='postgres_default',
    sql='INSERT INTO ml_logs (status) VALUES (%(status)s)',
    parameters={'status': 'completed'}
)
```

---

## Next Steps

1. ✅ Start Airflow stack: `docker-compose -f docker-compose.ml-airflow.yml up -d`
2. ✅ Access Web UI: http://localhost:8080 (admin/admin123)
3. ✅ Add PostgreSQL connection
4. ✅ Verify DAGs loaded
5. ✅ Trigger `ml_training_pipeline` manually
6. ✅ Monitor execution in Flower (http://localhost:5555)
7. ✅ Check logs and metrics

---

## Support

- **Airflow Docs**: https://airflow.apache.org/docs/
- **ML Training Guide**: `/app/ml/TRAINING_GUIDE.md`
- **DAG README**: `/app/airflow/README_ML_DAG.md`

**Status**: Ready for Production ✅

Last Updated: 2025-11-16
