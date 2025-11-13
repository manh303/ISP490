# GitHub Actions Workflows

## Available Workflows

### 1. Deploy Airflow (`deploy-airflow.yaml`)
Automatically deploys Airflow when changes are pushed to `airflow/` or `data-pipeline/` directories.

**Triggers:**
- Push to `main` or `production` branch
- Manual trigger via workflow_dispatch

**Steps:**
1. Build Airflow Docker image
2. Push to GitHub Container Registry
3. Deploy to server via SSH
4. Health check
5. Slack notification

### 2. CI/CD Pipeline (`ci-pipeline.yaml`)
Runs tests, linting, and deployment on every push.

**Jobs:**
- **test**: Run pytest with coverage
- **lint**: Run flake8 linting
- **build**: Build Docker images and run integration tests
- **deploy**: Deploy to production (main branch only)

### 3. Deploy Full Stack (`deploy-full-stack.yaml`)
Manual deployment of entire stack to chosen environment.

**Environments:**
- development
- staging
- production

**Steps:**
1. Set environment variables
2. Copy files to server
3. Deploy all services
4. Run database migrations
5. Notify via Slack

## Setup Instructions

### 1. Required Secrets

Add these secrets in GitHub Settings → Secrets and variables → Actions:

```
SERVER_HOST=your-server-ip
SERVER_USER=ubuntu
SSH_PRIVATE_KEY=your-ssh-private-key
DB_HOST=your-db-host
DB_NAME=ecommerce_dss
DB_USER=dss_user
DB_PASSWORD=your-db-password
JWT_SECRET_KEY=your-jwt-secret
AIRFLOW_FERNET_KEY=your-fernet-key
AIRFLOW_URL=https://airflow.yourdomain.com
SLACK_WEBHOOK=your-slack-webhook-url
```

### 2. Generate SSH Key

```bash
# On your local machine
ssh-keygen -t ed25519 -C "github-actions"

# Copy public key to server
ssh-copy-id -i ~/.ssh/id_ed25519.pub user@server

# Copy private key to GitHub Secrets
cat ~/.ssh/id_ed25519
```

### 3. Generate Airflow Fernet Key

```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

### 4. Server Setup

```bash
# On server
sudo mkdir -p /opt/ecommerce-dss
sudo chown $USER:$USER /opt/ecommerce-dss
cd /opt/ecommerce-dss

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

## Usage

### Deploy Airflow Only
```bash
# Automatic on push to main
git push origin main

# Or manual trigger
# Go to Actions → Deploy Airflow → Run workflow
```

### Deploy Full Stack
```bash
# Go to Actions → Deploy Full Stack → Run workflow
# Select environment: development/staging/production
```

### Run CI/CD Pipeline
```bash
# Automatic on every push/PR
git push origin develop
```

## Monitoring

### Check Workflow Status
- GitHub → Actions tab
- View logs for each step
- Check deployment status

### Health Checks
```bash
# Backend
curl https://api.yourdomain.com/health

# Airflow
curl https://airflow.yourdomain.com/health
```

## Rollback

```bash
# SSH to server
ssh user@server

# Rollback to previous version
cd /opt/ecommerce-dss
git log --oneline
git checkout <previous-commit>
docker-compose up -d --build
```

## Troubleshooting

### Deployment Failed
1. Check GitHub Actions logs
2. SSH to server and check logs:
   ```bash
   docker-compose logs backend
   docker-compose logs airflow-webserver
   ```

### Health Check Failed
```bash
# Check service status
docker-compose ps

# Restart services
docker-compose restart backend airflow-webserver
```

### Database Migration Failed
```bash
# Manual migration
docker-compose exec backend alembic upgrade head
docker-compose exec airflow-webserver airflow db upgrade
```
