# Deploy Airflow to Google Cloud Composer

## Setup Steps:

### 1. Enable Cloud Composer API
```bash
gcloud services enable composer.googleapis.com
```

### 2. Create Composer Environment
```bash
gcloud composer environments create ecommerce-dss-airflow \
    --location=asia-southeast1 \
    --python-version=3 \
    --node-count=3 \
    --disk-size=20GB \
    --machine-type=e2-medium
```

### 3. Upload DAGs
```bash
gsutil cp -r ./dags/* gs://[COMPOSER_BUCKET]/dags/
```

### 4. Set Airflow Variables
```bash
gcloud composer environments run ecommerce-dss-airflow \
    --location asia-southeast1 \
    variables set -- MONGODB_URI "your-mongo-connection"

gcloud composer environments run ecommerce-dss-airflow \
    --location asia-southeast1 \
    variables set -- KAFKA_SERVERS "your-kafka-servers"
```

### 5. Install Python Packages
```bash
gcloud composer environments update ecommerce-dss-airflow \
    --location asia-southeast1 \
    --update-pypi-packages-from-file requirements.txt
```