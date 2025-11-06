@echo off
REM E-commerce DSS Data Standardization Pipeline Runner - Local Version
REM Adapted for current Docker setup

echo 🚀 Starting E-commerce DSS Data Standardization Pipeline (Local)...

REM Check if Docker is running
docker --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker is not running or not installed
    exit /b 1
)

REM Check if Spark cluster is running
echo 🔍 Checking Spark cluster availability...
docker exec spark-master echo "Spark master accessible" >nul 2>&1
if errorlevel 1 (
    echo ❌ Spark cluster is not accessible
    echo Please make sure Spark containers are running:
    echo docker-compose up -d
    exit /b 1
) else (
    echo ✅ Spark cluster is running
)

REM Check current directory structure in Spark container
echo 📂 Checking container structure...
docker exec spark-master ls -la

REM Create directories in container
echo 📁 Creating required directories...
docker exec spark-master mkdir -p /app/raw_data
docker exec spark-master mkdir -p /app/processed_data
docker exec spark-master mkdir -p /app/scripts

REM Copy raw data from local to container
echo 📋 Copying raw data to container...
docker cp "%cd%\..\..\data-collection\outputs\fixed_lazada_products.json" spark-master:/app/raw_data/
docker cp "%cd%\..\..\data-collection\outputs\fptshop_complete_1762194191.json" spark-master:/app/raw_data/
docker cp "%cd%\..\..\data-collection\outputs\cellphones_test_1762077170.jsonl" spark-master:/app/raw_data/

REM Copy the standardization script to container
echo 📋 Copying standardization script...
docker cp "%cd%\standardization_pipeline_local.py" spark-master:/app/scripts/

REM Install Python dependencies
echo 📦 Installing Python dependencies...
docker exec spark-master pip install --quiet pyspark pandas

REM Run the standardization pipeline
echo 🔥 Executing standardization pipeline...
docker exec spark-master python /app/scripts/standardization_pipeline_local.py

if errorlevel 1 (
    echo ❌ Data standardization pipeline failed!
    echo 📋 Checking logs...
    docker exec spark-master ls -la /app/
    exit /b 1
) else (
    echo ✅ Data standardization pipeline completed successfully!

    REM Show output files
    echo 📊 Generated output files:
    docker exec spark-master ls -la /app/processed_data/

    REM Copy results back to local
    echo 📥 Copying results back to local...
    if not exist "%cd%\..\..\data\processed\" mkdir "%cd%\..\..\data\processed\"
    docker cp spark-master:/app/processed_data/. "%cd%\..\..\data\processed\"
)

echo 🎉 Pipeline execution completed!
pause