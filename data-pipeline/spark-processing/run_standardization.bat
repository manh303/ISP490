@echo off
REM E-commerce DSS Data Standardization Pipeline Runner for Windows
REM Runs the standardization pipeline in Spark Docker cluster

echo 🚀 Starting E-commerce DSS Data Standardization Pipeline...

REM Check if Docker is running
docker --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker is not running or not installed
    exit /b 1
)

REM Check if Spark cluster is running
echo 🔍 Checking Spark cluster availability...
docker exec spark-master curl -s http://spark-master:8080 >nul 2>&1
if errorlevel 1 (
    echo ❌ Spark cluster is not accessible
    echo Please make sure Spark containers are running:
    echo docker-compose up -d
    exit /b 1
) else (
    echo ✅ Spark cluster is running
)

REM Check required data files
echo 📂 Checking data availability...
docker exec spark-master ls /opt/spark/data/raw/fixed_lazada_products.json >nul 2>&1
if errorlevel 1 (
    echo ⚠️  Lazada data not found at /opt/spark/data/raw/fixed_lazada_products.json
) else (
    echo ✅ Lazada data found
)

REM Create output directory if not exists
echo 📁 Preparing output directory...
docker exec spark-master mkdir -p /opt/spark/data/processed

REM Install Python dependencies in Spark master
echo 📦 Installing Python dependencies...
docker exec spark-master pip install --quiet --no-cache-dir pyspark

REM Copy the standardization script to Spark master
echo 📋 Copying standardization script...
docker cp "%~dp0standardization_pipeline.py" spark-master:/opt/spark/apps/

REM Run the standardization pipeline
echo 🔥 Executing standardization pipeline...
docker exec spark-master python /opt/spark/apps/standardization_pipeline.py

if errorlevel 1 (
    echo ❌ Data standardization pipeline failed!
    exit /b 1
) else (
    echo ✅ Data standardization pipeline completed successfully!

    REM Show output files
    echo 📊 Generated output files:
    docker exec spark-master find /opt/spark/data/processed -name "*.json" -o -name "*.parquet" 2>nul | findstr /C:"standardized_products"

    REM Show latest report summary
    echo 📋 Latest processing report created
)

echo 🎉 Pipeline execution completed!
pause