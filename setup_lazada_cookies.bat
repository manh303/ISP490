@echo off
echo ========================================
echo LAZADA COOKIES SETUP
echo ========================================
echo.

REM Step 1: Generate cookies
echo Step 1: Generate cookies on local machine
echo.
cd data-collection\crawlers\lazada\runners
echo Run: python lazada_cookie_generator.py
echo After login in browser, press Enter
echo.
pause

REM Step 2: Copy to Docker
echo.
echo Step 2: Copying cookies to Docker...
docker exec ecommerce-dss-project-airflow-webserver-1 mkdir -p /tmp/profiles/lazada
docker cp lazada_cookies.json ecommerce-dss-project-airflow-webserver-1:/tmp/profiles/lazada/cookies.json

REM Step 3: Verify
echo.
echo Step 3: Verifying...
docker exec ecommerce-dss-project-airflow-webserver-1 ls -la /tmp/profiles/lazada/

echo.
echo ========================================
echo DONE! Cookies copied successfully
echo Now you can trigger the DAG in Airflow
echo ========================================
pause
