"""Fix Spark job database environment variables in Airflow DAG"""

dag_file = r"c:\DoAn_FPT_FALL2025\ecommerce-dss-project\airflow\dags\minio_pipeline_dag.py"

# Read current content
with open(dag_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Find and replace the spark-submit command
old_submit = """  --conf spark.executor.memoryOverhead=512m \\
  --conf spark.driver.memoryOverhead=512m \\
  --jars /opt/spark/jars/postgresql-42.7.1.jar \\
  /app/src/spark_jobs/load_cleaned_from_minio.py"""

new_submit = """  --conf spark.executor.memoryOverhead=512m \\
  --conf spark.driver.memoryOverhead=512m \\
  --conf spark.executorEnv.DB_HOST=postgres \\
  --conf spark.executorEnv.DB_PORT=5432 \\
  --conf spark.executor Env.DB_NAME=ecommerce_dss \\
  --conf spark.executorEnv.DB_USER=dss_user \\
  --conf spark.executorEnv.DB_PASSWORD=dss_password_123 \\
  --conf spark.yarn.appMasterEnv.DB_HOST=postgres \\
  --conf spark.yarn.appMasterEnv.DB_PORT=5432 \\
  --conf spark.yarn.appMasterEnv.DB_NAME=ecommerce_dss \\
  --conf spark.yarn.appMasterEnv.DB_USER=dss_user \\
  --conf spark.yarn.appMasterEnv.DB_PASSWORD=dss_password_123 \\
  --jars /opt/spark/jars/postgresql-42.7.1.jar \\
  /app/src/spark_jobs/load_cleaned_from_minio.py"""

if old_submit in content:
    content = content.replace(old_submit, new_submit)
    with open(dag_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ Successfully updated Spark submit command with DB environment variables")
else:
    print("❌ Could not find the exact spark-submit command to replace")
    print("\nSearching for variations...")
    if "--jars /opt/spark/jars/postgresql-42.7.1.jar" in content:
        print("Found --jars line, but surrounding context doesn't match")
