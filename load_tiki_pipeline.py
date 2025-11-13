import subprocess
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def run_cmd(desc, cmd):
    print(f"\n{'='*70}")
    print(f"STEP: {desc}")
    print(f"{'='*70}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    if result.returncode != 0:
        print(f"FAILED with exit code {result.returncode}")
        return False
    print("SUCCESS")
    return True

print("TIKI DATA PIPELINE - LOAD TO DATABASE")
print("="*70)

# Step 1: Load to staging
if not run_cmd(
    "1. Load JSONL files to staging table",
    "docker exec ecommerce-dss-project-airflow-worker-1 python /app/src/staging/load_raw_data.py"
):
    sys.exit(1)

# Step 2: Transform to ODS
if not run_cmd(
    "2. Transform staging to ODS",
    """docker exec spark-master spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/jars/postgresql-42.7.1.jar \
      /app/src/spark_jobs/ods_transformation.py \
      --pg-url jdbc:postgresql://dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com:5432/ecommerce_dss \
      --pg-user dss_user \
      --pg-pass IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4"""
):
    sys.exit(1)

print("\n" + "="*70)
print("PIPELINE COMPLETED SUCCESSFULLY!")
print("="*70)
