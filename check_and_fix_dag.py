import subprocess
import json

# Get task state
result = subprocess.run(
    'docker exec ecommerce-dss-project-airflow-webserver-1 airflow tasks state tiki_lazada_pipeline load_to_stg "2025-11-12T10:00:00+00:00"',
    shell=True, capture_output=True, text=True
)

print("Task load_to_stg state:", result.stdout.strip())

# Clear the stuck task
print("\nClearing stuck task...")
clear_result = subprocess.run(
    'docker exec ecommerce-dss-project-airflow-webserver-1 airflow tasks clear tiki_lazada_pipeline -t load_to_stg -s "2025-11-12T10:00:00+00:00" -e "2025-11-12T10:00:00+00:00" -y',
    shell=True, capture_output=True, text=True
)

print(clear_result.stdout)
print("\nTask cleared. It will restart automatically.")
