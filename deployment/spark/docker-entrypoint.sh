#!/usr/bin/env bash
set -euo pipefail

: "${SPARK_MODE:=master}"

if [ "${SPARK_MODE}" = "master" ]; then
  : "${SPARK_MASTER_HOST:=0.0.0.0}"
  : "${SPARK_MASTER_PORT:=7077}"
  : "${SPARK_MASTER_WEBUI_PORT:=8080}"
  exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST}" \
      --port "${SPARK_MASTER_PORT}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT}"
else
  : "${SPARK_MASTER_URL:=spark://spark-master:7077}"
  : "${SPARK_WORKER_CORES:=2}"
  : "${SPARK_WORKER_MEMORY:=2g}"
  : "${SPARK_WORKER_WEBUI_PORT:=8081}"
  exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --cores "${SPARK_WORKER_CORES}" \
      --memory "${SPARK_WORKER_MEMORY}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
      "${SPARK_MASTER_URL}"
fi
