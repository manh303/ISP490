#!/bin/bash

# Simple healthcheck - just check if process is running
case "${SPARK_MODE:-master}" in
    "master")
        if pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null 2>&1; then
            exit 0
        fi
        ;;
    "worker")
        if pgrep -f "org.apache.spark.deploy.worker.Worker" > /dev/null 2>&1; then
            exit 0
        fi
        ;;
    "history-server")
        if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null 2>&1; then
            exit 0
        fi
        ;;
esac
exit 1