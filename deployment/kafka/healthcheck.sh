#!/bin/bash
set -e

# Wait for Kafka to be ready
timeout=30
count=0

while [ $count -lt $timeout ]; do
    if kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo "Kafka is ready!"
        exit 0
    fi
    echo "Waiting for Kafka to be ready... ($count/$timeout)"
    sleep 2
    count=$((count + 1))
done

echo "Kafka failed to start within $timeout seconds"
exit 1