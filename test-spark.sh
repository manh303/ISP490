#!/bin/bash
# Test Spark container separately

echo "Testing Spark Master container..."

# Build Spark image
docker-compose build spark-master

# Start only Spark master
docker-compose up -d spark-master

# Wait and check logs
echo "Waiting for Spark Master to start..."
sleep 30

echo "Checking Spark Master logs:"
docker-compose logs spark-master

echo "Checking Spark Master health:"
docker-compose ps spark-master

echo "Testing healthcheck manually:"
docker-compose exec spark-master /usr/local/bin/healthcheck.sh

echo "Done!"
