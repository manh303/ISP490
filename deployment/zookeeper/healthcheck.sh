#!/bin/bash
set -e

# Check if Zookeeper is responding
check_zookeeper() {
    local port=${ZOOKEEPER_CLIENT_PORT:-2181}
    
    # Test 1: Check if port is open
    if ! nc -z localhost $port; then
        echo "Zookeeper port $port is not accessible"
        return 1
    fi
    
    # Test 2: Send 'ruok' command (Are you OK?)
    local response=$(echo 'ruok' | nc localhost $port 2>/dev/null || echo "")
    if [ "$response" != "imok" ]; then
        echo "Zookeeper is not responding correctly. Response: '$response'"
        return 1
    fi
    
    # Test 3: Check server status
    local status=$(echo 'stat' | nc localhost $port 2>/dev/null | head -1 || echo "")
    if [[ ! "$status" =~ "Zookeeper version" ]]; then
        echo "Zookeeper status check failed. Status: '$status'"
        return 1
    fi
    
    echo "Zookeeper is healthy"
    return 0
}

# Perform health check
if check_zookeeper; then
    exit 0
else
    exit 1
fi