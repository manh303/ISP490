#!/bin/bash
set -e

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting Zookeeper custom entrypoint..."

# Set environment variables with defaults
export ZOOKEEPER_CLIENT_PORT=${ZOOKEEPER_CLIENT_PORT:-2181}
export ZOOKEEPER_TICK_TIME=${ZOOKEEPER_TICK_TIME:-2000}
export ZOOKEEPER_INITLIMIT=${ZOOKEEPER_INITLIMIT:-10}
export ZOOKEEPER_SYNCLIMIT=${ZOOKEEPER_SYNCLIMIT:-5}

# Create required directories
mkdir -p /var/lib/zookeeper/data
mkdir -p /var/lib/zookeeper/log

# Copy myid file if it doesn't exist
if [ ! -f /var/lib/zookeeper/data/myid ]; then
    log "Creating myid file..."
    echo "${ZOOKEEPER_SERVER_ID:-1}" > /var/lib/zookeeper/data/myid
fi

# Generate zookeeper.properties from template
log "Generating zookeeper.properties..."
cat > /etc/kafka/zookeeper.properties << EOF
tickTime=${ZOOKEEPER_TICK_TIME}
initLimit=${ZOOKEEPER_INITLIMIT}
syncLimit=${ZOOKEEPER_SYNCLIMIT}
dataDir=/var/lib/zookeeper/data
dataLogDir=/var/lib/zookeeper/log
clientPort=${ZOOKEEPER_CLIENT_PORT}
maxClientCnxns=60
autopurge.snapRetainCount=3
autopurge.purgeInterval=24
admin.enableServer=true
admin.serverPort=8080
4lw.commands.whitelist=*
EOF

# Add cluster configuration if multiple servers
if [ -n "$ZOOKEEPER_SERVERS" ]; then
    log "Adding cluster configuration..."
    echo "$ZOOKEEPER_SERVERS" >> /etc/kafka/zookeeper.properties
fi

log "Zookeeper configuration:"
cat /etc/kafka/zookeeper.properties

# Wait for dependencies if specified
if [ -n "$ZOOKEEPER_WAIT_FOR" ]; then
    log "Waiting for dependencies: $ZOOKEEPER_WAIT_FOR"
    for dep in $ZOOKEEPER_WAIT_FOR; do
        while ! nc -z $dep; do
            log "Waiting for $dep..."
            sleep 2
        done
        log "$dep is ready!"
    done
fi

log "Starting Zookeeper server..."

# Execute the original command
exec "$@"