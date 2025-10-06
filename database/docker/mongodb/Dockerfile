FROM mongo:7

# Copy initialization script and configuration
COPY init.js /docker-entrypoint-initdb.d/
COPY mongod.conf /etc/mongod.conf

# Set environment variables
ENV MONGO_INITDB_DATABASE=ecommerce_dss
ENV MONGO_INITDB_ROOT_USERNAME=admin
ENV MONGO_INITDB_ROOT_PASSWORD=admin_password

# Performance tuning environment variables
ENV MONGO_OPLOG_SIZE=2048
ENV MONGO_CACHE_SIZE_GB=2

# Create directories for optimized storage
RUN mkdir -p /data/db /data/logs

# Expose port
EXPOSE 27017

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
    CMD mongosh --eval "db.adminCommand('ping')"

# Use custom configuration
CMD ["mongod", "--config", "/etc/mongod.conf"]