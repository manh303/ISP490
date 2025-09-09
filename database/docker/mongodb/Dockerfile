FROM mongo:7

# Copy initialization script
COPY init.js /docker-entrypoint-initdb.d/

# Set environment variables
ENV MONGO_INITDB_DATABASE=ecommerce_dss
ENV MONGO_INITDB_ROOT_USERNAME=admin
ENV MONGO_INITDB_ROOT_PASSWORD=admin_password

# Expose port
EXPOSE 27017

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
    CMD mongosh --eval "db.adminCommand('ping')"