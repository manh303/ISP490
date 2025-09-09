FROM postgres:15-alpine

# Install additional extensions
RUN apk add --no-cache postgresql-contrib

# Copy initialization scripts
COPY init.sql /docker-entrypoint-initdb.d/

# Set default database
ENV POSTGRES_DB=ecommerce_dss
ENV POSTGRES_USER=dss_user
ENV POSTGRES_PASSWORD=dss_password_123

# Expose port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
    CMD pg_isready -U $POSTGRES_USER -d $POSTGRES_DB