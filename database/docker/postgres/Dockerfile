FROM postgres:15-alpine

# Install additional extensions
RUN apk add --no-cache postgresql-contrib

# Copy initialization scripts and configuration
COPY init.sql /docker-entrypoint-initdb.d/
COPY postgresql.conf /tmp/postgresql.conf

# Set default database
ENV POSTGRES_DB=ecommerce_dss
ENV POSTGRES_USER=dss_user
ENV POSTGRES_PASSWORD=dss_password_123

# Performance tuning environment variables
ENV POSTGRES_INITDB_ARGS="--data-checksums"
ENV POSTGRES_SHARED_BUFFERS=512MB
ENV POSTGRES_EFFECTIVE_CACHE_SIZE=1536MB
ENV POSTGRES_WORK_MEM=16MB
ENV POSTGRES_MAINTENANCE_WORK_MEM=128MB

# Copy custom PostgreSQL configuration
RUN echo "include_if_exists = '/tmp/postgresql.conf'" >> /usr/local/share/postgresql/postgresql.conf.sample

# Expose port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
    CMD pg_isready -U $POSTGRES_USER -d $POSTGRES_DB