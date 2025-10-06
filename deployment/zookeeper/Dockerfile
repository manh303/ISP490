FROM confluentinc/cp-zookeeper:7.4.0

# Switch to root for package installation
USER root

# Install additional utilities
RUN yum update -y && \
    yum install -y \
    curl \
    nc \
    procps \
    vim \
    && yum clean all

# Create custom config directory
RUN mkdir -p /etc/zookeeper-custom

# Copy custom configuration files
COPY zoo.cfg /etc/zookeeper-custom/
COPY myid /etc/zookeeper-custom/
COPY log4j.properties /etc/zookeeper-custom/

# Copy startup and health check scripts
COPY docker-entrypoint.sh /usr/local/bin/
COPY healthcheck.sh /usr/local/bin/

# Make scripts executable
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/healthcheck.sh

# Create data and log directories
RUN mkdir -p /var/lib/zookeeper/data /var/lib/zookeeper/log

# Set ownership
RUN chown -R appuser:appuser /var/lib/zookeeper /etc/zookeeper-custom /usr/local/bin/

# Switch back to appuser
USER appuser

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD /usr/local/bin/healthcheck.sh

# Expose port
EXPOSE 2181 2888 3888

# Use custom entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["zookeeper-server-start", "/etc/kafka/zookeeper.properties"]