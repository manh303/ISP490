# deployment/kafka/Dockerfile
FROM confluentinc/cp-kafka:7.4.0

USER root

RUN yum update -y && \
    yum install -y curl nc procps && \
    yum clean all

# Copy custom server.properties to REPLACE default config
COPY server.properties /etc/kafka/server.properties

# Copy health check scripts
COPY healthcheck.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/healthcheck.sh

# Set ownership
RUN chown -R appuser:appuser /etc/kafka/ /usr/local/bin/

USER appuser

EXPOSE 9092 9997 29092

# Use the default path (now contains our custom config)
CMD ["kafka-server-start", "/etc/kafka/server.properties"]