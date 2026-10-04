# ==============================================================================
# Multi-stage production container for WebTransport4J Clustered Node
# ==============================================================================

# Stage 1: Build & Dependency Resolution
FROM maven:3.9.9-eclipse-temurin-21-alpine AS builder
WORKDIR /build

COPY pom.xml ./
# Pre-fetch dependencies
RUN mvn dependency:go-offline -B || true

COPY src ./src
COPY scripts ./scripts

# Package Multi-Release JAR and gather runtime dependencies
RUN mvn clean package -DskipTests dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib \
    && cp "target/$(mvn -q help:evaluate -Dexpression=project.build.finalName -DforceStdout).jar" target/application.jar

# Stage 2: Hardened Production Runtime
FROM eclipse-temurin:25-jre AS runtime

LABEL org.opencontainers.image.title="WebTransport4J Cluster Node" \
      org.opencontainers.image.description="High-performance HTTP/3 WebTransport cluster node with stateless QUIC resumption and Generational ZGC" \
      org.opencontainers.image.licenses="Apache-2.0"

# Create unprivileged application user
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -r wtgroup && useradd -r -g wtgroup wtuser
WORKDIR /app

# Copy application artifacts from builder
COPY --from=builder /build/target/application.jar /app/webtransport4j.jar
COPY --from=builder /build/target/lib /app/lib
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

RUN chown -R wtuser:wtgroup /app
USER wtuser

# Expose QUIC/WebTransport (UDP) and Health/Metrics (TCP)
EXPOSE 4433/udp
EXPOSE 8080/tcp

# JVM Garbage Collection configuration from environment (defaults to ZGC on Java 25)
ENV JAVA_GC="ZGC"
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 \
               -XX:+ExitOnOutOfMemoryError \
               -Dio.netty.leakDetection.level=SIMPLE \
               -Dio.netty.allocator.type=pooled \
               -Djava.net.preferIPv4Stack=true"

ENV PORT=4433
ENV METRICS_PORT=8080
ENV POD_NAME="webtransport-node"

# Health check using auxiliary HTTP probe
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/docker-entrypoint.sh"]

