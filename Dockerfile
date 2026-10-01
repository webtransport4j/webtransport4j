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
RUN mvn clean package -DskipTests dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib

# Stage 2: Hardened Production Runtime
FROM eclipse-temurin:21-jre-alpine AS runtime

LABEL org.opencontainers.image.title="WebTransport4J Cluster Node" \
      org.opencontainers.image.description="High-performance HTTP/3 WebTransport cluster node with stateless QUIC resumption" \
      org.opencontainers.image.licenses="Apache-2.0"

# Create unprivileged application user
RUN addgroup -S wtgroup && adduser -S wtuser -G wtgroup
WORKDIR /app

# Copy application artifacts from builder
COPY --from=builder /build/target/webtransport4j-*.jar /app/webtransport4j.jar
COPY --from=builder /build/target/lib /app/lib

RUN chown -R wtuser:wtgroup /app
USER wtuser

# Expose QUIC/WebTransport (UDP) and Health/Metrics (TCP)
EXPOSE 4433/udp
EXPOSE 8080/tcp

# JVM Ergonomics for High-Throughput QUIC Server
ENV JAVA_OPTS="-XX:+UseG1GC \
               -XX:MaxRAMPercentage=75.0 \
               -XX:+ExitOnOutOfMemoryError \
               -Dio.netty.leakDetection.level=SIMPLE"

ENV PORT=4433
ENV METRICS_PORT=8080
ENV POD_NAME="webtransport-node"

# Health check using auxiliary HTTP probe
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8080/healthz || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp '/app/webtransport4j.jar:/app/lib/*' io.github.webtransport4j.example.ClusterNodeSample"]
