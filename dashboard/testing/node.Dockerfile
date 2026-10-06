# Isolated integration-test node and client image; not a production deployment.
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml ./
COPY src ./src
COPY config ./config
COPY scripts ./scripts
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -DskipTests -Dmaven.compiler.release=25 compile dependency:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory=target/lib

FROM eclipse-temurin:25-jre
RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/target/classes /app/classes
COPY --from=build /build/target/lib /app/lib
COPY dashboard/testing/verify_stack.py /tests/verify_stack.py
USER 10001:10001
ENV PYTHONUNBUFFERED=1
CMD ["java", "-XX:+UseZGC", "-XX:MaxRAMPercentage=60.0", "-cp", "/app/classes:/app/lib/*", "io.github.webtransport4j.example.ClusterNodeSample"]
