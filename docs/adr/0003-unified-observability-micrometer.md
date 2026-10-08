# ADR 0003: Unified Observability & Telemetry via WebTransportMetricsListener

## Status
Accepted

## Context
Production enterprise deployments require rich telemetry: OpenTelemetry traces, Prometheus metrics, and Datadog counters. Directly hardcoding a specific monitoring framework (such as Micrometer or OpenTelemetry SDK) into the core library introduces heavy transitive dependencies, classpath conflicts, and version lock-in.

## Decision
We established a lightweight, zero-dependency SPI: `WebTransportMetricsListener`:
1. Core events defined:
   - `onSessionOpened(long sessionId, String path)`
   - `onSessionClosed(long sessionId, String path, int closeCode)`
   - `onSessionRejected(String reason)`
   - `onDatagramReceived(long sessionId, int byteLength)`
   - `onDatagramSent(long sessionId, int byteLength)`
   - `onDatagramDiscarded(long sessionId, String reason)`
   - `onConnectionMigration(long sessionId, String oldAddress, String newAddress)`
2. The default implementation is `NoOpWebTransportMetricsListener`, which incurs zero overhead.
3. Bridge modules (e.g. `WebTransportMicrometerMetricsListener`, `WebTransportOtlpMetricsListener`) adapt these calls to Prometheus, Grafana, OpenTelemetry, and Datadog registries.

## Consequences

### Positive
- **Zero Core Dependency Bloat**: Core JAR remains lightweight without bulky monitoring SDKs.
- **Microsecond Execution**: Callback invocation consists of a single polymorphic interface dispatch.
- **Enterprise Integration**: Seamlessly maps to Spring Boot Actuator, Micrometer, and Cloud-Native telemetry pipelines.

### Negative / Trade-offs
- Users who need metrics must provide an implementation or configure an adapter listener on the server builder.
