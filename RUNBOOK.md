# WebTransport4J SRE Runbook & Operational Playbook

This runbook provides Site Reliability Engineers (SREs), DevOps engineers, and cluster operators with actionable procedures for monitoring, alerting, troubleshooting, and tuning `webtransport4j` in high-scale production environments.

---

## 1. Golden Signals & Key Metrics

Monitor the following core metrics exported via `WebTransportMetricsListener` (or Micrometer / Prometheus):

| Metric | Target / Threshold | Alert Severity | Description |
|---|---|---|---|
| `webtransport.sessions.active` | < 80% configured max | Warning | Active concurrent WebTransport sessions on node |
| `webtransport.sessions.rejected` | Rate > 1/min | Critical | Sessions shed or rejected by overload policy / rate limit |
| `webtransport.datagrams.discarded` | Rate > 0.01% of received | Warning / Critical | Dropped datagrams due to queue full or closed mailbox |
| `webtransport.migration.count` | Diagnostic counter | Info | QUIC path connection migration events |
| `jvm.memory.direct.bytes` | < 85% `-XX:MaxDirectMemorySize` | Critical | Netty off-heap direct buffer memory consumption |
| `quic.handshake.failure` | Rate > 5% of handshakes | Warning | Handshake errors (TLS mismatch, invalid token, timeout) |

---

## 2. Production Incident Playbooks

### Playbook 1: High Datagram Drops (`mailbox_full` or Kernel Drops)

#### Symptoms
- Clients observe high datagram packet loss.
- `webtransport.datagrams.discarded` metric increases with reason `mailbox_full`.
- Linux `netstat -su` or `ip -s link` shows `packet receive errors` or `buffer errors`.

#### Root Causes
1. Datagram ingestion rate exceeds the business executor processing throughput.
2. Kernel UDP receive buffer is too small for high packet arrival bursts.
3. Mailbox queue capacity limit reached.

#### Triage & Resolution Steps
1. **Check Kernel Drops**:
   ```bash
   netstat -su | grep "buffer errors"
   ```
2. **Increase Kernel Socket Limits**:
   Add to `/etc/sysctl.conf` and apply with `sysctl -p`:
   ```ini
   net.core.rmem_max = 16777216
   net.core.wmem_max = 16777216
   net.core.rmem_default = 4194304
   net.core.wmem_default = 4194304
   ```
3. **Scale WebTransport Mailbox Capacity**:
   Update `webtransport-dynamic.properties` or environment variables:
   ```properties
   webtransport4j.datagram.mailbox.capacity=2048
   webtransport4j.datagram.mailbox.batch_size=64
   ```
4. **Tune UDP Socket Buffer in Configuration**:
   ```properties
   webtransport4j.server.socket.rcvbuf=8388608
   webtransport4j.server.socket.sndbuf=8388608
   webtransport4j.server.socket.autotune=true
   ```

---

### Playbook 2: Sessions Rejected with HTTP 503 (`overload_shed`)

#### Symptoms
- Clients receive HTTP 503 `Service Unavailable` during `CONNECT`; the default adaptive policy supplies `Retry-After: 5`, while custom policies may omit it.
- Server logs: `Rejecting session: Overload policy shed load`.

#### Root Causes
1. JVM heap memory pressure exceeded `maxHeapUsageRatio` (default 85%).
2. Other active sessions and pending reservations reached the adaptive policy's `maxActiveSessions` ceiling.
3. `AdaptiveCircuitBreaker` tripped to `OPEN` after repeated capacity or heap-pressure failures.

#### Triage & Resolution Steps
1. **Inspect Heap and Garbage Collection**:
   ```bash
   jstat -gcutil <pid> 1000 10
   ```
   Check if GC pause times (G1GC / ZGC) or old gen occupancy are above 85%.
2. **Horizontal Pod Autoscaling (HPA)**:
   Ensure Kubernetes HPA triggers based on active session count:
   ```yaml
   metrics:
   - type: Pods
     pods:
       metric:
         name: webtransport_sessions_active
       target:
         type: AverageValue
         averageValue: "35000"
   ```
3. **Verify Downstream Latency**:
   If business executor is saturated, inspect thread pools using `jstack <pid> | grep -A 20 "VirtualThread"`.

---

### Global Session-Limit Rejections (HTTP 429)

When `webtransport4j.server.max_concurrent_sessions` is reached, global slot tracking rejects the request with HTTP 429 before the adaptive overload policy runs. A per-connection session limit also returns HTTP 429. Triage these separately from HTTP 503 adaptive shedding.

### Playbook 3: Off-Heap / Direct ByteBuf Memory Leaks

#### Symptoms
- OutOfMemoryError: `Direct buffer memory` or native process memory continuously expanding without garbage collector release.
- Warning in logs: `LEAK: ByteBuf.release() was not called before it's garbage-collected`.

#### Triage & Resolution Steps
1. **Enable Netty Resource Leak Detector**:
   Set JVM property:
   ```bash
   -Dio.netty.leakDetection.level=PARANOID
   ```
2. **Analyze Stack Traces**:
   Review server logs for Netty leak traces highlighting which stream or datagram buffer was retained without release.
3. **Ensure Safe Frame Lifecycle**:
   Every custom `WebTransportHandler` must release consumed buffers or pass them to reactive pipelines that handle consumption.

---

### Playbook 4: TLS Certificate Expiry & Hot-Reload Failure

#### Symptoms
- Client handshakes fail with `SSLHandshakeException` or `certificate expired`.
- Server logs: `TlsCertificateWatcher` error during file modification detection.

#### Triage & Resolution Steps
1. **Inspect Certificate on Disk**:
   ```bash
   openssl x509 -in /etc/tls/cert.pem -noout -dates
   ```
2. **Force Hot-Reload**:
   Touch the certificate file to trigger file-watcher reload:
   ```bash
   touch /etc/tls/cert.pem
   ```
3. **Verify Server Log**:
   Confirm log entry: `TLS certificate and private key successfully reloaded`.

---

## 3. Recommended Prometheus Alerting Rules

```yaml
groups:
- name: webtransport4j.alerts
  rules:
  - alert: HighWebTransportLoadShedding
    expr: rate(webtransport_sessions_rejected_total[1m]) > (1 / 60)
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "WebTransport server is shedding sessions due to overload"
      description: "Instance {{ $labels.instance }} is rejecting incoming WebTransport CONNECT streams with HTTP 503."

  - alert: HighDatagramDropRate
    expr: rate(webtransport_datagrams_discarded_total[1m]) > 50
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "High WebTransport datagram drop rate detected"
      description: "Datagram mailbox drops on {{ $labels.instance }} exceed 50/sec. Check mailbox capacity and socket buffers."

  - alert: DirectMemoryNearExhaustion
    expr: jvm_memory_direct_bytes / jvm_memory_direct_max_bytes > 0.90
    for: 3m
    labels:
      severity: critical
    annotations:
      summary: "Netty direct memory above 90%"
      description: "Instance {{ $labels.instance }} is close to Direct buffer OutOfMemoryError."
```

---

## 4. JVM & OS Kernel Tuning Matrix

| Parameter | Recommended Production Value | Notes |
|---|---|---|
| `-XX:+UseZGC -XX:+ZGenerational` | Enabled (Java 21+) | Sub-millisecond GC pause times under high session volume |
| `-XX:MaxDirectMemorySize` | Minimum 4G to 16G | Required for Netty and QUIC native off-heap ring buffers |
| `net.core.rmem_max` | `16777216` (16 MB) | Prevents kernel UDP datagram buffer overflow |
| `net.core.wmem_max` | `16777216` (16 MB) | Prevents UDP socket send buffer exhaustion |
| `net.core.somaxconn` | `65535` | Maximum socket listen queue |
| `fs.file-max` | `2097152` | Ensure OS has file descriptors for 100k+ concurrent connections |
| `nofile` (limits.conf) | `1048576` | Soft and hard file descriptor limit per process |
