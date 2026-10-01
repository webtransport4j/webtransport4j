# WebTransport4J Enterprise Observability & Chaos Cockpit

Production-grade real-time telemetry, distributed tracing, and authenticated admin chaos engine for WebTransport4J clusters powered by **Java 25 LTS** and **Generational ZGC**.

---

## 🌟 Zero-Simulation Architecture

The dashboard strictly visualizes **genuine real-world metrics** from active cluster nodes, Prometheus endpoints, and OTLP receivers. **No synthetic random data is generated.**
- Starts at 0 (idle) when no client traffic is active.
- Real-time updates reflect actual QUIC packets, streams, datagrams, and buffer allocations.
- Real W3C distributed traces (`00-<trace_id>-<span_id>-<flags>`) captured from live HTTP/3 CONNECT requests.

---

## 🔐 Mission Control & Authenticated Admin Chaos Console

A dedicated enterprise administration and traffic generator console is available at **[`/admin.html`](http://localhost:8085/admin.html)**.

### Access Credentials & Security:
- **Operator ID**: `secops-admin`
- **Secret Passkey**: `webtransport2026!`
- **Clearance Level**: `Tier-3 Production Chaos & Traffic Orchestrator`
- **Authentication Security**: 
  - Login form requires manual credential entry (no pre-filled passwords in DOM or scripts).
  - Credentials are cryptographically evaluated against a salted **PBKDF2-HMAC-SHA256** hash (100,000 iterations) with salt `wt4j-enterprise-secops-salt-2026`.
  - Stored hash verification uses constant-time digest comparison (`hmac.compare_digest`) to protect against side-channel and timing attacks. Plaintext passwords are never stored or directly compared.

### Chaos Safeguards & Operational Controls:
- **Confirmation Warning Modal**: High-impact, aggressive bulk operations (e.g., Queue Overflow Storm of 1,000 datagrams, Abrupt Socket Tear / Chaos Kill, and datagram bursts $\ge 500$) trigger a confirmation dialog requiring explicit operator approval before execution.
- **Frictionless Individual Operations**: Standard and individual operations (e.g., single handshakes, targeted bidi/uni streams, small datagram transmissions $< 500$) execute directly without modal interruptions.
- **Start Fresh / Zero-Baseline Reset**: An instant telemetry reset endpoint (`/api/reset` and UI button) clears all live counters, charts, and traces back to pristine zero state.

### Operational Capabilities:
1. **Real Handshake Orchestrator**:
   - Establishes genuine TLS 1.3 / QUIC handshake with HTTP/3 CONNECT WebTransport session upgrade.
   - Inject custom W3C `traceparent` headers to observe distributed trace propagation.
2. **Stream Transmission Engine**:
   - Open Uni-directional or Bi-directional streams with custom payloads (text, JSON, binary).
   - Concurrency slider (1 to 200 concurrent streams) with real echo checksum validation.
3. **High-Throughput Datagram Hammer**:
   - Fire 10 to 5,000 UDP datagrams into the QUIC engine at up to 50,000 packets/sec.
   - Configurable payload MTU size (64B to 1350B).
4. **Production Chaos & Failure Lab**:
   - 💥 **Queue Overflow Storm**: blasts 1,000 unthrottled datagrams to saturate Netty queues and force real packet drop metrics.
   - 🛑 **Abrupt Socket Tear (Unclean Close)**: severs the network socket without sending QUIC `CONNECTION_CLOSE` to test Netty leak detection and connection sweepers.
   - 🐌 **Concurrency Backpressure**: bursts 50 concurrent streams to verify QUIC flow control windows.
5. **Live Execution Wire Terminal**:
   - Monospace real-time terminal showing wire results, round-trip time, session IDs, and **Dashboard Reasoning Hints** explaining the telemetry impact.
6. **Immutable SecOps Audit Trail**:
   - Records every administrative action, operator ID, target URL, and execution status.

---

## ⚡ Java 25 & Generational ZGC Ergonomics

WebTransport4J cluster nodes run with cutting-edge JVM low-latency flags:
```bash
JAVA_OPTS="-XX:+UseZGC \
           -XX:MaxRAMPercentage=75.0 \
           -XX:+ExitOnOutOfMemoryError \
           -Dio.netty.leakDetection.level=SIMPLE \
           -Dio.netty.allocator.type=pooled"
```
- **Sub-millisecond GC pauses**: eliminates packet jitter during high-velocity 50,000 pps datagram streaming.
- **Pooled Netty Direct Memory**: zero-copy buffer pooling prevents GC pressure on high-throughput QUIC paths.

---

## 🚀 Running the Cockpit

### 1. Launch Observability Gateway & Admin Server
```bash
python3 dashboard/server.py 8085
```
- **Observability Dashboard**: [http://localhost:8085/](http://localhost:8085/)
- **Admin Chaos Console**: [http://localhost:8085/admin.html](http://localhost:8085/admin.html)
- **Live Telemetry API**: [http://localhost:8085/api/live-telemetry](http://localhost:8085/api/live-telemetry)
- **Prometheus Metrics**: [http://localhost:8085/metrics](http://localhost:8085/metrics)
- **SecOps Audit Trail**: [http://localhost:8085/api/admin/audit-log](http://localhost:8085/api/admin/audit-log)

### 2. Standalone Real Traffic Generator CLI
```bash
# Connect and handshake:
java -cp "target/classes:target/lib/*" io.github.webtransport4j.example.RealTrafficGenerator handshake https://localhost:4433/echo

# Send 200 real datagrams (512 bytes each at 2000 pps):
java -cp "target/classes:target/lib/*" io.github.webtransport4j.example.RealTrafficGenerator datagrams https://localhost:4433/echo 200 512 2000

# Open 10 bidirectional streams:
java -cp "target/classes:target/lib/*" io.github.webtransport4j.example.RealTrafficGenerator streams https://localhost:4433/echo bidi 10 "Payload Text"

# Inject queue overflow chaos storm:
java -cp "target/classes:target/lib/*" io.github.webtransport4j.example.RealTrafficGenerator chaos-burst https://localhost:4433/echo 1000
```
