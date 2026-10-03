# WebTransport4J Enterprise Observability & Operations Cockpit

Real-time telemetry, distributed tracing, and authenticated admin operations console for WebTransport4J clusters powered by **Java 25 LTS** and **Generational ZGC**.

For remote production deployment, follow [PRODUCTION.md](PRODUCTION.md). The
production image includes the API backend, requires HTTPS and explicit remote
targets, and protects analytics and management endpoints with authentication.
Development traffic-generation tools are disabled in production mode.

---

## 🌟 Zero-Simulation Architecture

The dashboard strictly visualizes **genuine real-world metrics** from active cluster nodes, Prometheus endpoints, and OTLP receivers. **No synthetic random data or fake values are generated.**
- Starts at 0 (idle) when no client traffic is active.
- Real-time updates reflect actual QUIC packets, streams, datagrams, and buffer allocations.
- Real W3C distributed traces (`00-<trace_id>-<span_id>-<flags>`) captured from live HTTP/3 CONNECT requests.
- True instantaneous rate decay to 0 when cluster is idle.

---

## 🔐 Enterprise Authenticated Operations & Analytics Console

A dedicated enterprise administration and traffic generator console is available at **[`/admin.html`](http://localhost:8085/admin.html)**.

### Access Credentials & Security:
- **Operator ID**: configured with `WT4J_ADMIN_USERNAME`
- **Secret Passkey**: required through `WT4J_ADMIN_PASSWORD`; no password is shipped with the service
- **Clearance Level**: `Tier-3 Production Operations Admin`
- **Authentication Security**: 
  - Login form requires manual credential entry (no pre-filled passwords in DOM or scripts).
  - Credentials are cryptographically evaluated against a salted **PBKDF2-HMAC-SHA256** hash (100,000 iterations) with a random per-process salt unless explicitly configured.
  - Stored hash verification uses constant-time digest comparison (`hmac.compare_digest`) to protect against side-channel and timing attacks. Password verification uses the derived hash; provision the password through protected deployment secrets.

### Operational Capabilities:
1. **Real Handshake Orchestrator**:
   - Establishes genuine TLS 1.3 / QUIC handshake with HTTP/3 CONNECT WebTransport session upgrade.
   - Inject custom W3C `traceparent` headers to observe distributed trace propagation.
2. **Stream Transmission Engine**:
   - Open Uni-directional or Bi-directional streams with custom payloads (text, JSON, binary).
   - Concurrency slider (1 to 200 concurrent streams) with real echo checksum validation.
3. **High-Throughput Datagram Engine**:
   - Transmit 10 to 5,000 UDP datagrams into the QUIC engine at up to 50,000 packets/sec.
   - Configurable payload MTU size (64B to 1350B).
4. **Cluster Health & Latency Diagnostics**:
   - 🌐 **Probe All Cluster Nodes**: performs concurrent live HTTP health probes against all nodes (`wt-node-1`, `wt-node-2`, `wt-node-3`).
   - ⏱️ **Measure Handshake RTT**: executes genuine QUIC handshakes to measure real wire RTT latency.
   - 🔄 **Resync Live Topology**: live pulls active session states, memory profiles, and cluster topology.
   - 🏷️ **New W3C Traceparent**: generates standard W3C distributed trace context for tracing end-to-end sessions.
5. **Live Execution Wire Terminal**:
   - Monospace real-time terminal showing wire results, round-trip time, session IDs, and operations diagnostics.
6. **SecOps Audit Trail**:
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
- **Admin Operations Console**: [http://localhost:8085/admin.html](http://localhost:8085/admin.html)
- **Live Telemetry API**: [http://localhost:8085/api/live-telemetry](http://localhost:8085/api/live-telemetry)
- **Prometheus Metrics**: [http://localhost:8085/metrics](http://localhost:8085/metrics)
- **SecOps Audit Trail**: [http://localhost:8085/api/admin/audit-log](http://localhost:8085/api/admin/audit-log)

### 2. Standalone Real Traffic Generator CLI
```bash
# Connect and handshake:
java -cp "target/classes:target/lib/*" io.github.webtransport4j.example.RealTrafficGenerator handshake https://localhost:4433/echo

# Send 200 real datagrams (512 bytes each at 2000 pps):
java -cp "target/classes:target/lib/*" io.github.webtransport4j.example.RealTrafficGenerator datagrams https://localhost:4433/echo 200 512 2000


---

## ⚙️ Configuration & Decoupling (`WT4J_*` Environment Variables)

The Admin & Observability service is fully decoupled from the Java WebTransport server. It can run standalone, as an observability gateway scraping external Prometheus endpoints, or receiving OTLP JSON metrics without requiring a local Java environment.

All core settings are configurable via environment variables:

| Environment Variable | Default Value | Description |
| :--- | :--- | :--- |
| `WT4J_ENV` | `development` | Set `production` to enforce deployment guardrails |
| `WT4J_CLUSTER_NODES` | unset | Remote HTTP(S) management origins; required in production |
| `WT4J_OTLP_TOKEN` | unset | Separate bearer token for JSON OTLP ingestion |
| `WT4J_ADMIN_PORT` | `8085` | Port for the Admin & Telemetry HTTP Server |
| `WT4J_BIND_HOST` | `127.0.0.1` | Listen host interface; non-local exposure requires TLS configuration |
| `WT4J_ADMIN_USERNAME` | `secops-admin` | Operator username for admin authorization |
| `WT4J_ADMIN_PASSWORD` | unset | Required admin password (hashed with PBKDF2-HMAC-SHA256) |
| `WT4J_ADMIN_SALT` | random per process | Cryptographic salt for password hashing; sessions remain process-local |
| `WT4J_SESSION_TTL_SEC`| `3600` (1 hour) | Bearer session token time-to-live before automatic expiration |
| `WT4J_CLUSTER_HOST` | `127.0.0.1` | Target hostname or IP for WebTransport cluster nodes |
| `WT4J_CLUSTER_PORTS` | `8081,8082,8083` | Comma-delimited list of HTTP/Prometheus ports to scrape |
| `WT4J_NODE_MANAGEMENT_TOKEN` | unset | Bearer token forwarded to node drain/close APIs; must equal each node's `MANAGEMENT_AUTH_TOKEN` |
| `WT4J_PROMETHEUS_TARGETS` | `http://127.0.0.1:8081/metrics,...` | Comma-delimited Prometheus targets for metrics aggregation |
| `WT4J_CORS_ORIGIN` | unset | Optional explicit allowed CORS origin; cross-origin management is disabled by default |
| `WT4J_MAX_AUDIT_LOG` | `1000` | Maximum retention capacity for in-memory SecOps audit trail |
| `WT4J_MAX_PAYLOAD_BYTES` | `10485760` (10 MB; production manifests use 1 MB) | Maximum accepted JSON body size to prevent memory exhaustion |
| `WT4J_SCRAPE_TIMEOUT_SEC` | `1.0` | Socket timeout when probing cluster nodes or scraping metrics |
| `WT4J_TLS_CERT_FILE` / `WT4J_TLS_KEY_FILE` | unset | Required certificate and key paths when binding the dashboard beyond loopback |

---

## 🛡️ Security Hardening & Production Guardrails

1. **Path Traversal Protection**:
   - `EnterpriseObservabilityHandler.translate_path` strictly validates that the canonical real path (`os.path.realpath`) resides within the declared web root directory.
   - Any relative `../` or symlink traversal attempts are rejected immediately with `403 Forbidden`.
2. **Payload Size Enforcement**:
   - Every `POST` request validates `Content-Length` against `MAX_PAYLOAD_BYTES`.
   - Payloads exceeding the configured limit are rejected with `413 Payload Too Large`.
3. **Session TTL and Automatic Pruning**:
   - Admin bearer tokens expire after `SESSION_TTL_SEC`.
   - Expired tokens are purged dynamically during authentication checks.
4. **Bounded Memory Allocations**:
   - Audit trail capped at `MAX_AUDIT_LOG_ENTRIES` (oldest entries are popped when full).
   - Wire event logs and stream frame histories are strictly capped at 50 events per stream/session.
5. **RFC 9297 Stream & Session Guardrails**:
   - Draft-16 Section 4.7 permits new streams and datagrams on `DRAINING` sessions. Closed sessions reject stream creation with `400 Bad Request`.
   - Session stays `DRAINING` until closed; active stream counts show remaining work separately.
   - Stream #0 (CONNECT control stream) is isolated from application stream counters and stays open during drain until session closure.
6. **Graceful Fault Tolerance (Decoupled Mode)**:
   - Cluster node probe failures mark nodes `OFFLINE` without throwing unhandled exceptions.
   - Real traffic generator execution catches missing Java/Maven environments and returns `503 Service Unavailable` with actionable guidance instead of crashing.
   - Prometheus scraper gracefully skips malformed metric lines without interrupting ingestion.

---

## 🧪 Comprehensive Automated Test Suites

The test suite covers positive and negative cases across security, protocol compliance, session lifecycle, and decoupled telemetry:

```bash
# 1. Run Unit Tests (34 test cases covering security, bounds, guardrails, and fault tolerance):
python3 dashboard/test_dashboard_unit.py

# 2. Run Verification End-to-End Tests (41 test cases covering APIs, sessions, streams, capsules):
python3 dashboard/test_admin_verification.py

# 3. Run Live Unmocked Integration Tests (6 test cases against running cluster):
python3 dashboard/test_dashboard_unmocked_integration.py

# 4. Run Java RFC & Wire Compatibility Tests:
mvn test -Dtest=ServerIdConnectionIdGeneratorTest,Draft16Section9IanaAndCodepointsTest
```
