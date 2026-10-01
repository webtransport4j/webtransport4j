# WebTransport4J Enterprise Observability Dashboard

Production-grade real-time telemetry, distributed tracing, and reliability operations cockpit for WebTransport4J clusters.

---

## 🌟 Key Capabilities

1. **Multi-Source Ingestion Engine**:
   - **OpenTelemetry Protocol (OTLP/HTTP)**: Direct ingestion endpoint at `POST /v1/metrics` and `POST /v1/traces`.
   - **Prometheus Scraper**: Standard text exposition scrape endpoint at `GET /metrics`.
   - **Live Cluster Simulator**: Realistic multi-node edge telemetry generator simulating QUIC sessions, stream multiplexing, BBR congestion control, datagram bursts, and connection migrations.
   - **Direct WebTransport4J Agent**: Real-time polling or stream ingestion.

2. **Executive Health & SLO Banner**:
   - 99.99% Availability SLO tracker with real-time error budget burn rate.
   - P99 connection handshake latency (0-RTT session resumption tracking).
   - Netty Buffer Leak Detector status (`-Dio.netty.leakDetection.level=PARANOID` verified 0 leaks).

3. **High-Performance 60fps Telemetry Charts**:
   - **Sessions & Streams Dynamics**: Active sessions vs open streams (bidi/uni breakdown).
   - **Datagram Throughput & Drops**: Sent packets/sec vs queue overflow drops.
   - **QUIC RTT Latency & Jitter**: Mean vs P99 latency spline curves.
   - **Netty Direct Memory**: Buffer pool allocation and capacity tracking.

4. **W3C Distributed Tracing Explorer**:
   - Compliant with [W3C TraceContext Recommendation](https://www.w3.org/TR/trace-context/).
   - Monospace trace table displaying `traceId`, `spanId`, `traceparent`, and `tracestate`.
   - Interactive waterfall modal visualizing HTTP/3 CONNECT handshake, WebTransport session initialization, and stream/datagram dispatch spans.

5. **Live Anomaly & Chaos Testing Toolbar**:
   - **Nominal Flow**: Baseline sub-millisecond QUIC performance.
   - **Network Jitter & Loss (8.5%)**: Simulates mobile carrier degradation and RTT latency spikes.
   - **Queue Drops Spike (380/s)**: Simulates receiver queue saturation and triggers critical alert rules.
   - **Migration Surge (42/s)**: Simulates Wi-Fi <-> Cellular client handover bursts.
   - **Handshake Concurrency**: Simulates sudden spike in incoming CONNECT requests.

---

## 🚀 Running the Dashboard

### 1. Launch with Python (Zero Dependencies)
```bash
python3 dashboard/server.py 8085
```
Open **[http://localhost:8085/](http://localhost:8085/)** in your browser.

### 2. Live Endpoints
- **Web UI**: `http://localhost:8085/`
- **Prometheus Metrics**: `http://localhost:8085/metrics`
- **OTLP Metrics Receiver**: `http://localhost:8085/v1/metrics`
- **OTLP Traces Receiver**: `http://localhost:8085/v1/traces`
- **Health Check**: `http://localhost:8085/health`

---

## 🛠️ Ingestion Configuration

Click the **Source: Live Cluster Simulator** badge in the dashboard navigation bar to switch between:
1. **Live Cluster Simulator**: Built-in 8-node edge cluster simulation.
2. **OpenTelemetry Collector**: Point to an existing OpenTelemetry collector (e.g., `http://collector:4318/v1/metrics`).
3. **Prometheus Server**: Scrape from a Prometheus endpoint.
4. **WebTransport4J Embedded Agent**: Connect directly to a running Java server instance.
