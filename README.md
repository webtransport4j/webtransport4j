<p align="center">
  <img src="banner.svg" alt="webtransport4j Banner" width="100%" />
</p>

<p align="center">
  <a href="#features"><img src="https://img.shields.io/badge/Protocol-WebTransport%20%2F%20HTTP%2F3-58a6ff?style=for-the-badge&logo=googlechrome&logoColor=white" alt="Protocol"></a>
  <a href="#license"><img src="https://img.shields.io/badge/License-Apache%202.0-8858ff?style=for-the-badge" alt="License"></a>
  <a href="https://img.shields.io/badge/Java-17%2B-bc8cff?style=for-the-badge&logo=openjdk&logoColor=white"><img src="https://img.shields.io/badge/Java-17%2B-bc8cff?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java Version"></a>
  <a href="#performance"><img src="https://img.shields.io/badge/QUIC-Powered-00D1B2?style=for-the-badge&logo=fastapi&logoColor=white" alt="QUIC Powered"></a>
  <a href="#build"><img src="https://img.shields.io/badge/Throughput-Ultra%20Low%20Latency-ff6c37?style=for-the-badge&logo=speedtest&logoColor=white" alt="Low Latency"></a>
</p>

---

## ✨ Why webtransport4j?

**webtransport4j** brings the raw power of the [W3C/IETF WebTransport protocol](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3) to the Java ecosystem. Designed as the modern, high-performance successor to WebSockets, it leverages **HTTP/3 and QUIC** to deliver real-time, ultra-low-latency client-server communication without the bottlenecks of TCP head-of-line blocking.

---

## ⚡ WebTransport vs. WebSockets

| Feature | Traditional WebSockets (TCP) | webtransport4j (HTTP/3 + QUIC) |
| :--- | :---: | :---: |
| **Transport Layer** | TCP / TLS | **UDP / QUIC** |
| **Head-of-Line Blocking** | ❌ Yes (Packet loss halts all traffic) | **✅ Zero HoL Blocking (Independent streams)** |
| **Delivery Modes** | Reliable Ordered Only | **Reliable Streams + Unreliable Datagrams** |
| **Connection Handshake** | 2–3 RTT (TCP + TLS + HTTP Upgrade) | **⚡ 1-RTT (Integrated TLS 1.3)** |
| **Network Switching** | ❌ Drops connection on Wi-Fi ↔ 4G/5G switch | **✅ Seamless Connection Migration** |
| **Multiplexing** | Single stream per connection | **🌊 Thousands of concurrent streams** |

---

## 🚀 Core Capabilities & Architecture

### 🌐 Next-Gen Transport & Connectivity
*   **⚡ HTTP/3 & QUIC Foundation:** Bypasses legacy TCP bottlenecks by transmitting over UDP-based QUIC, ensuring exceptional throughput even on high-latency or lossy mobile networks.
*   **📱 Seamless Connection Migration:** Switch effortlessly between Wi-Fi and 4G/5G cellular networks without dropping active sessions, losing state, or triggering heavy re-handshakes—ideal for mobile and edge clients.
*   **🔐 1-RTT TLS Resumption:** Powered by native TLS 1.3 over QUIC, enabling instant reconnects with 1 round-trip time (1-RTT) payload delivery for previously authenticated clients.

### 🌊 Advanced Stream & Data Management
*   **🔀 Multiplexed Bidirectional & Unidirectional Streams:** Spin up lightweight, concurrent streams over a single connection. Open dedicated request/response channels or high-throughput push pipelines without stream-to-stream blocking.
*   **📦 Unreliable & Unordered Datagrams:** Need sheer speed over guaranteed delivery? Dispatch low-overhead UDP datagrams perfect for high-frequency gaming state sync, live audio/video streaming, and real-time telemetry.
*   **🛡️ Zero Head-of-Line (HoL) Blocking:** Unlike TCP, packet loss on QUIC only pauses the exact stream missing a packet. All other parallel streams and datagrams continue transmitting at full velocity.
*   **⚖️ Granular Flow Control & Backpressure:** Built-in stream-level and connection-level flow control prevents memory exhaustion and handles slow consumers gracefully without degrading global connection throughput.

### ☕ Engineered for Java Performance
*   **🧠 Idiomatic & Asynchronous Java API:** Designed from the ground up for modern Java 17+, integrating cleanly with non-blocking event loops, `CompletableFuture`, and reactive pipelines.
*   **🏎️ Zero-Copy & Off-Heap Buffering:** Optimized buffer management minimizes garbage collection overhead and memory copies, unlocking maximum I/O throughput for high-concurrency workloads.
*   **🔒 Enterprise-Grade Security:** Enforces mandatory TLS 1.3 encryption with state-of-the-art cipher suites, protecting data in transit with zero configuration overhead.

---

## 🗺️ Architecture Overview

```text
        Client (Browser / Mobile / Edge)
        │
        │  1️⃣  QUIC Handshake (TLS 1.3 / 1-RTT)
        ▼
┌──────────────────────────────────────────────┐
│           webtransport4j Server              │
│                                              │
│  ├── 🌊 Bidi Stream #1  (Interactive RPC)    │
│  ├── 🌊 Bidi Stream #2  (Database Sync)      │
│  ├── 🌊 Uni Stream #3   (Live Server Push)   │
│  └── 📦 Datagrams       (60fps Player State) │
└──────────────────────────────────────────────┘
        │
        ▼
   Zero HoL Blocking • High Throughput • Off-Heap
```
# webtransport4j

The first high-performance WebTransport server for the Java ecosystem, powered by Netty's asynchronous HTTP/3 stack.

# Local Development Guide

Follow these steps to run `webtransport4j` locally with a trusted self-signed certificate and a secure browser connection.

## 1. Generate Certificates (mkcert)

WebTransport requires HTTPS. We use `mkcert` to create a locally trusted certificate.

1. **Install mkcert:**
```bash
brew install mkcert
brew install nss  # Only needed if you use Firefox

```


2. **Initialize Root CA:**
```bash
mkcert -install

```


3. **Generate Certs:**
Run this in your **Documents** folder to match the Java config below.
```bash
cd ~/Documents
mkcert localhost

```


*Output:* `localhost.pem` and `localhost-key.pem`

```
openssl req -new -key /Users/<username>/Documents/localhost-key.pem \
  -out /tmp/localhost.csr \
  -subj "/CN=localhost" \
  -config <(printf "[req]\ndistinguished_name=dn\nreq_extensions=ext\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1")

CAROOT=$(mkcert -CAROOT)

openssl x509 -req -in /tmp/localhost.csr \
  -CA "$CAROOT/rootCA.pem" \
  -CAkey "$CAROOT/rootCA-key.pem" \
  -CAcreateserial \
  -out /Users/<username>/Documents/localhost.pem \
  -days 10 -sha256 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1")
```

may need to add rootca in keychain and firefox authorities
---

## 2. Server Setup (Java)

Configure your Netty/Java server to use the generated certificates.

**Code Snippet:**

```java
QuicSslContext sslContext = QuicSslContextBuilder.forServer(
        new File("/Users/<username>/Documents/localhost-key.pem"), // Private Key
        null,
        new File("/Users/<username>/Documents/localhost.pem"))     // Public Cert
    .applicationProtocols(Http3.supportedApplicationProtocols())
    .build();

```

---

## 3. Client Setup (HTML)



**Use this in html to test webtrasnport all uni/bi/datagram apis**
***Run server***
```
 cd /
sudo http-server -S \
-C /Users/<username>/Documents/localhost.pem \
-K /Users/<username>/Documents/localhost-key.pem \
-p 8443
```
***Navigate to html***
```

https://localhost:8443/Users/<username>/Documents/GitHub/webtransport4j-incubator/native-wt-test.html
or
https://localhost:8443/Users/<username>/Documents/GitHub/webtransport4j-incubator/socketio-wt-test.html
```

**If you are using firefox**
add rootCA of mkcert in manage certificate -> authorities
&
about:config
```
network.http.http3.disable_when_third_party_roots_found	false		
network.http.http3.enable_localhost	true		
network.http.http3.enabled	true
```
