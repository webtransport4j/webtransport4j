# WebTransport4J Enterprise Capacity Planning & Sizing Guide

This guide provides quantitative resource sizing formulas, CPU/memory recommendations, and operating system limits for operating `webtransport4j` clusters at scale.

---

## 1. Resource Consumption Formulas

### Memory Sizing Model
Total memory per node is composed of **JVM Heap**, **Netty Off-Heap Direct Memory**, and **Native QUIC / BoringSSL state**:

$$\text{Total RAM} = \text{Heap} + \text{DirectMemory} + \text{NativeOS}$$

Where:
- **Heap Memory**: Approximately **16 KB to 32 KB per active session** for Java object graphs (`WebTransportSession`, `Http3Headers`, capsule state, routing tables).
- **Pending Datagram Mailbox Payload**: One `DatagramMailbox` is created per active QUIC connection using the business executor, shared by that connection's sessions. For queued payloads no larger than an assumed `DatagramMTU`:
  $$\text{PendingMailboxPayload} \le N_{\text{connections}} \times \text{MailboxCapacity} \times \text{DatagramMTU}$$
  With capacity 1024 and a 1252-byte payload sizing assumption, a full mailbox holds up to 1,282,048 bytes (1.223 MiB) of queued payload. Count this against direct memory when the inbound `ByteBuf` is direct, or heap when it is heap-backed. `WebTransportDatagramDecoder` retains a slice of the incoming buffer; the mailbox does not select or change its allocation type.
- **Other Buffer Storage**: Budget separately for an in-flight datagram per mailbox, stream queues, transport buffers, allocator overhead and pooled memory. Retained slices may keep a larger backing allocation alive, so the payload formula is not a bound on allocated memory. Flow-control windows describe protocol credit, not necessarily allocated buffer bytes.
- **Native Quiche / BoringSSL Contexts**: Approximately **24 KB to 48 KB per QUIC connection** allocated directly by the C native library.

---

## 2. Cluster Node Sizing Matrix

These illustrative memory budgets assume **one session per connection**, capacity 1024, full datagram mailboxes and the 1252-byte payload assumption above. They conservatively charge queued payloads to direct memory; verify the actual allocator and retained backing-buffer sizes in your deployment. Multiple sessions on one connection share its mailbox. Heap and native-state estimates above are planning assumptions, not measured guarantees.

| Concurrent Sessions / Connections | JVM Heap (`-Xmx`) | Full Mailbox Payload (GiB) | Direct Memory Budget (`-XX:MaxDirectMemorySize`) | Total Node RAM Budget |
|---|---|---|---|---|
| **1,000** | 512 MiB | 1.19 | 2 GiB | 4 GiB |
| **10,000** | 2 GiB | 11.94 | 16 GiB | 24 GiB |
| **50,000** | 8 GiB | 59.70 | 80 GiB | 96 GiB |
| **100,000** | 16 GiB | 119.40 | 160 GiB | 192 GiB |

The remaining budget must cover other buffers, heap, native QUIC/TLS allocations and OS memory. These budgets do not establish CPU or throughput capacity. Measure peak resident memory, buffer backing type, queue occupancy and processing latency under representative load. Reduce mailbox capacity or connections per node if the measured budget does not fit.

---

## 3. Detailed Breakdown by Scale Tier

### Tier 1: 10,000 Concurrent Sessions (Standard Kubernetes Pod)
- **Use Case**: Typical microservice or gaming lobby server pod behind an AWS Network Load Balancer (NLB) or Kubernetes Ingress.
- **Resource Allocations**:
  ```yaml
  resources:
    requests:
      cpu: "4000m"
      memory: "20Gi"
    limits:
      cpu: "8000m"
      memory: "24Gi"
  ```
- **JVM Configuration**:
  ```bash
  -Xms2g -Xmx2g -XX:MaxDirectMemorySize=16g
  -XX:+UseZGC -XX:+ZGenerational
  ```
- **Configuration Tunings (`webtransport.properties`)**:
  ```properties
  webtransport4j.server.max_concurrent_sessions=10000
  webtransport4j.datagram.mailbox.capacity=1024
  webtransport4j.datagram.mailbox.batch_size=64
  ```

---

### Tier 2: 50,000 Concurrent Sessions (High-Density Edge Node)
- **Use Case**: Large-scale financial real-time ticker fan-out, IoT fleet telemetry ingestion, or live streaming chat.
- **Resource Allocations**:
  ```yaml
  resources:
    requests:
      cpu: "12000m"
      memory: "88Gi"
    limits:
      cpu: "16000m"
      memory: "96Gi"
  ```
- **JVM Configuration**:
  ```bash
  -Xms8g -Xmx8g -XX:MaxDirectMemorySize=80g
  -XX:+UseZGC -XX:+ZGenerational
  ```
- **OS Kernel Sizing (`/etc/sysctl.conf`)**:
  ```ini
  fs.file-max = 1048576
  net.core.rmem_max = 16777216
  net.core.wmem_max = 16777216
  net.core.somaxconn = 65535
  net.ipv4.udp_mem = 4096000 8192000 16384000
  ```

---

### Tier 3: 100,000 Concurrent Sessions (Mega Carrier-Grade Node)
- **Use Case**: Telecom gateway, central regional hub, or massive multiplayer online (MMO) backend.
- **Hardware Profile**: A node with at least 192 GiB RAM for this illustrative buffer budget; size CPU and NIC throughput from load tests.
- **JVM Configuration**:
  ```bash
  -Xms16g -Xmx16g -XX:MaxDirectMemorySize=160g
  -XX:+UseZGC -XX:+ZGenerational
  ```
- **Process Limits (`/etc/security/limits.conf`)**:
  ```ini
  * soft nofile 1048576
  * hard nofile 1048576
  * soft nproc  524288
  * hard nproc  524288
  ```

---

## 4. Datagram Rate and Packet Per Second (PPS) Limits

QUIC and WebTransport datagrams incur UDP kernel packet processing overhead:
- **Maximum MTU**: QUIC default IPv4 datagram MTU is typically 1252 bytes (1200 bytes QUIC minimum, 1280 bytes IPv6 minimum).
- **Single-Core SoftIRQ Bound**: A single CPU core processing Linux kernel network interrupts can handle approximately **800,000 to 1,200,000 PPS**.
- **Receive Side Scaling (RSS)**: Ensure multi-queue NICs and RSS are enabled across all available CPU cores:
  ```bash
  ethtool -L eth0 combined 8
  ```
- **UDP Socket Tuning**: Enable `webtransport4j.server.socket.autotune=true` to request larger kernel buffers up to detected Linux limits. Explicit `socket.rcvbuf` and `socket.sndbuf` settings take precedence independently; raise OS limits when they are below the target. The kernel determines the actual buffer size, and tuning does not guarantee freedom from packet drops.
