# WebTransport4J Enterprise Capacity Planning & Sizing Guide

This guide provides quantitative resource sizing formulas, CPU/memory recommendations, and operating system limits for operating `webtransport4j` clusters at scale.

---

## 1. Resource Consumption Formulas

### Memory Sizing Model
Total memory per node is composed of **JVM Heap**, **Netty Off-Heap Direct Memory**, and **Native QUIC / BoringSSL state**:

$$\text{Total RAM} = \text{Heap} + \text{DirectMemory} + \text{NativeOS}$$

Where:
- **Heap Memory**: Approximately **16 KB to 32 KB per active session** for Java object graphs (`WebTransportSession`, `Http3Headers`, capsule state, routing tables).
- **Direct Off-Heap Memory**: Approximately **64 KB to 128 KB per active session** for Netty channel buffers, ring buffers, and pending datagram mailboxes:
  $$\text{DirectMemory} \approx N_{\text{sessions}} \times (\text{FlowControlWindow}_{\text{stream}} + \text{MailboxCapacity} \times \text{DatagramMTU})$$
- **Native Quiche / BoringSSL Contexts**: Approximately **24 KB to 48 KB per QUIC connection** allocated directly by the C native library.

---

## 2. Cluster Node Sizing Matrix

| Concurrent Sessions | vCPU Cores | JVM Heap (`-Xmx`) | Direct Memory (`-XX:MaxDirectMemorySize`) | Total Node RAM | Est. Network Throughput |
|---|---|---|---|---|---|
| **1,000** (Edge / Dev) | 2 vCPU | 512 MB | 1 GB | 2 GB | Up to 100 Mbps |
| **10,000** (Standard Pod) | 4 to 8 vCPU | 2 GB | 4 GB | 8 GB | Up to 1 Gbps |
| **50,000** (High-Density) | 16 vCPU | 8 GB | 16 GB | 32 GB | Up to 10 Gbps |
| **100,000** (Mega Node) | 32 vCPU | 16 GB | 32 GB | 64 GB | Up to 25 Gbps |

---

## 3. Detailed Breakdown by Scale Tier

### Tier 1: 10,000 Concurrent Sessions (Standard Kubernetes Pod)
- **Use Case**: Typical microservice or gaming lobby server pod behind an AWS Network Load Balancer (NLB) or Kubernetes Ingress.
- **Resource Allocations**:
  ```yaml
  resources:
    requests:
      cpu: "4000m"
      memory: "6Gi"
    limits:
      cpu: "8000m"
      memory: "8Gi"
  ```
- **JVM Configuration**:
  ```bash
  -Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g
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
      memory: "24Gi"
    limits:
      cpu: "16000m"
      memory: "32Gi"
  ```
- **JVM Configuration**:
  ```bash
  -Xms8g -Xmx8g -XX:MaxDirectMemorySize=16g
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
- **Hardware Profile**: Bare-metal or c6i.8xlarge / c7g.8xlarge EC2 instance.
- **JVM Configuration**:
  ```bash
  -Xms16g -Xmx16g -XX:MaxDirectMemorySize=32g
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
- **UDP Socket Tuning**: Enable `webtransport4j.server.socket.autotune=true` to automatically allocate multi-megabyte kernel buffers and avoid packet drops during throughput spikes.
