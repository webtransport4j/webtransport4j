# WebTransport4J: 1GB Datagram Ingestion Report (Java 25.0.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **1.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Total Datagrams Received** | 1,048,576 packets |
| **Total Ingested Payload** | 1,073,741,824 bytes (1024.00 MB / 1.00 GiB) |
| **Active Transfer Duration** | 9.617 s |
| **Data Throughput** | **106.48 MB/s** |
| **Packet Throughput** | **109,036.6 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **94** | Concurrently managed |
| **Total GC Pause Time** | **302 ms** | < 1 ms sub-millisecond pause |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MB) | Baseline before flood |
| **Peak Used Heap** | 109,051,904 bytes (104.00 MB) | Maximum heap during 1GB flood |
| **Final Used Heap** | 90,177,536 bytes (86.00 MB) | Steady-state post-flood |
| **Heap Delta** | 85,983,232 bytes (82.00 MB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Cycles | Total Pause Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 30 | 0 ms |
| `ZGC Major Pauses` | 45 | 0 ms |
| `ZGC Major Cycles` | 9 | 228 ms |
| `ZGC Minor Cycles` | 10 | 74 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | Megabytes Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 506,444,616 bytes | 482.98 MB |
| `main` | 42,592 bytes | 0.04 MB |
| `wt-rate-limit-reloader` | 16,568 bytes | 0.02 MB |
| `wt-tls-cert-watcher` | 3,624 bytes | 0.00 MB |
| `Reference Handler` | 0 bytes | 0.00 MB |
| `Common-Cleaner` | 0 bytes | 0.00 MB |
| `Signal Dispatcher` | 0 bytes | 0.00 MB |
| `Finalizer` | 0 bytes | 0.00 MB |
| `Notification Thread` | 0 bytes | 0.00 MB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
```

### 6. Memory Pools Usage

| Memory Pool | Used (MB) | Max (MB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 1.44 MB | 5.56 MB |
| `Metaspace` | 24.26 MB | -0.00 MB |
| `CodeHeap 'profiled nmethods'` | 4.43 MB | 117.20 MB |
| `Compressed Class Space` | 4.77 MB | 1024.00 MB |
| `ZGC Old Generation` | 50.00 MB | 512.00 MB |
| `ZGC Young Generation` | 36.00 MB | 512.00 MB |
| `CodeHeap 'non-profiled nmethods'` | 1.39 MB | 117.23 MB |

