# WebTransport4J: 1GB Datagram Ingestion Report (Java 25.0.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **1.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Total Datagrams Received** | 1,048,576 packets |
| **Total Ingested Payload** | 1,073,741,824 bytes (1024.00 MB / 1.00 GiB) |
| **Active Transfer Duration** | 8.688 s |
| **Data Throughput** | **117.86 MB/s** |
| **Packet Throughput** | **120,692.0 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **104** | Concurrently managed |
| **Total GC Pause Time** | **291 ms** | < 1 ms sub-millisecond pause |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MB) | Baseline before flood |
| **Peak Used Heap** | 104,857,600 bytes (100.00 MB) | Maximum heap during 1GB flood |
| **Final Used Heap** | 92,274,688 bytes (88.00 MB) | Steady-state post-flood |
| **Heap Delta** | 88,080,384 bytes (84.00 MB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Cycles | Total Pause Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 42 | 0 ms |
| `ZGC Major Pauses` | 40 | 0 ms |
| `ZGC Major Cycles` | 8 | 163 ms |
| `ZGC Minor Cycles` | 14 | 128 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | Megabytes Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 591,973,464 bytes | 564.55 MB |
| `main` | 44,056 bytes | 0.04 MB |
| `wt-rate-limit-reloader` | 17,200 bytes | 0.02 MB |
| `wt-tls-cert-watcher` | 3,784 bytes | 0.00 MB |
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
| `CodeHeap 'profiled nmethods'` | 4.44 MB | 117.20 MB |
| `Compressed Class Space` | 2.92 MB | 1024.00 MB |
| `ZGC Old Generation` | 50.00 MB | 512.00 MB |
| `ZGC Young Generation` | 40.00 MB | 512.00 MB |
| `CodeHeap 'non-profiled nmethods'` | 1.40 MB | 117.23 MB |

