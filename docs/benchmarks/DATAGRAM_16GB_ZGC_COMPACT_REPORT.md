# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.1 + ZGC)
 
Empirical benchmark measuring high-velocity transfer of **16.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Total Datagrams Received** | 15,973,549 packets |
| **Total Ingested Payload** | 16,356,914,176 bytes (15599.17 MB / 15.23 GiB) |
| **Active Transfer Duration** | 129.007 s |
| **Data Throughput** | **120.92 MB/s** |
| **Packet Throughput** | **123,819.1 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **42** | Concurrently managed |
| **Total GC Pause Time** | **260 ms** | < 1 ms sub-millisecond pause |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MB) | Baseline before flood |
| **Peak Used Heap** | 2,575,302,656 bytes (2456.00 MB) | Maximum heap during 1GB flood |
| **Final Used Heap** | 306,184,192 bytes (292.00 MB) | Steady-state post-flood |
| **Heap Delta** | 301,989,888 bytes (288.00 MB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Cycles | Total Pause Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 35 | 0 ms |
| `ZGC Major Cycles` | 7 | 260 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | Megabytes Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 7,712,720,696 bytes | 7355.42 MB |
| `wt-rate-limit-reloader` | 200,120 bytes | 0.19 MB |
| `main` | 42,592 bytes | 0.04 MB |
| `wt-tls-cert-watcher` | 5,736 bytes | 0.01 MB |
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
| `Metaspace` | 24.28 MB | -0.00 MB |
| `CodeHeap 'profiled nmethods'` | 4.41 MB | 117.20 MB |
| `Compressed Class Space` | 4.79 MB | 1024.00 MB |
| `ZGC Old Generation` | 62.00 MB | 8192.00 MB |
| `ZGC Young Generation` | 232.00 MB | 8192.00 MB |
| `CodeHeap 'non-profiled nmethods'` | 1.70 MB | 117.23 MB |

