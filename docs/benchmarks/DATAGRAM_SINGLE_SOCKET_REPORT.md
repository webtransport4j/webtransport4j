# WebTransport4J: 0.02 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **0.02 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Total Datagrams Received** | 21,684 packets |
| **Total Ingested Payload** | 22,204,416 bytes (21.18 MB / 0.02 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 0.661 s |
| **Data Throughput** | **32.03 MB/s** |
| **Packet Throughput** | **32,802.8 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `a0276728` | 21,684 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Concurrently managed |
| **Total GC Pause Time** | **41 ms** | < 1 ms sub-millisecond pause |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MB) | Baseline before flood |
| **Peak Used Heap** | 98,566,144 bytes (94.00 MB) | Maximum heap during 1GB flood |
| **Final Used Heap** | 98,566,144 bytes (94.00 MB) | Steady-state post-flood |
| **Heap Delta** | 90,177,536 bytes (86.00 MB) | Net change |
| **Off-Heap Direct Memory** | 8,388,608 bytes (8.00 MB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Cycles | Total Pause Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 41 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | Megabytes Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 9,245,784 bytes | 8.82 MB |
| `main` | 81,952 bytes | 0.08 MB |
| `wt-rate-limit-reloader` | 47,160 bytes | 0.04 MB |
| `wt-tls-cert-watcher` | 3,176 bytes | 0.00 MB |
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
| `CodeHeap 'non-nmethods'` | 1.44 MB | 5.57 MB |
| `Metaspace` | 24.03 MB | -0.00 MB |
| `CodeHeap 'profiled nmethods'` | 5.82 MB | 117.22 MB |
| `Compressed Class Space` | 4.81 MB | 1024.00 MB |
| `ZGC Old Generation` | 0.00 MB | 2048.00 MB |
| `ZGC Young Generation` | 96.00 MB | 2048.00 MB |
| `CodeHeap 'non-profiled nmethods'` | 1.53 MB | 117.22 MB |

