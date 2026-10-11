# WebTransport4J: 1.00 GiB Datagram Ingestion Report (Java 25.0.2 + ZGC)

Empirical benchmark measuring high-velocity transfer of **1.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Standard Object Headers**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **COMPLETE** |
| **Target Payload** | 1,073,741,824 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | true |
| **Total Datagrams Received** | 1,048,576 packets |
| **Total Ingested Payload** | 1,073,741,824 bytes (1024.00 MB / 1.00 GiB) |
| **Active Transfer Duration** | 5.770 s |
| **Data Throughput** | **177.47 MB/s** |
| **Packet Throughput** | **181,731.4 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **12** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **264 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MB) | Baseline before flood |
| **Peak Used Heap** | 438,304,768 bytes (418.00 MB) | Maximum heap during flood |
| **Final Used Heap** | 367,001,600 bytes (350.00 MB) | Steady-state post-flood |
| **Heap Delta** | 358,612,992 bytes (342.00 MB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 10 | 0 ms |
| `ZGC Major Cycles` | 2 | 264 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | Megabytes Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 571,807,152 bytes | 545.32 MB |
| `main` | 154,400 bytes | 0.15 MB |
| `wt-rate-limit-reloader` | 0 bytes | 0.00 MB |
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
| `CodeHeap 'non-nmethods'` | 1.51 MB | 5.56 MB |
| `Metaspace` | 25.41 MB | -0.00 MB |
| `CodeHeap 'profiled nmethods'` | 4.45 MB | 117.21 MB |
| `Compressed Class Space` | 3.05 MB | 1024.00 MB |
| `ZGC Old Generation` | 0.00 MB | 2048.00 MB |
| `ZGC Young Generation` | 352.00 MB | 2048.00 MB |
| `CodeHeap 'non-profiled nmethods'` | 2.16 MB | 117.22 MB |

