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
| **Total Ingested Payload** | 1,073,741,824 bytes (1024.00 MiB / 1.00 GiB) |
| **Active Transfer Duration** | 5.770 s |
| **Data Throughput** | **177.47 MiB/s** |
| **Packet Throughput** | **181,731.4 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **12** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **264 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 438,304,768 bytes (418.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 367,001,600 bytes (350.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 358,612,992 bytes (342.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 10 | 0 ms |
| `ZGC Major Cycles` | 2 | 264 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 571,807,152 bytes | 545.32 MiB |
| `main` | 154,400 bytes | 0.15 MiB |
| `wt-rate-limit-reloader` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 1.51 MiB | 5.56 MiB |
| `Metaspace` | 25.41 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.45 MiB | 117.21 MiB |
| `Compressed Class Space` | 3.05 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 2048.00 MiB |
| `ZGC Young Generation` | 352.00 MiB | 2048.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.16 MiB | 117.22 MiB |

