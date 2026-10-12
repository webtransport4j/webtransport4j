# WebTransport4J: 1.00 GiB Datagram Ingestion Report (Java 25.0.2 + ZGC)

Empirical benchmark measuring high-velocity transfer of **1.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** |
| **Target Payload** | 1,073,741,824 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | false |
| **Total Datagrams Received** | 1,044,402 packets |
| **Total Ingested Payload** | 1,069,467,648 bytes (1019.92 MiB / 1.00 GiB) |
| **Active Transfer Duration** | 6.212 s |
| **Data Throughput** | **164.19 MiB/s** |
| **Packet Throughput** | **168,134.1 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **12** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **207 ms** | Concurrent cycle time |
| **Initial Used Heap** | 6,291,456 bytes (6.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 427,819,008 bytes (408.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 264,241,152 bytes (252.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 257,949,696 bytes (246.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 10 | 0 ms |
| `ZGC Major Cycles` | 2 | 207 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 485,043,952 bytes | 462.57 MiB |
| `main` | 149,848 bytes | 0.14 MiB |
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
| `CodeHeap 'non-nmethods'` | 1.53 MiB | 5.56 MiB |
| `Metaspace` | 25.44 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.48 MiB | 117.21 MiB |
| `Compressed Class Space` | 5.01 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 2048.00 MiB |
| `ZGC Young Generation` | 254.00 MiB | 2048.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.11 MiB | 117.22 MiB |

