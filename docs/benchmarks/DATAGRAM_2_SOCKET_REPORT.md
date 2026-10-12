# WebTransport4J: 0.10 GiB Datagram Ingestion Report (Java 25.0.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **0.10 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** |
| **Target Payload** | 104,857,600 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | false |
| **Total Datagrams Received** | 22,494 packets |
| **Total Ingested Payload** | 23,033,856 bytes (21.97 MiB / 0.02 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 0.554 s |
| **Data Throughput** | **39.69 MiB/s** |
| **Packet Throughput** | **40,639.3 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `04dcc937` | 22,494 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **39 ms** | Concurrent cycle time |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 94,371,840 bytes (90.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 94,371,840 bytes (90.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 90,177,536 bytes (86.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 39 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-2` | 12,268,368 bytes | 11.70 MiB |
| `main` | 190,720 bytes | 0.18 MiB |
| `wt-rate-limit-reloader` | 47,160 bytes | 0.04 MiB |
| `wt-tls-cert-watcher` | 3,176 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 0 bytes | 0.00 MiB |
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
| `CodeHeap 'non-nmethods'` | 1.45 MiB | 5.56 MiB |
| `Metaspace` | 24.48 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.59 MiB | 117.20 MiB |
| `Compressed Class Space` | 4.82 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 3072.00 MiB |
| `ZGC Young Generation` | 90.00 MiB | 3072.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.33 MiB | 117.23 MiB |

