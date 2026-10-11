# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **16.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** |
| **Target Payload** | 17,179,869,184 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | false |
| **Total Datagrams Received** | 149,412 packets |
| **Total Ingested Payload** | 152,997,888 bytes (145.91 MiB / 0.14 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 6.213 s |
| **Data Throughput** | **23.49 MiB/s** |
| **Packet Throughput** | **24,049.0 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `0d78bad1` | 149,412 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **57 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 148,897,792 bytes (142.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 148,897,792 bytes (142.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 140,509,184 bytes (134.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 8,388,608 bytes (8.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 57 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 55,124,480 bytes | 52.57 MiB |
| `main` | 151,288 bytes | 0.14 MiB |
| `wt-rate-limit-reloader` | 47,240 bytes | 0.05 MiB |
| `wt-tls-cert-watcher` | 3,304 bytes | 0.00 MiB |
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
| `CodeHeap 'non-nmethods'` | 1.44 MiB | 5.57 MiB |
| `Metaspace` | 24.15 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 6.24 MiB | 117.22 MiB |
| `Compressed Class Space` | 4.83 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 3072.00 MiB |
| `ZGC Young Generation` | 142.00 MiB | 3072.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.74 MiB | 117.22 MiB |

