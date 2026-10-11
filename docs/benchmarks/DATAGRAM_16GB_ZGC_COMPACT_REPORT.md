# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.1 + ZGC)
 
Empirical benchmark measuring high-velocity transfer of **16.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** (16 GiB target not reached) |
| **Target Payload** | 17,179,869,184 bytes |
| **Target Reached** | false |
| **Total Datagrams Received** | 15,973,549 packets |
| **Total Ingested Payload** | 16,356,914,176 bytes (15599.17 MiB / 15.23 GiB) |
| **Active Transfer Duration** | 129.007 s |
| **Data Throughput** | **120.92 MiB/s** |
| **Packet Throughput** | **123,819.1 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **42** | Concurrently managed |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **260 ms** | Concurrent cycle time |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 2,575,302,656 bytes (2456.00 MiB) | Maximum heap during 1GB flood |
| **Final Used Heap** | 306,184,192 bytes (292.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 301,989,888 bytes (288.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 35 | 0 ms |
| `ZGC Major Cycles` | 7 | 260 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 7,712,720,696 bytes | 7355.42 MiB |
| `wt-rate-limit-reloader` | 200,120 bytes | 0.19 MiB |
| `main` | 42,592 bytes | 0.04 MiB |
| `wt-tls-cert-watcher` | 5,736 bytes | 0.01 MiB |
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
| `CodeHeap 'non-nmethods'` | 1.44 MiB | 5.56 MiB |
| `Metaspace` | 24.28 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.41 MiB | 117.20 MiB |
| `Compressed Class Space` | 4.79 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 62.00 MiB | 8192.00 MiB |
| `ZGC Young Generation` | 232.00 MiB | 8192.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.70 MiB | 117.23 MiB |

