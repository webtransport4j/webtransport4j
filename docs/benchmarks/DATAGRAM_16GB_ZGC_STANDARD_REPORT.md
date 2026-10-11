# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.1 + ZGC)
 
Empirical benchmark measuring high-velocity transfer of **16.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and Standard Object Headers.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** (16 GiB target not reached) |
| **Target Payload** | 17,179,869,184 bytes |
| **Target Reached** | false |
| **Total Datagrams Received** | 15,971,433 packets |
| **Total Ingested Payload** | 16,354,747,392 bytes (15597.10 MB / 15.23 GiB) |
| **Active Transfer Duration** | 127.360 s |
| **Data Throughput** | **122.46 MB/s** |
| **Packet Throughput** | **125,403.7 datagrams/s** |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **48** | Concurrently managed |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **282 ms** | Concurrent cycle time |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MB) | Baseline before flood |
| **Peak Used Heap** | 2,575,302,656 bytes (2456.00 MB) | Maximum heap during 1GB flood |
| **Final Used Heap** | 725,614,592 bytes (692.00 MB) | Steady-state post-flood |
| **Heap Delta** | 721,420,288 bytes (688.00 MB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 40 | 0 ms |
| `ZGC Major Cycles` | 8 | 282 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | Megabytes Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 8,996,794,896 bytes | 8580.01 MB |
| `wt-rate-limit-reloader` | 191,792 bytes | 0.18 MB |
| `main` | 44,056 bytes | 0.04 MB |
| `wt-tls-cert-watcher` | 5,808 bytes | 0.01 MB |
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
| `CodeHeap 'profiled nmethods'` | 4.46 MB | 117.20 MB |
| `Compressed Class Space` | 2.93 MB | 1024.00 MB |
| `ZGC Old Generation` | 64.00 MB | 8192.00 MB |
| `ZGC Young Generation` | 630.00 MB | 8192.00 MB |
| `CodeHeap 'non-profiled nmethods'` | 1.70 MB | 117.23 MB |

