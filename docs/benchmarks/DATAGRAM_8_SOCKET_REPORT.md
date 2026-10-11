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
| **Total Datagrams Received** | 2,039,940 packets |
| **Total Ingested Payload** | 2,088,898,560 bytes (1992.13 MiB / 1.95 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 8 socket(s) |
| **Active Transfer Duration** | 15.850 s |
| **Data Throughput** | **125.69 MiB/s** |
| **Packet Throughput** | **128,703.7 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `4aaab53e` | 279,552 packets | 13.7% |
| `a4b3b93d` | 100,326 packets | 4.9% |
| `23dc7d4f` | 429,052 packets | 21.0% |
| `f8e7b179` | 200,417 packets | 9.8% |
| `9dfefb5d` | 221,099 packets | 10.8% |
| `9196c750` | 588,645 packets | 28.9% |
| `fb5768b7` | 124,960 packets | 6.1% |
| `38f6ef71` | 95,889 packets | 4.7% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **16** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **3607 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 564,133,888 bytes (538.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 564,133,888 bytes (538.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 555,745,280 bytes (530.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 67,108,864 bytes (64.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 3 | 0 ms |
| `ZGC Major Pauses` | 10 | 0 ms |
| `ZGC Major Cycles` | 2 | 3007 ms |
| `ZGC Minor Cycles` | 1 | 600 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-2` | 227,158,576 bytes | 216.64 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 165,653,888 bytes | 157.98 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 106,979,040 bytes | 102.02 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 84,611,728 bytes | 80.69 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 78,483,520 bytes | 74.85 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 49,344,960 bytes | 47.06 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 39,539,632 bytes | 37.71 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 38,357,288 bytes | 36.58 MiB |
| `main` | 151,376 bytes | 0.14 MiB |
| `wt-rate-limit-reloader` | 62,456 bytes | 0.06 MiB |
| `wt-tls-cert-watcher` | 3,560 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 1.44 MiB | 5.57 MiB |
| `Metaspace` | 24.47 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.52 MiB | 117.22 MiB |
| `Compressed Class Space` | 5.00 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 74.00 MiB | 3072.00 MiB |
| `ZGC Young Generation` | 464.00 MiB | 3072.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.18 MiB | 117.22 MiB |

