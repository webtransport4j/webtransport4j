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
| **Total Datagrams Received** | 3,526,517 packets |
| **Total Ingested Payload** | 3,611,153,408 bytes (3443.86 MiB / 3.36 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 8 socket(s) |
| **Active Transfer Duration** | 76.792 s |
| **Data Throughput** | **44.85 MiB/s** |
| **Packet Throughput** | **45,922.7 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `1b62c75e` | 230,653 packets | 6.5% |
| `fa237741` | 461,064 packets | 13.1% |
| `ccf5c843` | 170,461 packets | 4.8% |
| `ad3a872c` | 391,350 packets | 11.1% |
| `fd9a2816` | 348,066 packets | 9.9% |
| `4acfd4eb` | 910,688 packets | 25.8% |
| `ca60990f` | 440,650 packets | 12.5% |
| `50eb1da8` | 573,585 packets | 16.3% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **22** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **2254 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 643,825,664 bytes (614.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 283,115,520 bytes (270.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 274,726,912 bytes (262.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 67,108,864 bytes (64.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 3 | 0 ms |
| `ZGC Major Pauses` | 15 | 0 ms |
| `ZGC Major Cycles` | 3 | 2092 ms |
| `ZGC Minor Cycles` | 1 | 162 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-8` | 363,109,656 bytes | 346.29 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 224,932,504 bytes | 214.51 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 179,807,736 bytes | 171.48 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 172,551,064 bytes | 164.56 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 151,869,824 bytes | 144.83 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 135,372,488 bytes | 129.10 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 90,428,304 bytes | 86.24 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 68,417,000 bytes | 65.25 MiB |
| `wt-rate-limit-reloader` | 460,312 bytes | 0.44 MiB |
| `main` | 151,296 bytes | 0.14 MiB |
| `wt-tls-cert-watcher` | 9,880 bytes | 0.01 MiB |
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
| `Metaspace` | 24.44 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.86 MiB | 117.22 MiB |
| `Compressed Class Space` | 4.98 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 66.00 MiB | 3072.00 MiB |
| `ZGC Young Generation` | 206.00 MiB | 3072.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.17 MiB | 117.22 MiB |

