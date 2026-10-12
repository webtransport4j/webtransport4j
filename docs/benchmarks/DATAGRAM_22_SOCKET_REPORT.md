# WebTransport4J: 0.25 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **0.25 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **COMPLETE** |
| **Target Payload** | 268,435,456 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | true |
| **Total Datagrams Received** | 262,144 packets |
| **Total Ingested Payload** | 268,435,456 bytes (256.00 MiB / 0.25 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 14 socket(s) |
| **Active Transfer Duration** | 0.802 s |
| **Data Throughput** | **319.10 MiB/s** |
| **Packet Throughput** | **326,756.6 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `5ccd3743` | 11,916 packets | 4.5% |
| `c55c33fd` | 23,832 packets | 9.1% |
| `08bd5833` | 11,916 packets | 4.5% |
| `708bdb88` | 11,916 packets | 4.5% |
| `ce72e85b` | 11,916 packets | 4.5% |
| `cf76871b` | 11,916 packets | 4.5% |
| `00b67c78` | 23,832 packets | 9.1% |
| `3cd4d5e8` | 11,916 packets | 4.5% |
| `62a682d9` | 47,664 packets | 18.2% |
| `dfd4ae97` | 11,916 packets | 4.5% |
| `73d4c376` | 23,832 packets | 9.1% |
| `492128ef` | 23,832 packets | 9.1% |
| `85c9a0d3` | 23,832 packets | 9.1% |
| `c01411cb` | 11,916 packets | 4.5% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **58 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 266,338,304 bytes (254.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 266,338,304 bytes (254.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 255,852,544 bytes (244.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 58,720,256 bytes (56.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 58 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-3` | 24,408,192 bytes | 23.28 MiB |
| `multiThreadIoEventLoopGroup-2-18` | 12,258,944 bytes | 11.69 MiB |
| `multiThreadIoEventLoopGroup-2-10` | 12,258,208 bytes | 11.69 MiB |
| `multiThreadIoEventLoopGroup-2-17` | 12,192,752 bytes | 11.63 MiB |
| `multiThreadIoEventLoopGroup-2-14` | 12,158,816 bytes | 11.60 MiB |
| `multiThreadIoEventLoopGroup-2-19` | 12,054,656 bytes | 11.50 MiB |
| `multiThreadIoEventLoopGroup-2-13` | 6,236,360 bytes | 5.95 MiB |
| `multiThreadIoEventLoopGroup-2-11` | 6,196,440 bytes | 5.91 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 6,164,464 bytes | 5.88 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 6,141,264 bytes | 5.86 MiB |
| `multiThreadIoEventLoopGroup-2-21` | 6,127,864 bytes | 5.84 MiB |
| `multiThreadIoEventLoopGroup-2-16` | 6,108,392 bytes | 5.83 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 6,102,848 bytes | 5.82 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 6,062,048 bytes | 5.78 MiB |
| `main` | 151,016 bytes | 0.14 MiB |
| `wt-tls-cert-watcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-12` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-9` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-20` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-22` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-15` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 3.22 MiB | 7.25 MiB |
| `Metaspace` | 25.84 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 6.84 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.26 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 256.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.76 MiB | 116.38 MiB |