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
| **Total Datagrams Received** | 262,145 packets |
| **Total Ingested Payload** | 268,436,480 bytes (256.00 MiB / 0.25 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 7 socket(s) |
| **Active Transfer Duration** | 0.819 s |
| **Data Throughput** | **312.45 MiB/s** |
| **Packet Throughput** | **319,948.5 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `c66e138d` | 35,748 packets | 13.6% |
| `81cfc24c` | 47,664 packets | 18.2% |
| `40da044c` | 35,748 packets | 13.6% |
| `b5a8d8d5` | 11,916 packets | 4.5% |
| `40627a32` | 59,580 packets | 22.7% |
| `93fa07d8` | 47,664 packets | 18.2% |
| `94899ee6` | 23,832 packets | 9.1% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **60 ms** | Concurrent cycle time |
| **Initial Used Heap** | 12,582,912 bytes (12.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 247,463,936 bytes (236.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 247,463,936 bytes (236.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 234,881,024 bytes (224.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 29,360,128 bytes (28.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 60 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-8` | 30,383,864 bytes | 28.98 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 24,550,832 bytes | 23.41 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 24,084,480 bytes | 22.97 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 18,338,056 bytes | 17.49 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 18,333,120 bytes | 17.48 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 12,081,800 bytes | 11.52 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 6,219,240 bytes | 5.93 MiB |
| `main` | 150,912 bytes | 0.14 MiB |
| `wt-tls-cert-watcher` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 3.16 MiB | 7.25 MiB |
| `Metaspace` | 25.38 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 6.41 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.07 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 236.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.40 MiB | 116.38 MiB |