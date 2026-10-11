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
| **Total Datagrams Received** | 2,589,045 packets |
| **Total Ingested Payload** | 2,651,182,080 bytes (2528.36 MiB / 2.47 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 8 socket(s) |
| **Active Transfer Duration** | 10.720 s |
| **Data Throughput** | **235.85 MiB/s** |
| **Packet Throughput** | **241,513.3 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `ce02396f` | 374,845 packets | 14.5% |
| `a11d3019` | 278,051 packets | 10.7% |
| `c46570f3` | 372,750 packets | 14.4% |
| `5c075b14` | 496,235 packets | 19.2% |
| `0b769dab` | 468,657 packets | 18.1% |
| `47620e15` | 120,567 packets | 4.7% |
| `f2535724` | 219,269 packets | 8.5% |
| `94a26c38` | 258,671 packets | 10.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **18** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **973 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 662,700,032 bytes (632.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 352,321,536 bytes (336.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 343,932,928 bytes (328.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 67,108,864 bytes (64.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 15 | 0 ms |
| `ZGC Major Cycles` | 3 | 973 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-2` | 192,111,032 bytes | 183.21 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 180,143,272 bytes | 171.80 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 145,372,144 bytes | 138.64 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 143,538,992 bytes | 136.89 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 107,304,400 bytes | 102.33 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 100,318,600 bytes | 95.67 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 85,878,368 bytes | 81.90 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 47,467,232 bytes | 45.27 MiB |
| `main` | 151,336 bytes | 0.14 MiB |
| `wt-rate-limit-reloader` | 47,160 bytes | 0.04 MiB |
| `wt-tls-cert-watcher` | 3,192 bytes | 0.00 MiB |
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
| `Metaspace` | 24.50 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.86 MiB | 117.22 MiB |
| `Compressed Class Space` | 5.00 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 70.00 MiB | 3072.00 MiB |
| `ZGC Young Generation` | 268.00 MiB | 3072.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.94 MiB | 117.22 MiB |

