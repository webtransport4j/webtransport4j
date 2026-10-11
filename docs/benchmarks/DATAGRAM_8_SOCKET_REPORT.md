# WebTransport4J: 0.06 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **0.06 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Total Datagrams Received** | 62,871 packets |
| **Total Ingested Payload** | 64,379,904 bytes (61.40 MB / 0.06 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 8 socket(s) |
| **Active Transfer Duration** | 0.763 s |
| **Data Throughput** | **80.52 MB/s** |
| **Packet Throughput** | **82,450.8 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `7db1b01a` | 6,902 packets | 11.0% |
| `8b5330e8` | 6,400 packets | 10.2% |
| `eef2d951` | 6,537 packets | 10.4% |
| `301ebca4` | 11,409 packets | 18.1% |
| `bcc45889` | 7,409 packets | 11.8% |
| `1f680931` | 5,741 packets | 9.1% |
| `368fdb23` | 10,030 packets | 16.0% |
| `caa526dd` | 8,443 packets | 13.4% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Concurrently managed |
| **Total GC Pause Time** | **39 ms** | < 1 ms sub-millisecond pause |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MB) | Baseline before flood |
| **Peak Used Heap** | 130,023,424 bytes (124.00 MB) | Maximum heap during 1GB flood |
| **Final Used Heap** | 130,023,424 bytes (124.00 MB) | Steady-state post-flood |
| **Heap Delta** | 121,634,816 bytes (116.00 MB) | Net change |
| **Off-Heap Direct Memory** | 67,108,864 bytes (64.00 MB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Cycles | Total Pause Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 39 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | Megabytes Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-3` | 4,576,192 bytes | 4.36 MB |
| `multiThreadIoEventLoopGroup-2-1` | 4,103,696 bytes | 3.91 MB |
| `multiThreadIoEventLoopGroup-2-7` | 3,415,160 bytes | 3.26 MB |
| `multiThreadIoEventLoopGroup-2-2` | 2,972,400 bytes | 2.83 MB |
| `multiThreadIoEventLoopGroup-2-8` | 2,866,792 bytes | 2.73 MB |
| `multiThreadIoEventLoopGroup-2-6` | 2,771,816 bytes | 2.64 MB |
| `multiThreadIoEventLoopGroup-2-4` | 2,692,808 bytes | 2.57 MB |
| `multiThreadIoEventLoopGroup-2-5` | 2,626,568 bytes | 2.50 MB |
| `wt-rate-limit-reloader` | 47,160 bytes | 0.04 MB |
| `main` | 42,680 bytes | 0.04 MB |
| `wt-tls-cert-watcher` | 3,064 bytes | 0.00 MB |
| `Common-Cleaner` | 0 bytes | 0.00 MB |
| `Signal Dispatcher` | 0 bytes | 0.00 MB |
| `Notification Thread` | 0 bytes | 0.00 MB |
| `Reference Handler` | 0 bytes | 0.00 MB |
| `Finalizer` | 0 bytes | 0.00 MB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
```

### 6. Memory Pools Usage

| Memory Pool | Used (MB) | Max (MB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 1.44 MB | 5.57 MB |
| `Metaspace` | 24.41 MB | -0.00 MB |
| `CodeHeap 'profiled nmethods'` | 6.20 MB | 117.22 MB |
| `Compressed Class Space` | 4.97 MB | 1024.00 MB |
| `ZGC Old Generation` | 0.00 MB | 2048.00 MB |
| `ZGC Young Generation` | 124.00 MB | 2048.00 MB |
| `CodeHeap 'non-profiled nmethods'` | 1.64 MB | 117.22 MB |

