# WebTransport4J: 10.00 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **10.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **COMPLETE** |
| **Target Payload** | 10,737,418,240 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | true |
| **Total Datagrams Received** | 10,485,760 packets |
| **Total Ingested Payload** | 10,737,418,240 bytes (10240.00 MiB / 10.00 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 8 socket(s) |
| **Active Transfer Duration** | 12.494 s |
| **Data Throughput** | **819.56 MiB/s** |
| **Packet Throughput** | **839,234.5 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `62ce4ed3` | 480,416 packets | 4.6% |
| `8e51aff4` | 1,436,336 packets | 13.7% |
| `85597267` | 1,438,803 packets | 13.7% |
| `e626bf6b` | 479,984 packets | 4.6% |
| `d5f2ed83` | 480,160 packets | 4.6% |
| `0791ffee` | 1,918,554 packets | 18.3% |
| `b644fe85` | 1,914,407 packets | 18.3% |
| `483eda20` | 2,337,100 packets | 22.3% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **28** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **199 ms** | Concurrent cycle time |
| **Initial Used Heap** | 12,582,912 bytes (12.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 3,468,689,408 bytes (3308.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 3,468,689,408 bytes (3308.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 3,456,106,496 bytes (3296.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 33,554,432 bytes (32.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 3 | 0 ms |
| `ZGC Major Pauses` | 20 | 0 ms |
| `ZGC Major Cycles` | 4 | 194 ms |
| `ZGC Minor Cycles` | 1 | 5 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-4` | 1,912,409,904 bytes | 1823.82 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 1,577,918,408 bytes | 1504.82 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 1,573,951,104 bytes | 1501.04 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 1,186,297,336 bytes | 1131.34 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 1,184,288,696 bytes | 1129.43 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 396,927,232 bytes | 378.54 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 396,597,176 bytes | 378.22 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 396,508,808 bytes | 378.14 MiB |
| `main` | 188,816 bytes | 0.18 MiB |
| `wt-tls-cert-watcher` | 3,464 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 1,696 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         95244        9773128  [B (java.base@25.0.4.1)
   2:          1800        5774984  [J (java.base@25.0.4.1)
   3:          8198        3408112  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71632        1719168  java.lang.String (java.base@25.0.4.1)
   5:          3560        1438856  [I (java.base@25.0.4.1)
   6:          6563        1299472  java.lang.Class (java.base@25.0.4.1)
   7:         21890         875600  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8701         741552  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16613         664520  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7769         621520  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          8192         589824  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  12:          9364         524384  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  13:           177         354976  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  14:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  15:          3440         302720  io.netty.buffer.PoolSubpage
  16:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  17:          1519         218736  java.lang.reflect.Method (java.base@25.0.4.1)
  18:          4589         146848  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  19:          3581         143240  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  20:           264         142312  [C (java.base@25.0.4.1)
  21:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  22:          2017         129088  java.lang.invoke.MemberName (java.base@25.0.4.1)
  23:          1368         122336  [Ljava.lang.String; (java.base@25.0.4.1)
  24:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  25:          1429          80024  java.lang.invoke.MethodType (java.base@25.0.4.1)
  26:          2025          72088  [Ljava.lang.Class; (java.base@25.0.4.1)
  27:          1429          68592  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  28:          1190          66640  java.util.HashMap (java.base@25.0.4.1)
  29:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  30:             8          65664  [Lio.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket;
  31:           494          65208  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  32:           560          62720  java.lang.reflect.Field (java.base@25.0.4.1)
  33:            96          61760  [Lio.netty.buffer.PoolSubpage;
...
Total        385149       33669488
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 3.17 MiB | 7.25 MiB |
| `Metaspace` | 26.65 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.41 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.27 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 82.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 3288.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 3.43 MiB | 116.38 MiB |

