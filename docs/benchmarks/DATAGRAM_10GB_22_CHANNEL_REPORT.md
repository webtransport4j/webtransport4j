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
| **Active Sockets (SO_REUSEPORT)** | 13 socket(s) |
| **Active Transfer Duration** | 12.503 s |
| **Data Throughput** | **819.02 MiB/s** |
| **Packet Throughput** | **838,673.7 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `3dca5d9e` | 478,096 packets | 4.6% |
| `45e2bb10` | 477,617 packets | 4.6% |
| `dd5db7fc` | 955,344 packets | 9.1% |
| `45a902c6` | 1,427,528 packets | 13.6% |
| `2d9bae8e` | 478,448 packets | 4.6% |
| `5c2435d2` | 1,427,244 packets | 13.6% |
| `ab1ad823` | 472,624 packets | 4.5% |
| `d9607e1b` | 477,952 packets | 4.6% |
| `8279811a` | 477,713 packets | 4.6% |
| `785b5adc` | 1,430,538 packets | 13.6% |
| `1ddef1ca` | 952,256 packets | 9.1% |
| `b8a03459` | 477,216 packets | 4.6% |
| `f6707f24` | 953,184 packets | 9.1% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **24** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **172 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 5,356,126,208 bytes (5108.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 5,356,126,208 bytes (5108.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 5,345,640,448 bytes (5098.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 54,525,952 bytes (52.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 20 | 0 ms |
| `ZGC Major Cycles` | 4 | 172 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-4` | 1,178,277,176 bytes | 1123.69 MiB |
| `multiThreadIoEventLoopGroup-2-13` | 1,176,852,664 bytes | 1122.33 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 1,176,672,536 bytes | 1122.16 MiB |
| `multiThreadIoEventLoopGroup-2-11` | 789,176,232 bytes | 752.62 MiB |
| `multiThreadIoEventLoopGroup-2-20` | 787,443,944 bytes | 750.97 MiB |
| `multiThreadIoEventLoopGroup-2-17` | 787,067,800 bytes | 750.61 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 395,789,392 bytes | 377.45 MiB |
| `multiThreadIoEventLoopGroup-2-12` | 395,481,720 bytes | 377.16 MiB |
| `multiThreadIoEventLoopGroup-2-21` | 395,006,656 bytes | 376.71 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 394,901,872 bytes | 376.61 MiB |
| `multiThreadIoEventLoopGroup-2-9` | 394,800,472 bytes | 376.51 MiB |
| `multiThreadIoEventLoopGroup-2-10` | 394,735,904 bytes | 376.45 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 391,456,568 bytes | 373.32 MiB |
| `main` | 189,376 bytes | 0.18 MiB |
| `wt-tls-cert-watcher` | 2,552 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 1,696 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-22` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-18` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-19` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-14` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-15` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-16` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        105812       10260632  [B (java.base@25.0.4.1)
   2:          1810        5776464  [J (java.base@25.0.4.1)
   3:          8883        4269440  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71787        1722888  java.lang.String (java.base@25.0.4.1)
   5:          3798        1640232  [I (java.base@25.0.4.1)
   6:          6687        1322288  java.lang.Class (java.base@25.0.4.1)
   7:         13312         958464  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
   8:         21914         876560  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   9:          8716         744096  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
  10:         16616         664640  java.util.HashMap$Node (java.base@25.0.4.1)
  11:          7769         621520  java.util.LinkedHashMap (java.base@25.0.4.1)
  12:          9364         524384  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  13:           181         356064  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  14:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  15:          3445         303160  io.netty.buffer.PoolSubpage
  16:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  17:          1519         218736  java.lang.reflect.Method (java.base@25.0.4.1)
  18:           278         199880  [C (java.base@25.0.4.1)
  19:          4594         147008  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  20:          3586         143440  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  21:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  22:          2017         129088  java.lang.invoke.MemberName (java.base@25.0.4.1)
  23:          1368         122328  [Ljava.lang.String; (java.base@25.0.4.1)
  24:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  25:            13         106704  [Lio.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket;
  26:           101          82320  [Lio.netty.buffer.PoolSubpage;
  27:          1422          79632  java.lang.invoke.MethodType (java.base@25.0.4.1)
  28:          2019          71800  [Ljava.lang.Class; (java.base@25.0.4.1)
  29:          1089          69696  java.util.WeakHashMap$Entry (java.base@25.0.4.1)
  30:          1223          68488  java.util.HashMap (java.base@25.0.4.1)
  31:          1422          68256  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  32:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  33:           492          64944  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
...
Total        406852       35971320
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.62 MiB | 7.25 MiB |
| `Metaspace` | 26.87 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.43 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.39 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 92.00 MiB | 6144.00 MiB |
| `ZGC Young Generation` | 54.00 MiB | 6144.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.86 MiB | 116.38 MiB |

