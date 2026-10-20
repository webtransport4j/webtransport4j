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
| **Active Sockets (SO_REUSEPORT)** | 14 socket(s) |
| **Active Transfer Duration** | 12.466 s |
| **Data Throughput** | **821.42 MiB/s** |
| **Packet Throughput** | **841,135.8 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `7e029e40` | 477,808 packets | 4.6% |
| `8c2cdb03` | 477,296 packets | 4.6% |
| `71d6c8c9` | 475,685 packets | 4.5% |
| `7ce5520d` | 954,082 packets | 9.1% |
| `98187b3c` | 477,616 packets | 4.6% |
| `1265a66a` | 955,088 packets | 9.1% |
| `502b94b0` | 954,173 packets | 9.1% |
| `b23df128` | 474,912 packets | 4.5% |
| `1f1b13b9` | 952,496 packets | 9.1% |
| `38634e78` | 953,120 packets | 9.1% |
| `310d7094` | 474,608 packets | 4.5% |
| `9ca62fef` | 476,320 packets | 4.5% |
| `e452982c` | 953,184 packets | 9.1% |
| `f8c4a110` | 1,429,372 packets | 13.6% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **28** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **190 ms** | Concurrent cycle time |
| **Initial Used Heap** | 12,582,912 bytes (12.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 3,500,146,688 bytes (3338.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 3,500,146,688 bytes (3338.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 3,487,563,776 bytes (3326.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 58,720,256 bytes (56.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 3 | 0 ms |
| `ZGC Major Pauses` | 20 | 0 ms |
| `ZGC Major Cycles` | 4 | 181 ms |
| `ZGC Minor Cycles` | 1 | 9 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-7` | 1,179,908,440 bytes | 1125.25 MiB |
| `multiThreadIoEventLoopGroup-2-16` | 789,799,328 bytes | 753.21 MiB |
| `multiThreadIoEventLoopGroup-2-13` | 788,332,368 bytes | 751.81 MiB |
| `multiThreadIoEventLoopGroup-2-17` | 788,231,296 bytes | 751.72 MiB |
| `multiThreadIoEventLoopGroup-2-18` | 786,925,384 bytes | 750.47 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 786,507,224 bytes | 750.07 MiB |
| `multiThreadIoEventLoopGroup-2-12` | 786,470,728 bytes | 750.04 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 395,051,000 bytes | 376.75 MiB |
| `multiThreadIoEventLoopGroup-2-22` | 394,739,248 bytes | 376.45 MiB |
| `multiThreadIoEventLoopGroup-2-9` | 394,447,880 bytes | 376.17 MiB |
| `multiThreadIoEventLoopGroup-2-20` | 393,047,472 bytes | 374.84 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 392,866,640 bytes | 374.67 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 392,818,696 bytes | 374.62 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 392,760,264 bytes | 374.57 MiB |
| `main` | 189,416 bytes | 0.18 MiB |
| `wt-tls-cert-watcher` | 3,464 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 1,696 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-10` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-11` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-21` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-19` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-14` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-15` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        108003       10333168  [B (java.base@25.0.4.1)
   2:          1810        5776216  [J (java.base@25.0.4.1)
   3:          8988        4437008  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71919        1726056  java.lang.String (java.base@25.0.4.1)
   5:          3838        1680040  [I (java.base@25.0.4.1)
   6:          6714        1327256  java.lang.Class (java.base@25.0.4.1)
   7:         14336        1032192  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
   8:         21918         876720  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   9:          8719         744528  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
  10:         16639         665560  java.util.HashMap$Node (java.base@25.0.4.1)
  11:          7769         621520  java.util.LinkedHashMap (java.base@25.0.4.1)
  12:          9364         524384  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  13:           182         356976  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  14:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  15:          3446         303248  io.netty.buffer.PoolSubpage
  16:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  17:          1519         218736  java.lang.reflect.Method (java.base@25.0.4.1)
  18:           278         199880  [C (java.base@25.0.4.1)
  19:          4595         147040  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  20:          3587         143480  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  21:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  22:          2017         129088  java.lang.invoke.MemberName (java.base@25.0.4.1)
  23:          1368         122336  [Ljava.lang.String; (java.base@25.0.4.1)
  24:            14         114912  [Lio.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket;
  25:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  26:           102          86432  [Lio.netty.buffer.PoolSubpage;
  27:          1425          79800  java.lang.invoke.MethodType (java.base@25.0.4.1)
  28:          1159          74176  java.util.WeakHashMap$Entry (java.base@25.0.4.1)
  29:          2021          71912  [Ljava.lang.Class; (java.base@25.0.4.1)
  30:          1224          68544  java.util.HashMap (java.base@25.0.4.1)
  31:          1425          68400  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  32:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  33:           494          65208  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
...
Total        411033       36377888
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.61 MiB | 7.25 MiB |
| `Metaspace` | 26.91 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.52 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.41 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 96.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 3328.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.89 MiB | 116.38 MiB |

