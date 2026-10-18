# WebTransport4J: 1.00 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **1.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** |
| **Target Payload** | 1,073,741,824 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | false |
| **Total Datagrams Received** | 96,159 packets |
| **Total Ingested Payload** | 98,466,816 bytes (93.91 MiB / 0.09 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 12 socket(s) |
| **Active Transfer Duration** | 1.477 s |
| **Data Throughput** | **63.56 MiB/s** |
| **Packet Throughput** | **65,085.9 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `0628f820` | 11,015 packets | 11.5% |
| `f2350706` | 7,974 packets | 8.3% |
| `d2a06cf1` | 7,969 packets | 8.3% |
| `949325e2` | 10,772 packets | 11.2% |
| `64573911` | 4,401 packets | 4.6% |
| `2ff34567` | 8,117 packets | 8.4% |
| `f23d3f71` | 9,915 packets | 10.3% |
| `2fe8f382` | 7,683 packets | 8.0% |
| `15ed7645` | 7,167 packets | 7.5% |
| `c7ca864e` | 7,869 packets | 8.2% |
| `87c7f113` | 6,903 packets | 7.2% |
| `265b9738` | 6,374 packets | 6.6% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **105 ms** | Concurrent cycle time |
| **Initial Used Heap** | 12,582,912 bytes (12.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 268,435,456 bytes (256.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 268,435,456 bytes (256.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 255,852,544 bytes (244.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 92,274,688 bytes (88.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 105 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-21` | 11,465,984 bytes | 10.93 MiB |
| `multiThreadIoEventLoopGroup-2-15` | 11,344,168 bytes | 10.82 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 10,249,528 bytes | 9.77 MiB |
| `multiThreadIoEventLoopGroup-2-16` | 8,615,040 bytes | 8.22 MiB |
| `multiThreadIoEventLoopGroup-2-22` | 8,448,768 bytes | 8.06 MiB |
| `multiThreadIoEventLoopGroup-2-11` | 8,340,776 bytes | 7.95 MiB |
| `multiThreadIoEventLoopGroup-2-19` | 8,279,864 bytes | 7.90 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 8,177,584 bytes | 7.80 MiB |
| `multiThreadIoEventLoopGroup-2-13` | 7,548,312 bytes | 7.20 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 7,270,008 bytes | 6.93 MiB |
| `multiThreadIoEventLoopGroup-2-17` | 6,872,696 bytes | 6.55 MiB |
| `multiThreadIoEventLoopGroup-2-9` | 5,064,328 bytes | 4.83 MiB |
| `main` | 188,936 bytes | 0.18 MiB |
| `multiThreadIoEventLoopGroup-2-10` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-12` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-20` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-18` | 32 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-14` | 32 bytes | 0.00 MiB |
| `wt-tls-cert-watcher` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        225925       18074000  java.nio.DirectByteBuffer (java.base@25.0.4.1)
   2:         77938        9380032  [B (java.base@25.0.4.1)
   3:          9990        5023320  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         33792        2973696  io.netty.channel.uring.MsgHdrMemory
   5:         70251        1686024  java.lang.String (java.base@25.0.4.1)
   6:          6818        1348424  java.lang.Class (java.base@25.0.4.1)
   7:          4209        1291664  [I (java.base@25.0.4.1)
   8:         22736         909440  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   9:          8833         763120  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
  10:            25         737680  [Ljava.nio.ByteBuffer; (java.base@25.0.4.1)
  11:          1946         705544  [J (java.base@25.0.4.1)
  12:         16810         672400  java.util.HashMap$Node (java.base@25.0.4.1)
  13:          7771         621680  java.util.LinkedHashMap (java.base@25.0.4.1)
  14:          9509         532504  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  15:           158         353776  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  16:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  17:          3444         303072  io.netty.buffer.PoolSubpage
  18:            66         271392  [Lio.netty.channel.uring.MsgHdrMemory;
  19:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  20:          1528         220032  java.lang.reflect.Method (java.base@25.0.4.1)
  21:           278         199880  [C (java.base@25.0.4.1)
  22:          2363         151232  java.lang.invoke.MemberName (java.base@25.0.4.1)
  23:          4593         146976  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  24:          3595         143800  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  25:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  26:          2295         128520  java.lang.invoke.MethodType (java.base@25.0.4.1)
  27:           110         119328  [Lio.netty.buffer.PoolSubpage;
  28:          7081         113296  java.util.jar.Attributes (java.base@25.0.4.1)
  29:          2295         110160  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  30:          1369         106040  [Ljava.lang.String; (java.base@25.0.4.1)
  31:          1760          84480  io.netty.util.internal.shaded.org.jctools.queues.atomic.unpadded.MpscAtomicUnpaddedArrayQueue
  32:          1619          77712  java.lang.invoke.LambdaForm$Name (java.base@25.0.4.1)
  33:          2127          76008  [Ljava.lang.Class; (java.base@25.0.4.1)
...
Total        630110       51477136
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.63 MiB | 7.25 MiB |
| `Metaspace` | 27.31 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 7.28 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.43 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 264.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 3.00 MiB | 116.38 MiB |

