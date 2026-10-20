# WebTransport4J: 10.00 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **10.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** |
| **Target Payload** | 10,737,418,240 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | false |
| **Total Datagrams Received** | 4,090,349 packets |
| **Total Ingested Payload** | 4,188,517,376 bytes (3994.48 MiB / 3.90 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 12.982 s |
| **Data Throughput** | **307.70 MiB/s** |
| **Packet Throughput** | **315,086.9 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `a6892c2a` | 4,090,349 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **24** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **143 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 1,281,359,872 bytes (1222.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 973,078,528 bytes (928.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 962,592,768 bytes (918.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 20 | 0 ms |
| `ZGC Major Cycles` | 4 | 143 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 3,187,848,864 bytes | 3040.17 MiB |
| `main` | 189,096 bytes | 0.18 MiB |
| `wt-tls-cert-watcher` | 2,664 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 1,696 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         80266        9196952  [B (java.base@25.0.4.1)
   2:          1802        5773216  [J (java.base@25.0.4.1)
   3:          7333        2217848  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71132        1707168  java.lang.String (java.base@25.0.4.1)
   5:          6406        1270488  java.lang.Class (java.base@25.0.4.1)
   6:          3221        1156472  [I (java.base@25.0.4.1)
   7:         21799         871960  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8680         738912  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16526         661040  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7769         621520  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          9364         524384  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  12:           147         346304  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  13:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  14:          3433         302104  io.netty.buffer.PoolSubpage
  15:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  16:          1514         218016  java.lang.reflect.Method (java.base@25.0.4.1)
  17:          4582         146624  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  18:          3574         142960  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  19:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  20:          2020         129280  java.lang.invoke.MemberName (java.base@25.0.4.1)
  21:          1368         122336  [Ljava.lang.String; (java.base@25.0.4.1)
  22:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  23:           256         109416  [C (java.base@25.0.4.1)
  24:          1432          80192  java.lang.invoke.MethodType (java.base@25.0.4.1)
  25:          1024          73728  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  26:          2015          71808  [Ljava.lang.Class; (java.base@25.0.4.1)
  27:          1432          68736  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  28:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  29:          1169          65464  java.util.HashMap (java.base@25.0.4.1)
  30:           492          64944  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  31:           560          62720  java.lang.reflect.Field (java.base@25.0.4.1)
  32:          2507          60168  java.util.ArrayList (java.base@25.0.4.1)
  33:          1441          57640  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
...
Total        351583       30441768
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.61 MiB | 7.25 MiB |
| `Metaspace` | 26.29 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.54 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.11 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 64.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 874.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.37 MiB | 116.38 MiB |

