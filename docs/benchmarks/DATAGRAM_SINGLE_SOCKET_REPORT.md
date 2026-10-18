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
| **Total Datagrams Received** | 11,995 packets |
| **Total Ingested Payload** | 12,282,880 bytes (11.71 MiB / 0.01 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 1.137 s |
| **Data Throughput** | **10.30 MiB/s** |
| **Packet Throughput** | **10,550.9 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `db409149` | 11,995 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **165 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 138,412,032 bytes (132.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 138,412,032 bytes (132.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 127,926,272 bytes (122.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 165 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 12,731,832 bytes | 12.14 MiB |
| `main` | 227,240 bytes | 0.22 MiB |
| `wt-udp-error-metrics-poller` | 30,704 bytes | 0.03 MiB |
| `wt-tls-cert-watcher` | 3,064 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 2,544 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        182693       14615440  java.nio.DirectByteBuffer (java.base@25.0.4.1)
   2:         76903        9105664  [B (java.base@25.0.4.1)
   3:         23040        2027520  io.netty.channel.uring.MsgHdrMemory
   4:         69409        1665816  java.lang.String (java.base@25.0.4.1)
   5:          7377        1503720  [Ljava.lang.Object; (java.base@25.0.4.1)
   6:          6414        1272632  java.lang.Class (java.base@25.0.4.1)
   7:         22444         897760  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          3266         810720  [I (java.base@25.0.4.1)
   9:          8800         758496  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
  10:            25         737680  [Ljava.nio.ByteBuffer; (java.base@25.0.4.1)
  11:         16767         670680  java.util.HashMap$Node (java.base@25.0.4.1)
  12:          7771         621680  java.util.LinkedHashMap (java.base@25.0.4.1)
  13:          1871         612608  [J (java.base@25.0.4.1)
  14:          9509         532504  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  15:           147         346560  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  16:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  17:          3433         302104  io.netty.buffer.PoolSubpage
  18:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  19:          1523         219312  java.lang.reflect.Method (java.base@25.0.4.1)
  20:            24         184704  [Lio.netty.channel.uring.MsgHdrMemory;
  21:          4582         146624  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  22:          3574         142960  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  23:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  24:          2042         130688  java.lang.invoke.MemberName (java.base@25.0.4.1)
  25:          2070         115920  java.lang.invoke.MethodType (java.base@25.0.4.1)
  26:          7081         113296  java.util.jar.Attributes (java.base@25.0.4.1)
  27:           256         109416  [C (java.base@25.0.4.1)
  28:          1369         106040  [Ljava.lang.String; (java.base@25.0.4.1)
  29:          2070          99360  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  30:          2118          75840  [Ljava.lang.Class; (java.base@25.0.4.1)
  31:          1288          72128  java.util.HashMap (java.base@25.0.4.1)
  32:           518          68376  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  33:          1559          62360  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
...
Total        555480       41663944
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.08 MiB | 7.25 MiB |
| `Metaspace` | 26.51 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 6.95 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.12 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 140.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.51 MiB | 116.38 MiB |

