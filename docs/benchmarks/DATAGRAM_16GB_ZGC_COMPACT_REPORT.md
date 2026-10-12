# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **16.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **COMPLETE** |
| **Target Payload** | 17,179,869,184 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | true |
| **Total Datagrams Received (target-capped)** | 16,777,216 packets |
| **Total Ingested Payload (target-capped)** | 17,179,869,184 bytes (16384.00 MiB / 16.00 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 134.321 s |
| **Data Throughput** | **121.98 MiB/s** |
| **Packet Throughput** | **124,904.0 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

Counts are capped at the 16,777,216-packet target (1,024 bytes per packet);
per-socket shares use that same target-capped total and exclude trailing packets.

| Socket ID | Datagrams Received (target-capped) | Share of target-capped total |
| :--- | :--- | :--- |
| `02d64dd1` | 16,777,216 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **72** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **508 ms** | Concurrent cycle time |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 2,560,622,592 bytes (2442.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 406,847,488 bytes (388.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 402,653,184 bytes (384.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 60 | 0 ms |
| `ZGC Major Cycles` | 12 | 508 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 12,118,708,824 bytes | 11557.30 MiB |
| `wt-rate-limit-reloader` | 200,120 bytes | 0.19 MiB |
| `main` | 149,896 bytes | 0.14 MiB |
| `wt-tls-cert-watcher` | 5,864 bytes | 0.01 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         76701        9164448  [B (java.base@25.0.1)
   2:          1722        6028440  [J (java.base@25.0.1)
   3:         68757        1650168  java.lang.String (java.base@25.0.1)
   4:          7192        1521256  [Ljava.lang.Object; (java.base@25.0.1)
   5:          6331        1256448  java.lang.Class (java.base@25.0.1)
   6:         21694         867760  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.1)
   7:          3279         803520  [I (java.base@25.0.1)
   8:          8653         735536  [Ljava.util.HashMap$Node; (java.base@25.0.1)
   9:         16615         664600  java.util.HashMap$Node (java.base@25.0.1)
  10:          7755         620400  java.util.LinkedHashMap (java.base@25.0.1)
  11:          9375         525000  java.util.LinkedHashMap$Entry (java.base@25.0.1)
  12:           152         347792  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.1)
  13:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.1)
  14:          2012         225344  java.security.Provider$Service (java.base@25.0.1)
  15:          1507         217008  java.lang.reflect.Method (java.base@25.0.1)
  16:          4583         146656  java.security.Provider$ServiceKey (java.base@25.0.1)
  17:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.1)
  18:           260         133608  [C (java.base@25.0.1)
  19:          1973         126272  java.lang.invoke.MemberName (java.base@25.0.1)
  20:          7080         113280  java.util.jar.Attributes (java.base@25.0.1)
  21:          1249         109912  io.netty.buffer.PoolSubpage
  22:          1383         107056  [Ljava.lang.String; (java.base@25.0.1)
  23:          1337          74872  java.lang.invoke.MethodType (java.base@25.0.1)
  24:          1836          73440  sun.security.util.ObjectIdentifier (java.base@25.0.1)
  25:          1975          70120  [Ljava.lang.Class; (java.base@25.0.1)
  26:          1376          66048  jdk.internal.util.WeakReferenceKey (java.base@25.0.1)
  27:           492          64944  [Ljava.lang.ref.SoftReference; (java.base@25.0.1)
  28:          1142          63952  java.util.HashMap (java.base@25.0.1)
  29:           558          62496  java.lang.reflect.Field (java.base@25.0.1)
  30:          2472          59328  java.util.ArrayList (java.base@25.0.1)
  31:           521          54184  java.lang.Class$ReflectionData (java.base@25.0.1)
  32:           446          53520  java.lang.reflect.Constructor (java.base@25.0.1)
  33:          1320          52800  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.1)
...
Total        337200       28983816
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 1.45 MiB | 5.56 MiB |
| `Metaspace` | 25.65 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.75 MiB | 117.20 MiB |
| `Compressed Class Space` | 5.03 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 62.00 MiB | 8192.00 MiB |
| `ZGC Young Generation` | 8.00 MiB | 8192.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.43 MiB | 117.23 MiB |

