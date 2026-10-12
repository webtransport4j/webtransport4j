# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **16.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Standard Object Headers**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **COMPLETE** |
| **Target Payload** | 17,179,869,184 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | true |
| **Total Datagrams Received** | 16,777,216 packets |
| **Total Ingested Payload** | 17,179,869,184 bytes (16384.00 MiB / 16.00 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 135.010 s |
| **Data Throughput** | **121.35 MiB/s** |
| **Packet Throughput** | **124,266.3 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `4ad4729a` | 16,787,877 packets | 100.1% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **84** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **542 ms** | Concurrent cycle time |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 2,558,525,440 bytes (2440.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 891,289,600 bytes (850.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 887,095,296 bytes (846.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 70 | 0 ms |
| `ZGC Major Cycles` | 14 | 542 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 14,317,529,648 bytes | 13654.26 MiB |
| `wt-rate-limit-reloader` | 207,664 bytes | 0.20 MiB |
| `main` | 154,304 bytes | 0.15 MiB |
| `wt-tls-cert-watcher` | 6,064 bytes | 0.01 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         76711        9526328  [B (java.base@25.0.1)
   2:          1722        6028440  [J (java.base@25.0.1)
   3:         68767        2200544  java.lang.String (java.base@25.0.1)
   4:          7197        1521424  [Ljava.lang.Object; (java.base@25.0.1)
   5:          6332        1307296  java.lang.Class (java.base@25.0.1)
   6:         21694         867760  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.1)
   7:          3279         810904  [I (java.base@25.0.1)
   8:          8653         735536  [Ljava.util.HashMap$Node; (java.base@25.0.1)
   9:          7755         682440  java.util.LinkedHashMap (java.base@25.0.1)
  10:         16595         663800  java.util.HashMap$Node (java.base@25.0.1)
  11:          9375         525000  java.util.LinkedHashMap$Entry (java.base@25.0.1)
  12:           152         347792  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.1)
  13:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.1)
  14:          1507         229064  java.lang.reflect.Method (java.base@25.0.1)
  15:          2012         225344  java.security.Provider$Service (java.base@25.0.1)
  16:          4583         183320  java.security.Provider$ServiceKey (java.base@25.0.1)
  17:          7080         169920  java.util.jar.Attributes (java.base@25.0.1)
  18:           260         135264  [C (java.base@25.0.1)
  19:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.1)
  20:          1974         126336  java.lang.invoke.MemberName (java.base@25.0.1)
  21:          1249         119904  io.netty.buffer.PoolSubpage
  22:          1383         107048  [Ljava.lang.String; (java.base@25.0.1)
  23:          1836          88128  sun.security.util.ObjectIdentifier (java.base@25.0.1)
  24:          1337          85568  java.lang.invoke.MethodType (java.base@25.0.1)
  25:          5336          85376  java.lang.Object (java.base@25.0.1)
  26:          2472          79104  java.util.ArrayList (java.base@25.0.1)
  27:          1142          73088  java.util.HashMap (java.base@25.0.1)
  28:          1975          70120  [Ljava.lang.Class; (java.base@25.0.1)
  29:          2116          67712  java.util.ImmutableCollections$List12 (java.base@25.0.1)
  30:          1376          66048  jdk.internal.util.WeakReferenceKey (java.base@25.0.1)
  31:           492          64944  [Ljava.lang.ref.SoftReference; (java.base@25.0.1)
  32:           558          62496  java.lang.reflect.Field (java.base@25.0.1)
  33:           521          54184  java.lang.Class$ReflectionData (java.base@25.0.1)
...
Total        337218       30497200
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 1.46 MiB | 5.56 MiB |
| `Metaspace` | 25.63 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.75 MiB | 117.20 MiB |
| `Compressed Class Space` | 3.08 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 66.00 MiB | 8192.00 MiB |
| `ZGC Young Generation` | 8.00 MiB | 8192.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.70 MiB | 117.23 MiB |

