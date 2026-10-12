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
| **Total Datagrams Received** | 16,777,216 packets |
| **Total Ingested Payload** | 17,179,869,184 bytes (16384.00 MiB / 16.00 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 133.184 s |
| **Data Throughput** | **123.02 MiB/s** |
| **Packet Throughput** | **125,970.5 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `520fcdd6` | 16,787,018 packets | 100.1% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **282** | Collections recorded |
| **Total GC Pause Time** | **1 ms** | Nonzero pause time recorded by pause MXBeans |
| **Total ZGC Cycle Time** | **1657 ms** | Concurrent cycle time |
| **Initial Used Heap** | 4,194,304 bytes (4.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 645,922,816 bytes (616.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 274,726,912 bytes (262.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 270,532,608 bytes (258.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 235 | 1 ms |
| `ZGC Major Cycles` | 47 | 1657 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 12,139,878,352 bytes | 11577.49 MiB |
| `wt-rate-limit-reloader` | 200,120 bytes | 0.19 MiB |
| `main` | 149,896 bytes | 0.14 MiB |
| `wt-tls-cert-watcher` | 5,736 bytes | 0.01 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         76617        9155288  [B (java.base@25.0.1)
   2:          1661        5716288  [J (java.base@25.0.1)
   3:         68674        1648176  java.lang.String (java.base@25.0.1)
   4:          6276        1245760  java.lang.Class (java.base@25.0.1)
   5:          7094        1206056  [Ljava.lang.Object; (java.base@25.0.1)
   6:         21594         863760  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.1)
   7:          3262         764848  [I (java.base@25.0.1)
   8:          8653         735536  [Ljava.util.HashMap$Node; (java.base@25.0.1)
   9:         16676         667040  java.util.HashMap$Node (java.base@25.0.1)
  10:          7755         620400  java.util.LinkedHashMap (java.base@25.0.1)
  11:          9375         525000  java.util.LinkedHashMap$Entry (java.base@25.0.1)
  12:           152         347792  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.1)
  13:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.1)
  14:          2012         225344  java.security.Provider$Service (java.base@25.0.1)
  15:          1507         217008  java.lang.reflect.Method (java.base@25.0.1)
  16:          4583         146656  java.security.Provider$ServiceKey (java.base@25.0.1)
  17:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.1)
  18:           260         133608  [C (java.base@25.0.1)
  19:          1961         125504  java.lang.invoke.MemberName (java.base@25.0.1)
  20:          7080         113280  java.util.jar.Attributes (java.base@25.0.1)
  21:          1254         110352  io.netty.buffer.PoolSubpage
  22:          1383         107080  [Ljava.lang.String; (java.base@25.0.1)
  23:          1322          74032  java.lang.invoke.MethodType (java.base@25.0.1)
  24:          1836          73440  sun.security.util.ObjectIdentifier (java.base@25.0.1)
  25:          1960          69400  [Ljava.lang.Class; (java.base@25.0.1)
  26:          1361          65328  jdk.internal.util.WeakReferenceKey (java.base@25.0.1)
  27:           488          64416  [Ljava.lang.ref.SoftReference; (java.base@25.0.1)
  28:          1142          63952  java.util.HashMap (java.base@25.0.1)
  29:          2440          58560  java.util.ArrayList (java.base@25.0.1)
  30:           503          56336  java.lang.reflect.Field (java.base@25.0.1)
  31:           446          53520  java.lang.reflect.Constructor (java.base@25.0.1)
  32:           513          53352  java.lang.Class$ReflectionData (java.base@25.0.1)
  33:          1320          52800  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.1)
...
Total        335657       28214320
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 1.45 MiB | 5.56 MiB |
| `Metaspace` | 25.25 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 4.69 MiB | 117.20 MiB |
| `Compressed Class Space` | 4.97 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 64.00 MiB | 2048.00 MiB |
| `ZGC Young Generation` | 8.00 MiB | 2048.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.58 MiB | 117.23 MiB |

