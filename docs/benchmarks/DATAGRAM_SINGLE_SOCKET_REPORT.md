# WebTransport4J: 0.10 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **0.10 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** |
| **Target Payload** | 104,857,600 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | false |
| **Total Datagrams Received** | 25,021 packets |
| **Total Ingested Payload** | 25,621,504 bytes (24.43 MiB / 0.02 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 0.472 s |
| **Data Throughput** | **51.81 MiB/s** |
| **Packet Throughput** | **53,054.8 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `7eaf2dd1` | 25,021 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **96 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 109,051,904 bytes (104.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 109,051,904 bytes (104.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 100,663,296 bytes (96.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 96 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 19,822,472 bytes | 18.90 MiB |
| `main` | 228,056 bytes | 0.22 MiB |
| `wt-rate-limit-reloader` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `wt-tls-cert-watcher` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         80597        9324160  [B (java.base@25.0.4.1)
   2:          1751        5768744  [J (java.base@25.0.4.1)
   3:          7325        2216736  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71123        1706952  java.lang.String (java.base@25.0.4.1)
   5:          6413        1271936  java.lang.Class (java.base@25.0.4.1)
   6:          3238        1168992  [I (java.base@25.0.4.1)
   7:         22418         896720  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8786         755712  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16691         667640  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7770         621600  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          9481         530936  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  12:           147         345920  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  13:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  14:          3433         302104  io.netty.buffer.PoolSubpage
  15:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  16:          1515         218160  java.lang.reflect.Method (java.base@25.0.4.1)
  17:          4582         146624  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  18:          3574         142960  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  19:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  20:          2017         129088  java.lang.invoke.MemberName (java.base@25.0.4.1)
  21:          1369         122352  [Ljava.lang.String; (java.base@25.0.4.1)
  22:          2047         114632  java.lang.invoke.MethodType (java.base@25.0.4.1)
  23:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  24:           256         109416  [C (java.base@25.0.4.1)
  25:          2047          98256  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  26:          2099          74872  [Ljava.lang.Class; (java.base@25.0.4.1)
  27:          1024          73728  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  28:          1274          71344  java.util.HashMap (java.base@25.0.4.1)
  29:           510          67320  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  30:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  31:           565          63280  java.lang.reflect.Field (java.base@25.0.4.1)
  32:          1559          62360  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
  33:          2510          60240  java.util.ArrayList (java.base@25.0.4.1)
...
Total        354228       30684400
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.61 MiB | 7.25 MiB |
| `Metaspace` | 26.23 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 6.25 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.12 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 110.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 1.81 MiB | 116.38 MiB |

