# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

Empirical benchmark measuring high-velocity transfer of **16.00 GiB** over WebTransport Datagrams
running in an isolated dedicated server process with **ZGC** and **Compact Object Headers (JEP 450)**.

### 1. Ingestion & Throughput Metrics

| Metric | Value |
| :--- | :--- |
| **Run Status** | **INCOMPLETE** |
| **Target Payload** | 17,179,869,184 bytes |
| **Completed On Time (latch)** | true |
| **Target Reached** | false |
| **Total Datagrams Received** | 1,012,577 packets |
| **Total Ingested Payload** | 1,036,878,848 bytes (988.84 MiB / 0.97 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 7.635 s |
| **Data Throughput** | **129.52 MiB/s** |
| **Packet Throughput** | **132,628.1 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `98f5ab40` | 1,012,577 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **12** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **149 ms** | Concurrent cycle time |
| **Initial Used Heap** | 12,582,912 bytes (12.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 851,443,712 bytes (812.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 96,468,992 bytes (92.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 83,886,080 bytes (80.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 10 | 0 ms |
| `ZGC Major Cycles` | 2 | 149 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 779,373,696 bytes | 743.27 MiB |
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
   1:         80841        9336728  [B (java.base@25.0.4.1)
   2:          1751        5768744  [J (java.base@25.0.4.1)
   3:          7329        2218336  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71367        1712808  java.lang.String (java.base@25.0.4.1)
   5:          6411        1271520  java.lang.Class (java.base@25.0.4.1)
   6:          3235        1168944  [I (java.base@25.0.4.1)
   7:         22423         896920  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
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
  22:          2050         114800  java.lang.invoke.MethodType (java.base@25.0.4.1)
  23:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  24:           256         109416  [C (java.base@25.0.4.1)
  25:          2050          98400  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  26:          2101          74960  [Ljava.lang.Class; (java.base@25.0.4.1)
  27:          1024          73728  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  28:          1274          71344  java.util.HashMap (java.base@25.0.4.1)
  29:           510          67320  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  30:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  31:           565          63280  java.lang.reflect.Field (java.base@25.0.4.1)
  32:          1559          62360  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
  33:          2510          60240  java.util.ArrayList (java.base@25.0.4.1)
...
Total        354757       30706016
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 3.16 MiB | 7.25 MiB |
| `Metaspace` | 26.28 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.01 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.12 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 58.00 MiB | 8192.00 MiB |
| `ZGC Young Generation` | 44.00 MiB | 8192.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.28 MiB | 116.38 MiB |

