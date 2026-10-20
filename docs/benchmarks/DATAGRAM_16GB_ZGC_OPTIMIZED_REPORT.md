# WebTransport4J: 16.00 GiB Datagram Ingestion Report (Java 25.0.4.1 + ZGC)

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
| **Active Transfer Duration** | 49.049 s |
| **Data Throughput** | **334.03 MiB/s** |
| **Packet Throughput** | **342,049.2 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `b45e54b0` | 16,777,216 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **88** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **505 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 1,927,282,688 bytes (1838.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 517,996,544 bytes (494.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 507,510,784 bytes (484.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 3 | 0 ms |
| `ZGC Major Pauses` | 70 | 0 ms |
| `ZGC Major Cycles` | 14 | 496 ms |
| `ZGC Minor Cycles` | 1 | 9 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 12,938,900,568 bytes | 12339.50 MiB |
| `main` | 188,784 bytes | 0.18 MiB |
| `wt-udp-error-metrics-poller` | 30,808 bytes | 0.03 MiB |
| `wt-tls-cert-watcher` | 4,520 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 3,392 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         79753        9173400  [B (java.base@25.0.4.1)
   2:          1656        5454088  [J (java.base@25.0.4.1)
   3:          7203        1899000  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         70662        1695888  java.lang.String (java.base@25.0.4.1)
   5:          6332        1256240  java.lang.Class (java.base@25.0.4.1)
   6:          3209        1117880  [I (java.base@25.0.4.1)
   7:         21657         866280  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8659         732816  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16382         655280  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7769         621520  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          9364         524384  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  12:           148         346064  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  13:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  14:          3438         302544  io.netty.buffer.PoolSubpage
  15:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  16:          1514         218016  java.lang.reflect.Method (java.base@25.0.4.1)
  17:          4582         146624  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  18:          3574         142960  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  19:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  20:          1984         126976  java.lang.invoke.MemberName (java.base@25.0.4.1)
  21:          1368         122288  [Ljava.lang.String; (java.base@25.0.4.1)
  22:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  23:           256         109416  [C (java.base@25.0.4.1)
  24:          1389          77784  java.lang.invoke.MethodType (java.base@25.0.4.1)
  25:          1024          73728  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  26:          1979          70216  [Ljava.lang.Class; (java.base@25.0.4.1)
  27:          1389          66672  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  28:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  29:           488          64416  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  30:          1148          64288  java.util.HashMap (java.base@25.0.4.1)
  31:          2475          59400  java.util.ArrayList (java.base@25.0.4.1)
  32:          1441          57640  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
  33:          3574          57184  java.util.concurrent.locks.ReentrantLock (java.base@25.0.4.1)
...
Total        346689       29513256
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 3.15 MiB | 7.25 MiB |
| `Metaspace` | 25.80 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.22 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.03 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 62.00 MiB | 2048.00 MiB |
| `ZGC Young Generation` | 456.00 MiB | 2048.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.35 MiB | 116.38 MiB |

