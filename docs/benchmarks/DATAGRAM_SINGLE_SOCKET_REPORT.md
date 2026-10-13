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
| **Total Datagrams Received** | 111,496 packets |
| **Total Ingested Payload** | 114,171,904 bytes (108.88 MiB / 0.11 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 1 socket(s) |
| **Active Transfer Duration** | 0.684 s |
| **Data Throughput** | **159.29 MiB/s** |
| **Packet Throughput** | **163,109.3 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `1f662a2d` | 111,496 packets | 100.0% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **53 ms** | Concurrent cycle time |
| **Initial Used Heap** | 12,582,912 bytes (12.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 182,452,224 bytes (174.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 182,452,224 bytes (174.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 169,869,312 bytes (162.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 53 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 86,825,960 bytes | 82.80 MiB |
| `main` | 188,768 bytes | 0.18 MiB |
| `wt-rate-limit-reloader` | 47,064 bytes | 0.04 MiB |
| `wt-udp-error-metrics-poller` | 30,768 bytes | 0.03 MiB |
| `wt-tls-cert-watcher` | 3,176 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         80787        9223184  [B (java.base@25.0.4.1)
   2:          1803        5773488  [J (java.base@25.0.4.1)
   3:          7326        2217880  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71309        1711416  java.lang.String (java.base@25.0.4.1)
   5:          6401        1269544  java.lang.Class (java.base@25.0.4.1)
   6:          3224        1156616  [I (java.base@25.0.4.1)
   7:         22402         896080  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8801         758160  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16740         669600  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7772         621760  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          9485         531160  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  12:           150         346992  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  13:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  14:          3433         302104  io.netty.buffer.PoolSubpage
  15:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  16:          1511         217584  java.lang.reflect.Method (java.base@25.0.4.1)
  17:          4582         146624  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  18:          3574         142960  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  19:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  20:          2006         128384  java.lang.invoke.MemberName (java.base@25.0.4.1)
  21:          1368         122328  [Ljava.lang.String; (java.base@25.0.4.1)
  22:          2031         113736  java.lang.invoke.MethodType (java.base@25.0.4.1)
  23:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  24:           256         109416  [C (java.base@25.0.4.1)
  25:          2031          97488  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  26:          2086          74400  [Ljava.lang.Class; (java.base@25.0.4.1)
  27:          1024          73728  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  28:          1287          72072  java.util.HashMap (java.base@25.0.4.1)
  29:           510          67320  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  30:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  31:           562          62944  java.lang.reflect.Field (java.base@25.0.4.1)
  32:          1559          62360  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
  33:          2510          60240  java.util.ArrayList (java.base@25.0.4.1)
...
Total        356196       30650408
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.61 MiB | 7.25 MiB |
| `Metaspace` | 26.28 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 6.51 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.12 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 182.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.12 MiB | 116.38 MiB |

