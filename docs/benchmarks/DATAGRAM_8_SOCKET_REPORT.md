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
| **Total Datagrams Received** | 11,201,384 packets |
| **Total Ingested Payload** | 11,470,217,216 bytes (10938.85 MiB / 10.68 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 5 socket(s) |
| **Active Transfer Duration** | 20.905 s |
| **Data Throughput** | **523.26 MiB/s** |
| **Packet Throughput** | **535,819.6 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `0a691eb1` | 2,154,940 packets | 19.2% |
| `94d97122` | 2,126,113 packets | 19.0% |
| `083fe5c3` | 2,741,184 packets | 24.5% |
| `299096bb` | 2,011,811 packets | 18.0% |
| `7ebaa939` | 2,167,336 packets | 19.3% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **24** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **348 ms** | Concurrent cycle time |
| **Initial Used Heap** | 8,388,608 bytes (8.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 4,439,670,784 bytes (4234.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 4,439,670,784 bytes (4234.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 4,431,282,176 bytes (4226.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 20,971,520 bytes (20.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 20 | 0 ms |
| `ZGC Major Cycles` | 4 | 348 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-1` | 2,233,167,776 bytes | 2129.71 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 1,765,369,536 bytes | 1683.59 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 1,755,981,336 bytes | 1674.63 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 1,732,136,424 bytes | 1651.89 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 1,638,565,736 bytes | 1562.66 MiB |
| `main` | 189,120 bytes | 0.18 MiB |
| `wt-tls-cert-watcher` | 2,808 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 2,120 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         88784        9668400  [B (java.base@25.0.4.1)
   2:          1751        5769272  [J (java.base@25.0.4.1)
   3:          7840        2900216  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71370        1712880  java.lang.String (java.base@25.0.4.1)
   5:          3417        1329776  [I (java.base@25.0.4.1)
   6:          6521        1291816  java.lang.Class (java.base@25.0.4.1)
   7:         21851         874040  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8677         738096  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16466         658640  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7769         621520  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          9363         524328  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  12:          5120         368640  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  13:           151         350848  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  14:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  15:          3437         302456  io.netty.buffer.PoolSubpage
  16:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  17:          1520         218880  java.lang.reflect.Method (java.base@25.0.4.1)
  18:          4586         146752  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  19:          3578         143120  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  20:           264         142312  [C (java.base@25.0.4.1)
  21:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  22:          2032         130048  java.lang.invoke.MemberName (java.base@25.0.4.1)
  23:          1369         122352  [Ljava.lang.String; (java.base@25.0.4.1)
  24:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  25:          1446          80976  java.lang.invoke.MethodType (java.base@25.0.4.1)
  26:          2037          72512  [Ljava.lang.Class; (java.base@25.0.4.1)
  27:          1446          69408  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  28:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  29:          1172          65632  java.util.HashMap (java.base@25.0.4.1)
  30:           492          64944  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  31:           565          63280  java.lang.reflect.Field (java.base@25.0.4.1)
  32:          2521          60504  java.util.ArrayList (java.base@25.0.4.1)
  33:          1441          57640  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
...
Total        367245       32273376
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.61 MiB | 7.25 MiB |
| `Metaspace` | 26.56 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.25 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.24 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 72.00 MiB | 8192.00 MiB |
| `ZGC Young Generation` | 4170.00 MiB | 8192.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.78 MiB | 116.38 MiB |

