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
| **Total Datagrams Received** | 69,501 packets |
| **Total Ingested Payload** | 71,169,024 bytes (67.87 MiB / 0.07 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 5 socket(s) |
| **Active Transfer Duration** | 0.559 s |
| **Data Throughput** | **121.34 MiB/s** |
| **Packet Throughput** | **124,254.9 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `a5c74315` | 14,249 packets | 20.5% |
| `71fba045` | 15,759 packets | 22.7% |
| `43d6273f` | 13,440 packets | 19.3% |
| `31620e78` | 13,440 packets | 19.3% |
| `b76742c1` | 12,613 packets | 18.1% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **103 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 186,646,528 bytes (178.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 186,646,528 bytes (178.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 176,160,768 bytes (168.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 20,971,520 bytes (20.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 103 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-6` | 12,833,768 bytes | 12.24 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 11,643,224 bytes | 11.10 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 11,297,824 bytes | 10.77 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 11,122,232 bytes | 10.61 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 10,305,648 bytes | 9.83 MiB |
| `main` | 188,400 bytes | 0.18 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 0 bytes | 0.00 MiB |
| `wt-tls-cert-watcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `wt-udp-error-metrics-poller` | 0 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         89318        9696312  [B (java.base@25.0.4.1)
   2:          1759        5769928  [J (java.base@25.0.4.1)
   3:          8021        2908704  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71546        1717104  java.lang.String (java.base@25.0.4.1)
   5:          3510        1331264  [I (java.base@25.0.4.1)
   6:          6591        1305352  java.lang.Class (java.base@25.0.4.1)
   7:         22555         902200  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8798         757440  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16706         668240  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7770         621600  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          9481         530936  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  12:          5120         368640  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  13:           151         350848  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  14:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  15:          3437         302456  io.netty.buffer.PoolSubpage
  16:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  17:          1520         218880  java.lang.reflect.Method (java.base@25.0.4.1)
  18:          4587         146784  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  19:          3578         143120  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  20:           264         142312  [C (java.base@25.0.4.1)
  21:          2162         138368  java.lang.invoke.MemberName (java.base@25.0.4.1)
  22:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  23:          1369         122352  [Ljava.lang.String; (java.base@25.0.4.1)
  24:          2152         120512  java.lang.invoke.MethodType (java.base@25.0.4.1)
  25:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  26:          2152         103296  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  27:          2116          75376  [Ljava.lang.Class; (java.base@25.0.4.1)
  28:          1292          72352  java.util.HashMap (java.base@25.0.4.1)
  29:           510          67320  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  30:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  31:          1323          63504  java.lang.invoke.LambdaForm$Name (java.base@25.0.4.1)
  32:           565          63280  java.lang.reflect.Field (java.base@25.0.4.1)
  33:          1559          62360  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
...
Total        373184       32562096
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 3.16 MiB | 7.25 MiB |
| `Metaspace` | 26.69 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 6.37 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.27 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 184.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.13 MiB | 116.38 MiB |

