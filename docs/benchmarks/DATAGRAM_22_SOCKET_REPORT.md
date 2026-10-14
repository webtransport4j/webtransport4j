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
| **Total Datagrams Received** | 674,052 packets |
| **Total Ingested Payload** | 690,229,248 bytes (658.25 MiB / 0.64 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 13 socket(s) |
| **Active Transfer Duration** | 0.943 s |
| **Data Throughput** | **698.10 MiB/s** |
| **Packet Throughput** | **714,856.4 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `50bbb06c` | 50,046 packets | 7.4% |
| `c17ea1a3` | 65,146 packets | 9.7% |
| `1c13348f` | 76,098 packets | 11.3% |
| `9661214c` | 50,046 packets | 7.4% |
| `6c75b020` | 51,602 packets | 7.7% |
| `84f73e65` | 45,564 packets | 6.8% |
| `163f1a2a` | 47,531 packets | 7.1% |
| `aa9a8d78` | 49,201 packets | 7.3% |
| `ac12a576` | 49,545 packets | 7.4% |
| `eaee849b` | 49,922 packets | 7.4% |
| `44689910` | 48,229 packets | 7.2% |
| `ad37687a` | 49,509 packets | 7.3% |
| `889ac641` | 41,613 packets | 6.2% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **12** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **172 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 509,607,936 bytes (486.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 400,556,032 bytes (382.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 390,070,272 bytes (372.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 54,525,952 bytes (52.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 10 | 0 ms |
| `ZGC Major Cycles` | 2 | 172 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-14` | 62,134,328 bytes | 59.26 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 53,203,672 bytes | 50.74 MiB |
| `multiThreadIoEventLoopGroup-2-16` | 41,884,768 bytes | 39.94 MiB |
| `multiThreadIoEventLoopGroup-2-21` | 40,782,936 bytes | 38.89 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 40,740,208 bytes | 38.85 MiB |
| `multiThreadIoEventLoopGroup-2-13` | 40,523,616 bytes | 38.65 MiB |
| `multiThreadIoEventLoopGroup-2-22` | 40,398,616 bytes | 38.53 MiB |
| `multiThreadIoEventLoopGroup-2-19` | 40,227,768 bytes | 38.36 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 39,984,384 bytes | 38.13 MiB |
| `multiThreadIoEventLoopGroup-2-18` | 39,331,192 bytes | 37.51 MiB |
| `multiThreadIoEventLoopGroup-2-11` | 38,744,000 bytes | 36.95 MiB |
| `multiThreadIoEventLoopGroup-2-20` | 37,220,288 bytes | 35.50 MiB |
| `multiThreadIoEventLoopGroup-2-12` | 33,990,944 bytes | 32.42 MiB |
| `main` | 189,360 bytes | 0.18 MiB |
| `wt-rate-limit-reloader` | 47,064 bytes | 0.04 MiB |
| `wt-udp-error-metrics-poller` | 30,768 bytes | 0.03 MiB |
| `wt-tls-cert-watcher` | 3,064 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-10` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-9` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-17` | 0 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-15` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        106626       10295560  [B (java.base@25.0.4.1)
   2:          1828        5777312  [J (java.base@25.0.4.1)
   3:          9406        4293552  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         72363        1736712  java.lang.String (java.base@25.0.4.1)
   5:          3867        1640408  [I (java.base@25.0.4.1)
   6:          6913        1365680  java.lang.Class (java.base@25.0.4.1)
   7:         13312         958464  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
   8:         22754         910160  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   9:          8835         763152  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
  10:         16785         671400  java.util.HashMap$Node (java.base@25.0.4.1)
  11:          7770         621600  java.util.LinkedHashMap (java.base@25.0.4.1)
  12:          9482         530992  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  13:           161         354336  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  14:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  15:          3445         303160  io.netty.buffer.PoolSubpage
  16:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  17:          1516         218304  java.lang.reflect.Method (java.base@25.0.4.1)
  18:           278         199880  [C (java.base@25.0.4.1)
  19:          2432         155648  java.lang.invoke.MemberName (java.base@25.0.4.1)
  20:          4595         147040  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  21:          3586         143440  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  22:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  23:          2313         129528  java.lang.invoke.MethodType (java.base@25.0.4.1)
  24:          1368         122328  [Ljava.lang.String; (java.base@25.0.4.1)
  25:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  26:          2313         111024  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  27:            13         106704  [Lio.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket;
  28:          1768          84864  java.lang.invoke.LambdaForm$Name (java.base@25.0.4.1)
  29:           101          82320  [Lio.netty.buffer.PoolSubpage;
  30:          1341          75096  java.util.HashMap (java.base@25.0.4.1)
  31:          2103          74904  [Ljava.lang.Class; (java.base@25.0.4.1)
  32:          1089          69696  java.util.WeakHashMap$Entry (java.base@25.0.4.1)
  33:           510          67320  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
...
Total        411857       36188000
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.06 MiB | 7.25 MiB |
| `Metaspace` | 27.24 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.63 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.50 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 84.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 306.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.67 MiB | 116.38 MiB |

