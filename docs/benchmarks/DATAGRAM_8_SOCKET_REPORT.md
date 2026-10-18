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
| **Total Datagrams Received** | 93,899 packets |
| **Total Ingested Payload** | 96,152,576 bytes (91.70 MiB / 0.09 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 6 socket(s) |
| **Active Transfer Duration** | 1.541 s |
| **Data Throughput** | **59.52 MiB/s** |
| **Packet Throughput** | **60,948.0 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `1cf361fe` | 14,923 packets | 15.9% |
| `2d791b9a` | 16,100 packets | 17.1% |
| `814566e1` | 11,306 packets | 12.0% |
| `7f67031e` | 16,663 packets | 17.7% |
| `078e2bfa` | 15,130 packets | 16.1% |
| `5dddc681` | 19,777 packets | 21.1% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **6** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **108 ms** | Concurrent cycle time |
| **Initial Used Heap** | 12,582,912 bytes (12.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 247,463,936 bytes (236.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 247,463,936 bytes (236.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 234,881,024 bytes (224.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 33,554,432 bytes (32.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 5 | 0 ms |
| `ZGC Major Cycles` | 1 | 108 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-3` | 20,988,392 bytes | 20.02 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 17,751,136 bytes | 16.93 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 17,356,936 bytes | 16.55 MiB |
| `multiThreadIoEventLoopGroup-2-7` | 16,048,872 bytes | 15.31 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 15,945,048 bytes | 15.21 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 12,766,576 bytes | 12.18 MiB |
| `main` | 188,376 bytes | 0.18 MiB |
| `wt-udp-error-metrics-poller` | 30,712 bytes | 0.03 MiB |
| `wt-tls-cert-watcher` | 3,064 bytes | 0.00 MiB |
| `wt-rate-limit-reloader` | 2,544 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 496 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 496 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:        197113       15769040  java.nio.DirectByteBuffer (java.base@25.0.4.1)
   2:         77408        9213136  [B (java.base@25.0.4.1)
   3:          8355        2687384  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         26624        2342912  io.netty.channel.uring.MsgHdrMemory
   5:         69843        1676232  java.lang.String (java.base@25.0.4.1)
   6:          6625        1312224  java.lang.Class (java.base@25.0.4.1)
   7:          3649        1023120  [I (java.base@25.0.4.1)
   8:         22601         904040  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   9:          8815         760272  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
  10:            25         737680  [Ljava.nio.ByteBuffer; (java.base@25.0.4.1)
  11:         16784         671360  java.util.HashMap$Node (java.base@25.0.4.1)
  12:          1896         643256  [J (java.base@25.0.4.1)
  13:          7771         621680  java.util.LinkedHashMap (java.base@25.0.4.1)
  14:          9509         532504  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  15:           152         351632  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  16:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  17:          3438         302544  io.netty.buffer.PoolSubpage
  18:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  19:          1528         220032  java.lang.reflect.Method (java.base@25.0.4.1)
  20:            38         213600  [Lio.netty.channel.uring.MsgHdrMemory;
  21:          4587         146784  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  22:          3581         143240  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  23:           264         142312  [C (java.base@25.0.4.1)
  24:          2210         141440  java.lang.invoke.MemberName (java.base@25.0.4.1)
  25:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  26:          2194         122864  java.lang.invoke.MethodType (java.base@25.0.4.1)
  27:          7081         113296  java.util.jar.Attributes (java.base@25.0.4.1)
  28:          1369         106040  [Ljava.lang.String; (java.base@25.0.4.1)
  29:          2194         105312  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  30:          2133          76272  [Ljava.lang.Class; (java.base@25.0.4.1)
  31:          1307          73192  java.util.HashMap (java.base@25.0.4.1)
  32:           518          68376  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  33:          1357          65136  java.lang.invoke.LambdaForm$Name (java.base@25.0.4.1)
...
Total        581863       45087280
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 2.63 MiB | 7.25 MiB |
| `Metaspace` | 26.95 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 7.17 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.29 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 0.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 242.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 3.01 MiB | 116.38 MiB |

