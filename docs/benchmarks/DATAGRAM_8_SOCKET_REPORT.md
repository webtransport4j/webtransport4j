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
| **Total Datagrams Received** | 457,101 packets |
| **Total Ingested Payload** | 468,071,424 bytes (446.39 MiB / 0.44 GiB) |
| **Active Sockets (SO_REUSEPORT)** | 7 socket(s) |
| **Active Transfer Duration** | 0.887 s |
| **Data Throughput** | **503.35 MiB/s** |
| **Packet Throughput** | **515,429.3 datagrams/s** |

### 1.1. Per-Socket Datagram Distribution

| Socket ID | Datagrams Received | Share |
| :--- | :--- | :--- |
| `8838840e` | 80,132 packets | 17.5% |
| `2df952ec` | 78,987 packets | 17.3% |
| `b7b0200f` | 49,364 packets | 10.8% |
| `84b64174` | 48,963 packets | 10.7% |
| `15b86d7e` | 62,832 packets | 13.7% |
| `dcabe5c6` | 69,828 packets | 15.3% |
| `c76a750a` | 66,995 packets | 14.7% |

### 2. Garbage Collection & Heap Performance

| Metric | Value | Verdict |
| :--- | :--- | :--- |
| **GC Collections** | **12** | Collections recorded |
| **Total GC Pause Time** | **0 ms** | 0 ms recorded by pause MXBeans (millisecond resolution) |
| **Total ZGC Cycle Time** | **73 ms** | Concurrent cycle time |
| **Initial Used Heap** | 10,485,760 bytes (10.00 MiB) | Baseline before flood |
| **Peak Used Heap** | 455,081,984 bytes (434.00 MiB) | Maximum heap during flood |
| **Final Used Heap** | 180,355,072 bytes (172.00 MiB) | Steady-state post-flood |
| **Heap Delta** | 169,869,312 bytes (162.00 MiB) | Net change |
| **Off-Heap Direct Memory** | 29,360,128 bytes (28.00 MiB) | Netty direct buffer pool |

### 3. Active Garbage Collector Details

| Collector Name | Collection Count | Collection Time |
| :--- | :--- | :--- |
| `ZGC Minor Pauses` | 0 | 0 ms |
| `ZGC Major Pauses` | 10 | 0 ms |
| `ZGC Major Cycles` | 2 | 73 ms |
| `ZGC Minor Cycles` | 0 | 0 ms |

### 4. Thread-by-Thread Allocations During Flood

| Thread Name | Bytes Allocated | MiB Allocated |
| :--- | :--- | :--- |
| `multiThreadIoEventLoopGroup-2-7` | 65,309,448 bytes | 62.28 MiB |
| `multiThreadIoEventLoopGroup-2-6` | 64,283,696 bytes | 61.31 MiB |
| `multiThreadIoEventLoopGroup-2-2` | 56,931,248 bytes | 54.29 MiB |
| `multiThreadIoEventLoopGroup-2-4` | 54,490,712 bytes | 51.97 MiB |
| `multiThreadIoEventLoopGroup-2-5` | 51,385,432 bytes | 49.00 MiB |
| `multiThreadIoEventLoopGroup-2-3` | 40,110,472 bytes | 38.25 MiB |
| `multiThreadIoEventLoopGroup-2-1` | 39,785,088 bytes | 37.94 MiB |
| `main` | 188,512 bytes | 0.18 MiB |
| `wt-rate-limit-reloader` | 47,064 bytes | 0.04 MiB |
| `wt-udp-error-metrics-poller` | 30,768 bytes | 0.03 MiB |
| `wt-tls-cert-watcher` | 3,064 bytes | 0.00 MiB |
| `multiThreadIoEventLoopGroup-2-8` | 0 bytes | 0.00 MiB |
| `Common-Cleaner` | 0 bytes | 0.00 MiB |
| `Signal Dispatcher` | 0 bytes | 0.00 MiB |
| `Notification Thread` | 0 bytes | 0.00 MiB |
| `Reference Handler` | 0 bytes | 0.00 MiB |
| `Finalizer` | 0 bytes | 0.00 MiB |

### 5. Top Classes In Heap (Post-Flood Histogram)

```
 num     #instances         #bytes  class name (module)
-------------------------------------------------------
   1:         93671        9728328  [B (java.base@25.0.4.1)
   2:          1815        5775264  [J (java.base@25.0.4.1)
   3:          8356        3251672  [Ljava.lang.Object; (java.base@25.0.4.1)
   4:         71824        1723776  java.lang.String (java.base@25.0.4.1)
   5:          3592        1399080  [I (java.base@25.0.4.1)
   6:          6667        1319560  java.lang.Class (java.base@25.0.4.1)
   7:         22580         903200  java.util.concurrent.ConcurrentHashMap$Node (java.base@25.0.4.1)
   8:          8817         760176  [Ljava.util.HashMap$Node; (java.base@25.0.4.1)
   9:         16759         670360  java.util.HashMap$Node (java.base@25.0.4.1)
  10:          7770         621600  java.util.LinkedHashMap (java.base@25.0.4.1)
  11:          9482         530992  java.util.LinkedHashMap$Entry (java.base@25.0.4.1)
  12:          7168         516096  io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket
  13:           156         352208  [Ljava.util.concurrent.ConcurrentHashMap$Node; (java.base@25.0.4.1)
  14:          7669         306760  java.util.Hashtable$Entry (java.base@25.0.4.1)
  15:          3439         302632  io.netty.buffer.PoolSubpage
  16:          2013         225456  java.security.Provider$Service (java.base@25.0.4.1)
  17:          1516         218304  java.lang.reflect.Method (java.base@25.0.4.1)
  18:          4588         146816  java.security.Provider$ServiceKey (java.base@25.0.4.1)
  19:          2245         143680  java.lang.invoke.MemberName (java.base@25.0.4.1)
  20:          3580         143200  java.util.concurrent.locks.ReentrantLock$NonfairSync (java.base@25.0.4.1)
  21:           264         142312  [C (java.base@25.0.4.1)
  22:           114         134544  [Ljava.util.Hashtable$Entry; (java.base@25.0.4.1)
  23:          1368         122328  [Ljava.lang.String; (java.base@25.0.4.1)
  24:          2174         121744  java.lang.invoke.MethodType (java.base@25.0.4.1)
  25:          7080         113280  java.util.jar.Attributes (java.base@25.0.4.1)
  26:          2174         104352  jdk.internal.util.WeakReferenceKey (java.base@25.0.4.1)
  27:          2103          74904  [Ljava.lang.Class; (java.base@25.0.4.1)
  28:          1307          73192  java.util.HashMap (java.base@25.0.4.1)
  29:          1449          69552  java.lang.invoke.LambdaForm$Name (java.base@25.0.4.1)
  30:           510          67320  [Ljava.lang.ref.SoftReference; (java.base@25.0.4.1)
  31:             1          65672  [Lio.netty.channel.epoll.EpollIoEvent;
  32:           562          62944  java.lang.reflect.Field (java.base@25.0.4.1)
  33:          1559          62360  sun.security.util.ObjectIdentifier (java.base@25.0.4.1)
...
Total        383740       33363096
```

### 6. Memory Pools Usage

| Memory Pool | Used (MiB) | Max (MiB) |
| :--- | :--- | :--- |
| `CodeHeap 'non-nmethods'` | 3.16 MiB | 7.25 MiB |
| `Metaspace` | 26.81 MiB | N/A |
| `CodeHeap 'profiled nmethods'` | 5.53 MiB | 116.38 MiB |
| `Compressed Class Space` | 5.31 MiB | 1024.00 MiB |
| `ZGC Old Generation` | 74.00 MiB | 4096.00 MiB |
| `ZGC Young Generation` | 108.00 MiB | 4096.00 MiB |
| `CodeHeap 'non-profiled nmethods'` | 2.73 MiB | 116.38 MiB |

