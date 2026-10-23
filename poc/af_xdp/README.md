# AF_XDP (eXpress Data Path) Native Transport for WebTransport4J

This module provides a production-grade **Kernel-Bypass Native Data Plane** for WebTransport and QUIC, replacing the Linux UDP socket layer with **AF_XDP (XSK)**, **Netty's DatagramChannel architecture**, and **Java 25 Foreign Function & Memory (FFM / Project Panama)**.

---

## 1. Architecture

```
[ NIC Hardware RX Ring ]
           │
           ▼
[ eBPF Driver Hook: xdp_webtransport.bpf.o ]
           │
  ┌────────┴───────────────────────────┐
  │ Matches UDP Dst Port 443?          │
  ▼ YES (XDP_REDIRECT)                 ▼ NO (XDP_PASS)
[ AF_XDP Socket (XSK) ]        [ Normal Linux Stack (SSH, ARP, DNS) ]
           │
           ▼ Zero-Copy DMA
[ Off-Heap UMEM (Arena.ofShared) ]
           │
           ▼
[ AfXdpFrameParser (Zero-Copy) ]
           │
           ▼
[ AfXdpDatagramChannel ]
           │
  ┌────────┴───────────────────────────┐
  │ Inbound:                           │ Outbound:
  │ - Extract IP & Port                │ - Intercept write(DatagramPacket)
  │ - Auto-learn MAC in neighbor table │ - Assemble Eth + IP + UDP headers
  │ - Wrap UMEM into AfXdpFrameByteBuf │ - Push frame to UMEM TX ring
  │ - fireChannelRead(DatagramPacket)  │
  ▼                                    ▼
[ Netty ChannelPipeline: QuicServerCodec / WebTransportSession ]
```

---

## 2. Directory Structure

```
poc/af_xdp/
├── bpf/
│   ├── xdp_webtransport.bpf.c       # eBPF kernel filter redirecting port 443
│   └── Makefile                     # Build & interface attachment scripts
└── README.md                        # Architecture & usage guide

src/main/java-25/io/github/webtransport4j/afxdp/
├── AfXdpDatagramChannel.java        # Netty DatagramChannel implementation
├── AfXdpDatagramChannelConfig.java  # DatagramChannelConfig implementation
├── AfXdpChannelOption.java          # Custom Netty ChannelOptions for AF_XDP
├── AfXdpFrameByteBuf.java           # Zero-copy ByteBuf wrapper with UMEM recycling
├── AfXdpFrameParser.java            # Zero-copy Eth/IP/UDP parser & frame builder
├── AfXdpPacketDispatcher.java       # Rx ring consumption & channel dispatch
└── AfXdpUmem.java                   # Thread-safe off-heap UMEM circular frame pool (FFM)

src/test/java-25/io/github/webtransport4j/afxdp/
├── AfXdpDatagramChannelTest.java    # Full Netty pipeline integration test suite
├── AfXdpZeroCopyPocTest.java        # Parser regression test suite & microbenchmark
├── AfXdpEndToEndBenchmarkTest.java  # End-to-end ingestion microbenchmark
└── FullAfXdpWebTransportApp.java    # Standalone server & client application
```

---

## 3. How to Use `AfXdpDatagramChannel` in Netty

### Option A: Standard Netty Bootstrap
```java
EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

Bootstrap b = new Bootstrap();
b.group(group)
 .channel(AfXdpDatagramChannel.class)
 .handler(new ChannelInitializer<DatagramChannel>() {
     @Override
     protected void initChannel(DatagramChannel ch) {
         ch.pipeline().addLast(new MyDatagramHandler());
     }
 });

Channel channel = b.bind(443).sync().channel();
```

### Option B: WebTransportServer Transport Configuration
```java
System.setProperty("webtransport4j.server.transport", "afxdp");
WebTransportServer server = new WebTransportServer();
server.start();
```

---

## 4. Key Highlights & Optimizations

* **True Zero-Copy Ingestion**: Raw UMEM frames are wrapped directly into `AfXdpFrameByteBuf`. When Netty or QUIC finishes processing the packet, `DatagramPacket.release()` automatically recycles the frame address back to `AfXdpUmem` without any GC overhead or heap allocations.
* **Automatic Layer 2 MAC Learning**: Inbound Ethernet frames update a concurrent neighbor table with the sender's MAC address, allowing outbound QUIC responses (`writeAndFlush`) to be serialized with accurate Layer 2 Ethernet headers without needing an external ARP daemon.
* **Bi-directional Support**: Handles both RX ingestion (UMEM -> Netty pipeline) and TX transmission (Netty `DatagramPacket` -> Ethernet/IP/UDP serialization -> UMEM TX ring).
* **Multi-Release JAR**: Compiles natively with Java 25 Foreign Function & Memory API (FFM) while remaining dynamically linkable through `WebTransportServer`'s pluggable transport resolver.
