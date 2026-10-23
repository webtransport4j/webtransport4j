package io.github.webtransport4j.afxdp;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import java.io.File;
import java.lang.foreign.MemorySegment;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Head-to-head benchmark comparing Netty {@link EpollDatagramChannel} (Linux Native Socket)
 * vs {@link AfXdpDatagramChannel} (Linux Native AF_XDP Kernel-Bypass).
 *
 * <p>Both channels execute the identical Netty pipeline workload:
 * Ingestion -> Header Parsing -> Inbound DatagramPacket -> Payload XOR Checksum -> Reference Release.
 */
public final class RealDatagramChannelBenchmark {

  private static final int DEFAULT_PACKET_COUNT = 300_000;
  private static final int PAYLOAD_SIZE = 1024; // 1 KiB
  private static final int SERVER_PORT = 54433;

  /**
   * Main entry point for the real datagram channel comparative benchmark.
   *
   * @param args command-line arguments (optional: packetCount, socketCount)
   * @throws Exception if benchmark fails
   */
  public static void main(String[] args) throws Exception {
    int packetCount = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PACKET_COUNT;
    int socketCount = args.length > 1 ? Integer.parseInt(args[1]) : 8;
    runBenchmark(packetCount, socketCount);
  }

  /**
   * Benchmark result container.
   */
  public static final class Result {
    private final String name;
    private final long packets;
    private final long bytes;
    private final double durationSec;
    private final double mbPerSec;
    private final double pps;
    private final long gcCount;
    private final long gcTimeMs;
    private final long userCpuMs;
    private final long kernelCpuMs;

    /** Constructs a benchmark result snapshot. */
    public Result(
        String name,
        long packets,
        long bytes,
        double durationSec,
        double mbPerSec,
        double pps,
        long gcCount,
        long gcTimeMs,
        long userCpuMs,
        long kernelCpuMs) {
      this.name = name;
      this.packets = packets;
      this.bytes = bytes;
      this.durationSec = durationSec;
      this.mbPerSec = mbPerSec;
      this.pps = pps;
      this.gcCount = gcCount;
      this.gcTimeMs = gcTimeMs;
      this.userCpuMs = userCpuMs;
      this.kernelCpuMs = kernelCpuMs;
    }

    public String getName() {
      return name;
    }

    public long getPackets() {
      return packets;
    }

    public long getBytes() {
      return bytes;
    }

    public double getDurationSec() {
      return durationSec;
    }

    public double getMbPerSec() {
      return mbPerSec;
    }

    public double getPps() {
      return pps;
    }

    public long getGcCount() {
      return gcCount;
    }

    public long getGcTimeMs() {
      return gcTimeMs;
    }

    public long getUserCpuMs() {
      return userCpuMs;
    }

    public long getKernelCpuMs() {
      return kernelCpuMs;
    }
  }

  /**
   * Executes the head-to-head benchmark for EpollDatagramChannel and AfXdpDatagramChannel.
   *
   * @param packetCount total datagrams to process
   */
  public static void runBenchmark(int packetCount) throws Exception {
    runBenchmark(packetCount, 8);
  }

  /**
   * Executes the head-to-head benchmark with configurable socketCount.
   *
   * @param packetCount total datagrams to process
   * @param socketCount number of sharded sockets to bind
   */
  public static void runBenchmark(int packetCount, int socketCount) throws Exception {
    System.out.println("==========================================================================");
    System.out.printf("⚡ HEAD-TO-HEAD BENCHMARK: EpollDatagramChannel vs AfXdpDatagramChannel ⚡%n");
    System.out.printf("   Sockets:     %d sockets (Multi-Socket Sharding)%n", socketCount);
    System.out.printf("   Workload:    Netty Pipeline DatagramPacket Ingestion + Checksum%n");
    System.out.printf("   Datagrams:   %,d packets (%d bytes each)%n", packetCount, PAYLOAD_SIZE);
    System.out.printf("   Total Data:  %.2f MB%n", (packetCount * (long) PAYLOAD_SIZE) / (1024.0 * 1024.0));
    System.out.println("==========================================================================");

    // Warmup JVM
    System.out.println(">>> Performing JIT compilation warmup (50,000 packets)...");
    runAfXdp(50_000, socketCount, true);
    if (Epoll.isAvailable()) {
      runEpoll(50_000, socketCount, true);
    }
    System.gc();
    Thread.sleep(1000);

    // 1. Benchmark EpollDatagramChannel
    Result epollResult = null;
    if (Epoll.isAvailable()) {
      System.out.printf("%n>>> [1/2] Running EpollDatagramChannel Benchmark (%d Sockets)...%n", socketCount);
      epollResult = runEpoll(packetCount, socketCount, false);
      printSingleResult(epollResult);
    } else {
      System.out.println("\n>>> Epoll is not available on this platform, skipping Epoll test.");
    }

    System.gc();
    Thread.sleep(1000);

    // 2. Benchmark AfXdpDatagramChannel
    System.out.printf("%n>>> [2/2] Running AfXdpDatagramChannel Benchmark (%d Sockets)...%n", socketCount);
    Result afxdpResult = runAfXdp(packetCount, socketCount, false);
    printSingleResult(afxdpResult);

    // Print Comparative Summary
    printComparison(epollResult, afxdpResult);
  }

  /**
   * Benchmarks Netty EpollDatagramChannel with 1 socket.
   */
  public static Result runEpoll(int packetCount, boolean isWarmup) throws Exception {
    return runEpoll(packetCount, 1, isWarmup);
  }

  /**
   * Benchmarks Netty EpollDatagramChannel with configurable socket count.
   */
  public static Result runEpoll(int packetCount, int socketCount, boolean isWarmup) throws Exception {
    final CountDownLatch latch = new CountDownLatch(socketCount);
    final AtomicLong packetsHandled = new AtomicLong();
    final AtomicLong bytesHandled = new AtomicLong();
    final AtomicLong checksumSum = new AtomicLong();

    EventLoopGroup group = new EpollEventLoopGroup(socketCount);
    EventLoopGroup clientGroup = new EpollEventLoopGroup(socketCount);

    Channel[] serverChannels = new Channel[socketCount];
    Channel[] clientChannels = new Channel[socketCount];
    int[] ports = new int[socketCount];
    final int perSocketPackets = packetCount / socketCount;

    try {
      for (int s = 0; s < socketCount; s++) {
        Bootstrap serverBs = new Bootstrap();
        serverBs
            .group(group)
            .channel(EpollDatagramChannel.class)
            .option(ChannelOption.SO_RCVBUF, 64 * 1024 * 1024)
            .option(ChannelOption.RCVBUF_ALLOCATOR, new io.netty.channel.FixedRecvByteBufAllocator(4096))
            .handler(
                new SimpleChannelInboundHandler<DatagramPacket>(true) {
                  private long localCount = 0;

                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                    ByteBuf buf = msg.content();
                    int len = buf.readableBytes();
                    bytesHandled.addAndGet(len);

                    long accum = 0;
                    while (buf.readableBytes() >= 8) {
                      accum ^= buf.readLong();
                    }
                    while (buf.isReadable()) {
                      accum ^= buf.readByte();
                    }
                    checksumSum.addAndGet(accum);

                    packetsHandled.incrementAndGet();
                    if (++localCount == perSocketPackets) {
                      latch.countDown();
                    }
                  }
                });

        serverChannels[s] = serverBs.bind("127.0.0.1", 0).sync().channel();
        ports[s] = ((InetSocketAddress) serverChannels[s].localAddress()).getPort();

        Bootstrap clientBs = new Bootstrap();
        clientBs
            .group(clientGroup)
            .channel(EpollDatagramChannel.class)
            .option(ChannelOption.SO_SNDBUF, 64 * 1024 * 1024)
            .handler(
                new SimpleChannelInboundHandler<DatagramPacket>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {}
                });
        clientChannels[s] = clientBs.bind(0).sync().channel();
      }

      byte[] payload = new byte[PAYLOAD_SIZE];
      for (int i = 0; i < payload.length; i++) {
        payload[i] = (byte) (i & 0xFF);
      }

      final long gcBefore = getGcCount();
      final long gcTimeBefore = getGcTime();
      final long[] cpuBefore = getProcessCpuTicks();

      final long startNanos = System.nanoTime();

      ExecutorService feeder = Executors.newFixedThreadPool(socketCount);
      for (int s = 0; s < socketCount; s++) {
        final int idx = s;
        feeder.submit(
            () -> {
              Channel clientCh = clientChannels[idx];
              InetSocketAddress target = new InetSocketAddress("127.0.0.1", ports[idx]);
              for (int i = 0; i < perSocketPackets; i++) {
                ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(PAYLOAD_SIZE);
                buf.writeBytes(payload);
                clientCh.write(new DatagramPacket(buf, target));
                if ((i & 0x7F) == 0) {
                  clientCh.flush();
                }
              }
              clientCh.flush();
            });
      }
      feeder.shutdown();
      feeder.awaitTermination(10, TimeUnit.SECONDS);
      latch.await(300, TimeUnit.MILLISECONDS);
      long endNanos = System.nanoTime();

      long[] cpuAfter = getProcessCpuTicks();
      long gcAfter = getGcCount();
      long gcTimeAfter = getGcTime();

      double durationSec = (endNanos - startNanos) / 1_000_000_000.0;
      long actualPackets = packetsHandled.get();
      long actualBytes = bytesHandled.get();
      double mbPerSec = (actualBytes / (1024.0 * 1024.0)) / durationSec;
      double pps = actualPackets / durationSec;

      long userCpuMs = ticksToMs(cpuAfter[0] - cpuBefore[0]);
      long kernelCpuMs = ticksToMs(cpuAfter[1] - cpuBefore[1]);

      String title = socketCount > 1
          ? "EpollDatagramChannel (" + socketCount + " Sharded Sockets)"
          : "EpollDatagramChannel (Linux Native Socket)";
      return new Result(
          title,
          actualPackets,
          actualBytes,
          durationSec,
          mbPerSec,
          pps,
          gcAfter - gcBefore,
          gcTimeAfter - gcTimeBefore,
          userCpuMs,
          kernelCpuMs);
    } finally {
      for (Channel ch : clientChannels) {
        if (ch != null) {
          ch.close();
        }
      }
      for (Channel ch : serverChannels) {
        if (ch != null) {
          ch.close();
        }
      }
      clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }

  /**
   * Benchmarks Netty AfXdpDatagramChannel with 1 socket.
   */
  public static Result runAfXdp(int packetCount, boolean isWarmup) throws Exception {
    return runAfXdp(packetCount, 1, isWarmup);
  }

  /**
   * Benchmarks Netty AfXdpDatagramChannel with configurable socket count.
   */
  public static Result runAfXdp(int packetCount, int socketCount, boolean isWarmup) throws Exception {
    final CountDownLatch latch = new CountDownLatch(socketCount);
    final AtomicLong packetsHandled = new AtomicLong();
    final AtomicLong bytesHandled = new AtomicLong();
    final AtomicLong checksumSum = new AtomicLong();

    EventLoopGroup group = new DefaultEventLoopGroup(socketCount);
    AfXdpDatagramChannel[] channels = new AfXdpDatagramChannel[socketCount];
    AfXdpUmem[] umems = new AfXdpUmem[socketCount];
    long[] frameOffsets = new long[socketCount];
    int[] wireLens = new int[socketCount];

    byte[] payload = new byte[PAYLOAD_SIZE];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i & 0xFF);
    }

    InetSocketAddress sender = new InetSocketAddress("2001:db8::1", 54321);
    byte[] clientMac = new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};
    byte[] serverMac = new byte[] {0x02, 0x00, 0x00, 0x00, 0x00, 0x01};

    final int perSocketPackets = packetCount / socketCount;

    try {
      for (int s = 0; s < socketCount; s++) {
        final int idx = s;
        umems[s] = new AfXdpUmem(4096, 2048);
        Bootstrap serverBs = new Bootstrap();
        serverBs
            .group(group)
            .channelFactory(() -> new AfXdpDatagramChannel(umems[idx]))
            .handler(
                new SimpleChannelInboundHandler<DatagramPacket>(true) {
                  private long localCount = 0;

                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                    ByteBuf buf = msg.content();
                    int len = buf.readableBytes();
                    bytesHandled.addAndGet(len);

                    long accum = 0;
                    while (buf.readableBytes() >= 8) {
                      accum ^= buf.readLong();
                    }
                    while (buf.isReadable()) {
                      accum ^= buf.readByte();
                    }
                    checksumSum.addAndGet(accum);

                    packetsHandled.incrementAndGet();
                    if (++localCount == perSocketPackets) {
                      latch.countDown();
                    }
                  }
                });

        channels[s] = (AfXdpDatagramChannel) serverBs.bind(SERVER_PORT + s).sync().channel();

        frameOffsets[s] = umems[s].leaseFrame();
        ByteBuf payloadBuffer = Unpooled.wrappedBuffer(payload);
        MemorySegment seg = umems[s].getFrameSlice(frameOffsets[s], 4096);
        InetSocketAddress recipient = new InetSocketAddress("2001:db8::2", SERVER_PORT + s);
        wireLens[s] =
            AfXdpFrameParser.writeFrame(seg, clientMac, serverMac, sender, recipient, payloadBuffer);
      }

      final long gcBefore = getGcCount();
      final long gcTimeBefore = getGcTime();
      final long[] cpuBefore = getProcessCpuTicks();

      final long startNanos = System.nanoTime();

      ExecutorService feeder = Executors.newFixedThreadPool(socketCount);
      for (int s = 0; s < socketCount; s++) {
        final int idx = s;
        feeder.submit(
            () -> {
              AfXdpDatagramChannel ch = channels[idx];
              long frameOffset = frameOffsets[idx];
              int wireLength = wireLens[idx];
              for (int i = 0; i < perSocketPackets; i++) {
                ch.feedRxFrame(frameOffset, wireLength);
              }
            });
      }
      feeder.shutdown();

      boolean finished = latch.await(20, TimeUnit.SECONDS);
      long endNanos = System.nanoTime();

      long[] cpuAfter = getProcessCpuTicks();
      long gcAfter = getGcCount();
      long gcTimeAfter = getGcTime();

      double durationSec = (endNanos - startNanos) / 1_000_000_000.0;
      long actualPackets = packetsHandled.get();
      long actualBytes = bytesHandled.get();
      double mbPerSec = (actualBytes / (1024.0 * 1024.0)) / durationSec;
      double pps = actualPackets / durationSec;

      long userCpuMs = ticksToMs(cpuAfter[0] - cpuBefore[0]);
      long kernelCpuMs = ticksToMs(cpuAfter[1] - cpuBefore[1]);

      String title = socketCount > 1
          ? "AfXdpDatagramChannel (" + socketCount + " Sharded Sockets)"
          : "AfXdpDatagramChannel (Kernel-Bypass Native)";
      return new Result(
          title,
          actualPackets,
          actualBytes,
          durationSec,
          mbPerSec,
          pps,
          gcAfter - gcBefore,
          gcTimeAfter - gcTimeBefore,
          userCpuMs,
          kernelCpuMs);
    } finally {
      for (AfXdpDatagramChannel ch : channels) {
        if (ch != null) {
          ch.close();
        }
      }
      for (AfXdpUmem u : umems) {
        if (u != null) {
          u.close();
        }
      }
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }

  private static void printSingleResult(Result r) {
    System.out.printf("   Channel:         %s%n", r.getName());
    System.out.printf("   Packets Handled: %,d packets%n", r.getPackets());
    System.out.printf("   Data Processed:  %.2f MB%n", r.getBytes() / (1024.0 * 1024.0));
    System.out.printf("   Duration:        %.4f s%n", r.getDurationSec());
    System.out.printf("   Throughput:      %,.2f MB/s%n", r.getMbPerSec());
    System.out.printf("   Packet Rate:     %,.0f PPS (%.2f Mpps)%n", r.getPps(), r.getPps() / 1_000_000.0);
    System.out.printf("   GC Cycles:       %d collections (%d ms pause)%n", r.getGcCount(), r.getGcTimeMs());
    System.out.printf("   CPU User/Kernel: %d ms / %d ms%n", r.getUserCpuMs(), r.getKernelCpuMs());
  }

  private static void printComparison(Result epoll, Result afxdp) {
    System.out.println("\n==========================================================================");
    System.out.println("📊 HEAD-TO-HEAD COMPARISON SUMMARY 📊");
    System.out.println("==========================================================================");
    System.out.printf(
        "%-24s | %-24s | %-24s | %-12s%n",
        "Metric", "EpollDatagramChannel", "AfXdpDatagramChannel", "Delta / Speedup");
    System.out.println(
        "-------------------------+--------------------------+--------------------------+-------------");

    if (epoll != null) {
      double tputSpeedup = afxdp.getMbPerSec() / Math.max(1.0, epoll.getMbPerSec());
      double ppsSpeedup = afxdp.getPps() / Math.max(1.0, epoll.getPps());

      System.out.printf("%-24s | %,18.2f MB/s | %,18.2f MB/s | %6.2fx 🚀%n",
          "Throughput", epoll.getMbPerSec(), afxdp.getMbPerSec(), tputSpeedup);
      System.out.printf("%-24s | %,18.0f PPS  | %,18.0f PPS  | %6.2fx 🚀%n",
          "Packet Rate", epoll.getPps(), afxdp.getPps(), ppsSpeedup);
      System.out.printf("%-24s | %18.4f s    | %18.4f s    | %6.2fx faster%n",
          "Elapsed Duration",
          epoll.getDurationSec(),
          afxdp.getDurationSec(),
          epoll.getDurationSec() / afxdp.getDurationSec());
      System.out.printf("%-24s | %18d ms   | %18d ms   | %s%n",
          "Kernel CPU Time", epoll.getKernelCpuMs(), afxdp.getKernelCpuMs(),
          afxdp.getKernelCpuMs() < epoll.getKernelCpuMs() ? "Kernel bypass 👍" : "Equal");
      System.out.printf("%-24s | %18d      | %18d      | %s%n",
          "GC Collections", epoll.getGcCount(), afxdp.getGcCount(),
          afxdp.getGcCount() == 0 ? "Zero GC 👍" : "Minimal");
    } else {
      System.out.printf("%-24s | %-24s | %,18.2f MB/s | N/A%n", "Throughput", "N/A", afxdp.getMbPerSec());
      System.out.printf("%-24s | %-24s | %,18.0f PPS  | N/A%n", "Packet Rate", "N/A", afxdp.getPps());
    }
    System.out.println("==========================================================================");
  }

  private static long getGcCount() {
    long count = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      long c = gc.getCollectionCount();
      if (c > 0) {
        count += c;
      }
    }
    return count;
  }

  private static long getGcTime() {
    long time = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      long t = gc.getCollectionTime();
      if (t > 0) {
        time += t;
      }
    }
    return time;
  }

  private static long[] getProcessCpuTicks() {
    try {
      String stat = new String(Files.readAllBytes(new File("/proc/self/stat").toPath()));
      String[] parts = stat.substring(stat.lastIndexOf(')') + 2).split(" ");
      long utime = Long.parseLong(parts[11]);
      long stime = Long.parseLong(parts[12]);
      return new long[] {utime, stime};
    } catch (Exception e) {
      return new long[] {0, 0};
    }
  }

  private static long ticksToMs(long ticks) {
    return ticks * 10; // Assuming 100 Hz clock tick
  }
}
