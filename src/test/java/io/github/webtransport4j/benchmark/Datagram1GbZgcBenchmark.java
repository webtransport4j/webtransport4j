package io.github.webtransport4j.benchmark;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.NettyWebTransportSession;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ClientConnectionHandler;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.NonNull;

/**
 * High-velocity Datagram Ingestion Benchmark targeting 1 GB (and multi-GB) data transfers.
 *
 * <p>Measures raw datagram ingestion throughput, GC cycles, pause times, and heap performance
 * under Java 25 with Compact Object Headers, ZGC, and optional multi-socket SO_REUSEPORT sharding.
 */
public final class Datagram1GbZgcBenchmark {

  private static final String HOST = "127.0.0.1";
  public static final long ONE_GB = 1024L * 1024L * 1024L; // 1,073,741,824 bytes (1 GiB)
  public static final int DEFAULT_DGRAM_PAYLOAD = 1024; // 1 KiB per datagram

  private Datagram1GbZgcBenchmark() {}

  /**
   * Main entry point. Supports 'server' and 'client' modes.
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: Datagram1GbZgcBenchmark <server|client> [options...]");
      System.exit(1);
    }

    String mode = args[0];
    if ("server".equalsIgnoreCase(mode)) {
      int port = args.length > 1 ? Integer.parseInt(args[1]) : 0;
      long targetBytes = args.length > 2 ? Long.parseLong(args[2]) : ONE_GB;
      String reportPath = args.length > 3 ? args[3] : "target/DATAGRAM_1GB_ZGC_REPORT.md";
      int socketCount = args.length > 4 ? Integer.parseInt(args[4]) : 1;
      runServer(port, targetBytes, reportPath, socketCount);
    } else if ("client".equalsIgnoreCase(mode)) {
      int port = args.length > 1 ? Integer.parseInt(args[1]) : 54321;
      long targetBytes = args.length > 2 ? Long.parseLong(args[2]) : ONE_GB;
      int payloadSize = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_DGRAM_PAYLOAD;
      int concurrency = args.length > 4 ? Integer.parseInt(args[4]) : 1;
      runClient(HOST, port, targetBytes, payloadSize, concurrency);
    } else {
      System.err.println("Unknown mode: " + mode);
      System.exit(1);
    }
  }

  /**
   * Runs the server process measuring 1 GB datagram ingestion and GC.
   */
  public static void runServer(int port, long targetBytes, String reportPath) throws Exception {
    runServer(port, targetBytes, reportPath, 1);
  }

  /**
   * Runs the server process measuring datagram ingestion and GC with configurable socketCount.
   */
  public static void runServer(int port, long targetBytes, String reportPath, int socketCount)
      throws Exception {
    if (new File("localhost-key.pem").exists() && new File("localhost.pem").exists()) {
      System.setProperty("webtransport4j.ssl.key.path", "localhost-key.pem");
      System.setProperty("webtransport4j.ssl.cert.path", "localhost.pem");
    } else {
      System.setProperty("webtransport4j.dev_mode", "true");
    }

    final AtomicLong receivedDatagrams = new AtomicLong(0);
    final AtomicLong receivedBytes = new AtomicLong(0);
    final AtomicLong lastPacketTime = new AtomicLong(0);
    final CountDownLatch completionLatch = new CountDownLatch(1);
    final AtomicLong startTime = new AtomicLong(0);
    final AtomicLong endTime = new AtomicLong(0);
    final AtomicBoolean finished = new AtomicBoolean(false);

    final Map<String, Long> initialGcCounts = new HashMap<>();
    final Map<String, Long> initialGcTimes = new HashMap<>();
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      initialGcCounts.put(gc.getName(), gc.getCollectionCount());
      initialGcTimes.put(gc.getName(), gc.getCollectionTime());
    }

    final MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
    final long initialHeapUsed = mem.getHeapMemoryUsage().getUsed();
    final AtomicLong peakHeapUsed = new AtomicLong(initialHeapUsed);

    final java.lang.management.ThreadMXBean baseThreadBean = ManagementFactory.getThreadMXBean();
    final com.sun.management.ThreadMXBean sunThreadBean =
        (baseThreadBean instanceof com.sun.management.ThreadMXBean)
            ? (com.sun.management.ThreadMXBean) baseThreadBean
            : null;
    final Map<Long, Long> threadStartAlloc = new ConcurrentHashMap<>();
    final Map<Long, String> threadNames = new ConcurrentHashMap<>();
    final Map<String, AtomicLong> socketDatagrams = new ConcurrentHashMap<>();
    final AtomicInteger activeSessions = new AtomicInteger(0);

    WebTransportHandler handler =
        new WebTransportHandler() {
          @Override
          public void onSessionReady(@NonNull WebTransportSession session) {
            activeSessions.incrementAndGet();
            String sockInfo = "socket-0";
            if (session instanceof NettyWebTransportSession) {
              QuicStreamChannel stmCh = ((NettyWebTransportSession) session).getConnectStream();
              if (stmCh != null && stmCh.parent() != null && stmCh.parent().parent() != null) {
                sockInfo = stmCh.parent().parent().id().asShortText();
              }
            }
            final String assignedSocket = sockInfo;
            System.out.printf(
                ">>> Server: Session %d ready on socket [%s]. Active sessions: %d%n",
                session.sessionId(), assignedSocket, activeSessions.get());
            if (session instanceof NettyWebTransportSession) {
              ((NettyWebTransportSession) session)
                  .onRawDatagram(
                      raw -> {
                        socketDatagrams
                            .computeIfAbsent(assignedSocket, k -> new AtomicLong())
                            .incrementAndGet();
                        if (startTime.compareAndSet(0, System.nanoTime())) {
                          System.out.println(">>> Server: First datagram received! Benchmarking started...");
                          if (sunThreadBean != null && sunThreadBean.isThreadAllocatedMemorySupported()) {
                            for (long id : sunThreadBean.getAllThreadIds()) {
                              threadStartAlloc.put(id, sunThreadBean.getThreadAllocatedBytes(id));
                              java.lang.management.ThreadInfo ti = sunThreadBean.getThreadInfo(id);
                              if (ti != null) {
                                threadNames.put(id, ti.getThreadName());
                              }
                            }
                          }
                        }
                        long dgramCount = receivedDatagrams.incrementAndGet();
                        long total = receivedBytes.addAndGet(raw.readableBytes());
                        lastPacketTime.set(System.nanoTime());

                        if ((dgramCount & 0x7FFF) == 0) {
                          long currentUsed = mem.getHeapMemoryUsage().getUsed();
                          peakHeapUsed.accumulateAndGet(currentUsed, Math::max);
                        }

                        if (total >= targetBytes && finished.compareAndSet(false, true)) {
                          endTime.set(System.nanoTime());
                          long finalUsed = mem.getHeapMemoryUsage().getUsed();
                          peakHeapUsed.accumulateAndGet(finalUsed, Math::max);
                          System.out.printf(
                              ">>> Server: Target %,d bytes (%.2f MB) reached in %,d datagrams!%n",
                              total, total / (1024.0 * 1024.0), dgramCount);
                          completionLatch.countDown();
                        }
                      });
            }
          }

          @Override
          public void onSessionClosed(@NonNull WebTransportSession session) {
            int remaining = activeSessions.decrementAndGet();
            System.out.printf(
                ">>> Server: Session %d closed. Remaining active sessions: %d%n",
                session.sessionId(), remaining);
            if (remaining <= 0) {
              if (endTime.get() == 0) {
                endTime.set(System.nanoTime());
              }
              completionLatch.countDown();
            }
          }
        };

    WebTransportServer server =
        new WebTransportServerBuilder()
            .host(HOST)
            .port(port)
            .socketCount(socketCount)
            .enableZeroGc(true)
            .defaultHandler(handler)
            .build();

    server.start();
    int actualPort = server.getPort();
    int boundSockets = server.getChannels().size();
    System.out.printf("SERVER_READY_PORT=%d%n", actualPort);
    System.out.printf("SERVER_SOCKET_COUNT=%d (active: %d)%n",
        server.getSocketCount(), boundSockets);
    System.out.flush();

    // Await completion of the transfer
    long timeoutSeconds = Math.max(120, (targetBytes / (15L * 1024 * 1024)) + 60);
    boolean completedOnTime = completionLatch.await(timeoutSeconds, TimeUnit.SECONDS);
    if (!completedOnTime) {
      System.err.println("⚠️ Warning: Transfer timed out before reaching target bytes.");
    }
    if (endTime.get() == 0) {
      endTime.set(System.nanoTime());
    }

    final long totalDgrams = receivedDatagrams.get();
    final long totalBytes = receivedBytes.get();
    long durationNanos = (finished.get() && endTime.get() > startTime.get())
        ? (endTime.get() - startTime.get())
        : ((lastPacketTime.get() > startTime.get())
            ? (lastPacketTime.get() - startTime.get())
            : (endTime.get() - startTime.get()));
    double durationSec = durationNanos / 1_000_000_000.0;
    if (durationSec <= 0) {
      durationSec = 0.001;
    }

    long finalHeapUsed = mem.getHeapMemoryUsage().getUsed();
    peakHeapUsed.accumulateAndGet(finalHeapUsed, Math::max);

    long totalGcCount = 0;
    Map<String, long[]> collectorDetails = new HashMap<>();
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      long prevCount = initialGcCounts.getOrDefault(gc.getName(), 0L);
      long prevTime = initialGcTimes.getOrDefault(gc.getName(), 0L);
      long curCount = gc.getCollectionCount();
      long curTime = gc.getCollectionTime();
      long deltaCount = Math.max(0, curCount - prevCount);
      long deltaTime = Math.max(0, curTime - prevTime);
      totalGcCount += deltaCount;
      collectorDetails.put(gc.getName(), new long[] {deltaCount, deltaTime});
    }

    long totalGcPauseMs = collectionTime(collectorDetails, "Pauses");
    long totalGcCycleMs = collectionTime(collectorDetails, "Cycles");

    Map<String, Long> threadAllocDeltas = new HashMap<>();
    if (sunThreadBean != null && sunThreadBean.isThreadAllocatedMemorySupported()) {
      for (long id : sunThreadBean.getAllThreadIds()) {
        long start = threadStartAlloc.getOrDefault(id, 0L);
        long end = sunThreadBean.getThreadAllocatedBytes(id);
        long diff = Math.max(0, end - start);
        java.lang.management.ThreadInfo ti = sunThreadBean.getThreadInfo(id);
        String name = ti != null ? ti.getThreadName() : threadNames.getOrDefault(id, "Thread-" + id);
        threadAllocDeltas.put(name, diff);
      }
    }

    java.util.List<String> topHistogram = new java.util.ArrayList<>();

    long directUsed = PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      if ("direct".equalsIgnoreCase(pool.getName())) {
        directUsed = Math.max(directUsed, pool.getMemoryUsed());
      }
    }

    double throughputMbPerSec = (totalBytes / (1024.0 * 1024.0)) / durationSec;
    double throughputDgramSec = totalDgrams / durationSec;

    printConsoleReport(
        totalDgrams,
        totalBytes,
        targetBytes,
        completedOnTime,
        durationSec,
        throughputMbPerSec,
        throughputDgramSec,
        totalGcCount,
        totalGcPauseMs,
        totalGcCycleMs,
        initialHeapUsed,
        finalHeapUsed,
        peakHeapUsed.get(),
        directUsed,
        boundSockets,
        socketDatagrams,
        collectorDetails,
        threadAllocDeltas,
        topHistogram);

    writeMarkdownReport(
        reportPath,
        totalDgrams,
        totalBytes,
        targetBytes,
        completedOnTime,
        durationSec,
        throughputMbPerSec,
        throughputDgramSec,
        totalGcCount,
        totalGcPauseMs,
        totalGcCycleMs,
        initialHeapUsed,
        finalHeapUsed,
        peakHeapUsed.get(),
        directUsed,
        boundSockets,
        socketDatagrams,
        collectorDetails,
        threadAllocDeltas,
        topHistogram);

    server.stop();
    System.exit(totalBytes >= targetBytes ? 0 : 1);
  }

  /**
   * Runs the client process streaming datagrams until target bytes is reached.
   */
  public static void runClient(String host, int port, long targetBytes, int payloadSize)
      throws Exception {
    runClient(host, port, targetBytes, payloadSize, 1);
  }

  /**
   * Runs the client process with multiple concurrent client connections.
   */
  public static void runClient(
      String host, int port, long targetBytes, int payloadSize, int concurrency)
      throws Exception {
    if (concurrency <= 1) {
      runSingleClient(host, port, targetBytes, payloadSize, 0, null, null);
      return;
    }

    System.out.printf(
        ">>> Client: Launching %d concurrent client connections streaming total %,d bytes...%n",
        concurrency, targetBytes);
    QuicSslContext sslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();
    int clientIoThreads = Math.min(16, Math.max(4, Runtime.getRuntime().availableProcessors()));
    EventLoopGroup sharedGroup = new NioEventLoopGroup(clientIoThreads);
    try {
      ExecutorService executor = Executors.newFixedThreadPool(concurrency);
      long perClientBytes = targetBytes / concurrency;
      CountDownLatch clientLatch = new CountDownLatch(concurrency);
      AtomicReference<Throwable> failure = new AtomicReference<>();

      for (int i = 0; i < concurrency; i++) {
        final int clientId = i;
        final long bytesToSend = (i == concurrency - 1)
            ? (targetBytes - perClientBytes * (concurrency - 1))
            : perClientBytes;
        executor.submit(() -> {
          try {
            runSingleClient(host, port, bytesToSend, payloadSize, clientId, sharedGroup, sslContext);
          } catch (Throwable t) {
            System.err.printf("Client %d failed: %s%n", clientId, t.getMessage());
            failure.compareAndSet(null, t);
          } finally {
            clientLatch.countDown();
          }
        });
      }

      long clientTimeoutSeconds = Math.max(300, (targetBytes / (15L * 1024 * 1024)) + 60);
      clientLatch.await(clientTimeoutSeconds, TimeUnit.SECONDS);
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);

      if (failure.get() != null) {
        throw new RuntimeException("Concurrent client streaming encountered an error", failure.get());
      }
      System.out.printf(">>> Client: All %d client streams successfully completed.%n", concurrency);
    } finally {
      sharedGroup.shutdownGracefully(0, 200, TimeUnit.MILLISECONDS);
    }
  }

  private static void runSingleClient(
      String host, int port, long targetBytes, int payloadSize, int clientId,
      EventLoopGroup sharedGroup, QuicSslContext sharedSsl)
      throws Exception {
    EventLoopGroup group = sharedGroup != null ? sharedGroup : new NioEventLoopGroup(2);
    try {
      QuicSslContext sslContext = sharedSsl != null ? sharedSsl :
          QuicSslContextBuilder.forClient()
              .trustManager(InsecureTrustManagerFactory.INSTANCE)
              .applicationProtocols(Http3.supportedApplicationProtocols())
              .build();

      ChannelHandler codec =
          Http3.newQuicClientCodecBuilder()
              .sslContext(sslContext)
              .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
              .initialMaxData(2147483648L)
              .initialMaxStreamDataBidirectionalLocal(1073741824L)
              .initialMaxStreamDataBidirectionalRemote(1073741824L)
              .initialMaxStreamsBidirectional(100)
              .datagram(65536, 65536)
              .build();

      Channel udpChannel =
          new Bootstrap()
              .group(group)
              .channel(NioDatagramChannel.class)
              .option(ChannelOption.SO_RCVBUF, 32 * 1024 * 1024)
              .option(ChannelOption.SO_SNDBUF, 32 * 1024 * 1024)
              .handler(codec)
              .bind(0)
              .sync()
              .channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);
      settings.put(0x2c7cf000L, 1L);
      settings.put(0x2b61L, 2147483648L);

      QuicChannel quicChannel =
          QuicChannel.newBootstrap(udpChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new Http3ClientConnectionHandler(
                                  null,
                                  null,
                                  new UnknownStreamHandlerFactory(),
                                  new DefaultHttp3SettingsFrame(settings),
                                  false,
                                  (id, value) -> true));
                    }
                  })
              .remoteAddress(new InetSocketAddress(host, port))
              .connect()
              .get();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];

      Http3Headers headers = new DefaultHttp3Headers();
      headers.method("CONNECT");
      headers.scheme("https");
      headers.path("/");
      headers.authority(host + ":" + port);
      headers.set(":protocol", "webtransport");

      QuicStreamChannel connectStream =
          Http3.newRequestStream(
                  quicChannel,
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new SimpleChannelInboundHandler<Object>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                  if (msg instanceof Http3HeadersFrame) {
                                    Http3HeadersFrame resp = (Http3HeadersFrame) msg;
                                    if ("200".equals(resp.headers().status().toString())) {
                                      sessionIdHolder[0] = ch.streamId();
                                      handshakeLatch.countDown();
                                    }
                                  }
                                }
                              });
                    }
                  })
              .sync()
              .getNow();

      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();

      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Client " + clientId + ": WebTransport handshake timed out");
      }
      long sessionId = sessionIdHolder[0];

      // Prepare template datagram buffer
      byte[] payload = new byte[payloadSize];
      Arrays.fill(payload, (byte) 0x57); // 'W'
      ByteBuf templateDgram = Unpooled.directBuffer(payloadSize + 8);
      WebTransportUtils.writeVarInt(templateDgram, sessionId / 4);
      templateDgram.writeBytes(payload);

      System.out.printf(
          ">>> Client %d: Connected! Streaming %,d bytes in %,d-byte datagrams...%n",
          clientId, targetBytes, payloadSize);

      long bytesSent = 0;
      long packetsSent = 0;
      final long clientStart = System.nanoTime();

      while (bytesSent < targetBytes && quicChannel.isOpen()) {
        while (!quicChannel.isWritable() && quicChannel.isOpen()) {
          quicChannel.flush();
          LockSupport.parkNanos(5_000);
        }
        quicChannel.write(templateDgram.retainedDuplicate(), quicChannel.voidPromise());
        bytesSent += payloadSize;
        packetsSent++;
        if ((packetsSent & 0x7F) == 0) {
          quicChannel.flush();
        }
      }
      quicChannel.flush();

      // Short drain to ensure trailing network packets are processed
      Thread.sleep(200);
      double clientDuration = (System.nanoTime() - clientStart) / 1_000_000_000.0;
      System.out.printf(
          ">>> Client %d: Flood completed in %.2f s (%,d packets, %,d bytes sent).%n",
          clientId, clientDuration, packetsSent, bytesSent);

      templateDgram.release();
      quicChannel.close().sync();
    } finally {
      if (sharedGroup == null) {
        group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
      }
    }
  }

  static long collectionTime(Map<String, long[]> collectorDetails, String kind) {
    return collectorDetails.entrySet().stream()
        .filter(entry -> entry.getKey().contains(kind))
        .mapToLong(entry -> entry.getValue()[1])
        .sum();
  }

  static String runStatus(long totalBytes, long targetBytes) {
    return totalBytes >= targetBytes ? "COMPLETE" : "INCOMPLETE";
  }

  static String objectHeaderMode() {
    try {
      com.sun.management.HotSpotDiagnosticMXBean bean =
          ManagementFactory.getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class);
      if (bean != null) {
        return Boolean.parseBoolean(bean.getVMOption("UseCompactObjectHeaders").getValue())
            ? "Compact Object Headers (JEP 450)" : "Standard Object Headers";
      }
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Older or non-HotSpot JVMs may not expose this option.
    }
    return "Unknown Object Header Mode";
  }

  static void printConsoleReport(
      long totalDgrams,
      long totalBytes,
      long targetBytes,
      boolean completedOnTime,
      double durationSec,
      double throughputMbPerSec,
      double throughputDgramSec,
      long totalGcCount,
      long totalGcPauseMs,
      long totalGcCycleMs,
      long initialHeapUsed,
      long finalHeapUsed,
      long peakHeapUsed,
      long directUsed,
      Map<String, long[]> collectorDetails,
      Map<String, Long> threadAllocDeltas,
      java.util.List<String> topHistogram) {
    printConsoleReport(
        totalDgrams, totalBytes, targetBytes, completedOnTime, durationSec,
        throughputMbPerSec, throughputDgramSec, totalGcCount, totalGcPauseMs,
        totalGcCycleMs, initialHeapUsed, finalHeapUsed, peakHeapUsed, directUsed,
        1, Collections.emptyMap(), collectorDetails, threadAllocDeltas, topHistogram);
  }

  static void printConsoleReport(
      long totalDgrams,
      long totalBytes,
      long targetBytes,
      boolean completedOnTime,
      double durationSec,
      double throughputMbPerSec,
      double throughputDgramSec,
      long totalGcCount,
      long totalGcPauseMs,
      long totalGcCycleMs,
      long initialHeapUsed,
      long finalHeapUsed,
      long peakHeapUsed,
      long directUsed,
      int activeSocketCount,
      Map<String, AtomicLong> socketDatagrams,
      Map<String, long[]> collectorDetails,
      Map<String, Long> threadAllocDeltas,
      java.util.List<String> topHistogram) {

    System.out.println();
    System.out.println("=========================================================================================");
    System.out.printf("   DATAGRAM TRANSFER BENCHMARK REPORT (JAVA %s + ZGC + %s)%n",
        System.getProperty("java.version"), objectHeaderMode());
    System.out.println("=========================================================================================");
    System.out.printf("Run Status:                 %s%n", runStatus(totalBytes, targetBytes));
    System.out.printf("Target Payload:             %,d bytes%n", targetBytes);
    System.out.printf("Completed On Time (latch):  %s%n", completedOnTime);
    System.out.printf("Target Reached:             %s%n", totalBytes >= targetBytes);
    System.out.printf("Total Datagrams Received:    %,d packets%n", totalDgrams);
    System.out.printf("Total Ingested Payload:      %,d bytes (%.2f MB / %.2f GiB)%n",
        totalBytes, totalBytes / (1024.0 * 1024.0), totalBytes / (1024.0 * 1024.0 * 1024.0));
    if (activeSocketCount > 1 || !socketDatagrams.isEmpty()) {
      System.out.printf("Active Sockets (SO_REUSEPORT): %d socket(s)%n",
          socketDatagrams.isEmpty() ? activeSocketCount : socketDatagrams.size());
    }
    System.out.printf("Active Transfer Duration:    %.3f seconds%n", durationSec);
    System.out.printf("Throughput (Data Rate):      %.2f MB/s%n", throughputMbPerSec);
    System.out.printf("Throughput (Packet Rate):    %,.1f datagrams/sec%n", throughputDgramSec);
    if (!socketDatagrams.isEmpty()) {
      System.out.println("Per-Socket Datagram Distribution:");
      socketDatagrams.entrySet().stream()
          .sorted(Map.Entry.<String, AtomicLong>comparingByKey())
          .forEach(e -> {
            long count = e.getValue().get();
            double pct = totalDgrams > 0 ? (count * 100.0 / totalDgrams) : 0.0;
            System.out.printf("  - Socket [%-10s] : %,d packets (%.1f%%)%n", e.getKey(), count, pct);
          });
    }
    System.out.println("-----------------------------------------------------------------------------------------");
    System.out.printf("Garbage Collection Cycles:   %d collections%n", totalGcCount);
    System.out.printf("Total GC Pause Time:         %d ms (MXBean millisecond resolution)%n",
        totalGcPauseMs);
    System.out.printf("Total ZGC Cycle Time:        %d ms%n", totalGcCycleMs);
    System.out.printf("Initial Used Heap:           %,d bytes (%.2f MB)%n",
        initialHeapUsed, initialHeapUsed / (1024.0 * 1024.0));
    System.out.printf("Peak Used Heap:              %,d bytes (%.2f MB)%n",
        peakHeapUsed, peakHeapUsed / (1024.0 * 1024.0));
    System.out.printf("Final Used Heap:             %,d bytes (%.2f MB)%n",
        finalHeapUsed, finalHeapUsed / (1024.0 * 1024.0));
    System.out.printf("Heap Memory Delta:           %,d bytes (%.2f MB)%n",
        (finalHeapUsed - initialHeapUsed), (finalHeapUsed - initialHeapUsed) / (1024.0 * 1024.0));
    System.out.printf("Direct Off-Heap Memory:      %,d bytes (%.2f MB)%n",
        directUsed, directUsed / (1024.0 * 1024.0));
    System.out.println("-----------------------------------------------------------------------------------------");
    System.out.println("Active Garbage Collectors (MXBeans):");
    for (Map.Entry<String, long[]> entry : collectorDetails.entrySet()) {
      System.out.printf("  - %-25s : %d collections, %d ms collection time%n",
          entry.getKey(), entry.getValue()[0], entry.getValue()[1]);
    }
    System.out.println("-----------------------------------------------------------------------------------------");
    System.out.println("Thread Heap Allocations During Flood (ThreadMXBean):");
    threadAllocDeltas.entrySet().stream()
        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
        .forEach(e -> {
          System.out.printf("  - %-35s : %,d bytes (%.2f MB)%n",
              e.getKey(), e.getValue(), e.getValue() / (1024.0 * 1024.0));
        });
    System.out.println("-----------------------------------------------------------------------------------------");
    System.out.println("Top Classes In Heap (jcmd GC.class_histogram):");
    for (String line : topHistogram) {
      System.out.println("  " + line);
    }
    System.out.println("=========================================================================================");
    System.out.println();
  }

  static String formatMemoryPoolMax(long maxBytes) {
    return maxBytes < 0 ? "N/A" : String.format(Locale.ROOT, "%.2f MiB", maxBytes / (1024.0 * 1024.0));
  }

  static void writeMarkdownReport(
      String filePath,
      long totalDgrams,
      long totalBytes,
      long targetBytes,
      boolean completedOnTime,
      double durationSec,
      double throughputMbPerSec,
      double throughputDgramSec,
      long totalGcCount,
      long totalGcPauseMs,
      long totalGcCycleMs,
      long initialHeapUsed,
      long finalHeapUsed,
      long peakHeapUsed,
      long directUsed,
      Map<String, long[]> collectorDetails,
      Map<String, Long> threadAllocDeltas,
      java.util.List<String> topHistogram) {
    writeMarkdownReport(
        filePath, totalDgrams, totalBytes, targetBytes, completedOnTime,
        durationSec, throughputMbPerSec, throughputDgramSec,
        totalGcCount, totalGcPauseMs, totalGcCycleMs,
        initialHeapUsed, finalHeapUsed, peakHeapUsed, directUsed,
        1, Collections.emptyMap(), collectorDetails, threadAllocDeltas, topHistogram);
  }

  static void writeMarkdownReport(
      String filePath,
      long totalDgrams,
      long totalBytes,
      long targetBytes,
      boolean completedOnTime,
      double durationSec,
      double throughputMbPerSec,
      double throughputDgramSec,
      long totalGcCount,
      long totalGcPauseMs,
      long totalGcCycleMs,
      long initialHeapUsed,
      long finalHeapUsed,
      long peakHeapUsed,
      long directUsed,
      int activeSocketCount,
      Map<String, AtomicLong> socketDatagrams,
      Map<String, long[]> collectorDetails,
      Map<String, Long> threadAllocDeltas,
      java.util.List<String> topHistogram) {

    File file = new File(filePath);
    if (file.getParentFile() != null) {
      file.getParentFile().mkdirs();
    }

    try (PrintWriter out = new PrintWriter(new FileWriter(file))) {
      double gib = targetBytes / (1024.0 * 1024.0 * 1024.0);
      out.printf(Locale.ROOT, "# WebTransport4J: %.2f GiB Datagram Ingestion Report (Java %s + ZGC)%n%n",
          gib, System.getProperty("java.version"));
      out.printf(Locale.ROOT, "Empirical benchmark measuring high-velocity transfer of **%.2f GiB** over"
          + " WebTransport Datagrams%n", gib);
      out.printf("running in an isolated dedicated server process with **ZGC** and **%s**.%n",
          objectHeaderMode());
      out.println();
      out.println("### 1. Ingestion & Throughput Metrics");
      out.println();
      out.println("| Metric | Value |");
      out.println("| :--- | :--- |");
      out.printf("| **Run Status** | **%s** |%n", runStatus(totalBytes, targetBytes));
      out.printf(Locale.ROOT, "| **Target Payload** | %,d bytes |%n", targetBytes);
      out.printf("| **Completed On Time (latch)** | %s |%n", completedOnTime);
      out.printf("| **Target Reached** | %s |%n", totalBytes >= targetBytes);
      out.printf(Locale.ROOT, "| **Total Datagrams Received** | %,d packets |%n", totalDgrams);
      out.printf(Locale.ROOT, "| **Total Ingested Payload** | %,d bytes (%.2f MiB / %.2f GiB) |%n",
          totalBytes, totalBytes / (1024.0 * 1024.0), totalBytes / (1024.0 * 1024.0 * 1024.0));
      if (activeSocketCount > 1 || !socketDatagrams.isEmpty()) {
        out.printf("| **Active Sockets (SO_REUSEPORT)** | %d socket(s) |%n",
            socketDatagrams.isEmpty() ? activeSocketCount : socketDatagrams.size());
      }
      out.printf(Locale.ROOT, "| **Active Transfer Duration** | %.3f s |%n", durationSec);
      out.printf(Locale.ROOT, "| **Data Throughput** | **%.2f MiB/s** |%n", throughputMbPerSec);
      out.printf(Locale.ROOT, "| **Packet Throughput** | **%,.1f datagrams/s** |%n", throughputDgramSec);
      out.println();
      if (!socketDatagrams.isEmpty()) {
        out.println("### 1.1. Per-Socket Datagram Distribution");
        out.println();
        out.println("| Socket ID | Datagrams Received | Share |");
        out.println("| :--- | :--- | :--- |");
        for (Map.Entry<String, AtomicLong> entry : socketDatagrams.entrySet()) {
          long count = entry.getValue().get();
          double pct = totalDgrams > 0 ? (count * 100.0 / totalDgrams) : 0.0;
          out.printf(Locale.ROOT, "| `%s` | %,d packets | %.1f%% |%n", entry.getKey(), count, pct);
        }
        out.println();
      }
      out.println("### 2. Garbage Collection & Heap Performance");
      out.println();
      out.println("| Metric | Value | Verdict |");
      out.println("| :--- | :--- | :--- |");
      out.printf(Locale.ROOT, "| **GC Collections** | **%d** | %s |%n",
          totalGcCount, totalGcCount == 0 ? "No collections recorded" : "Collections recorded");
      out.printf(Locale.ROOT, "| **Total GC Pause Time** | **%d ms** | %s |%n",
          totalGcPauseMs, totalGcPauseMs == 0
              ? "0 ms recorded by pause MXBeans (millisecond resolution)"
              : "Nonzero pause time recorded by pause MXBeans");
      out.printf(Locale.ROOT, "| **Total ZGC Cycle Time** | **%d ms** | Concurrent cycle time |%n",
          totalGcCycleMs);
      out.printf(Locale.ROOT, "| **Initial Used Heap** | %,d bytes (%.2f MiB) | Baseline before flood |%n",
          initialHeapUsed, initialHeapUsed / (1024.0 * 1024.0));
      out.printf(Locale.ROOT, "| **Peak Used Heap** | %,d bytes (%.2f MiB) | Maximum heap during flood |%n",
          peakHeapUsed, peakHeapUsed / (1024.0 * 1024.0));
      out.printf(Locale.ROOT, "| **Final Used Heap** | %,d bytes (%.2f MiB) | Steady-state post-flood |%n",
          finalHeapUsed, finalHeapUsed / (1024.0 * 1024.0));
      out.printf(Locale.ROOT, "| **Heap Delta** | %,d bytes (%.2f MiB) | Net change |%n",
          (finalHeapUsed - initialHeapUsed), (finalHeapUsed - initialHeapUsed) / (1024.0 * 1024.0));
      out.printf(Locale.ROOT, "| **Off-Heap Direct Memory** | %,d bytes (%.2f MiB) | Netty direct buffer pool |%n",
          directUsed, directUsed / (1024.0 * 1024.0));
      out.println();
      out.println("### 3. Active Garbage Collector Details");
      out.println();
      out.println("| Collector Name | Collection Count | Collection Time |");
      out.println("| :--- | :--- | :--- |");
      for (Map.Entry<String, long[]> entry : collectorDetails.entrySet()) {
        out.printf(Locale.ROOT, "| `%s` | %d | %d ms |%n",
            entry.getKey(), entry.getValue()[0], entry.getValue()[1]);
      }
      out.println();
      out.println("### 4. Thread-by-Thread Allocations During Flood");
      out.println();
      out.println("| Thread Name | Bytes Allocated | MiB Allocated |");
      out.println("| :--- | :--- | :--- |");
      threadAllocDeltas.entrySet().stream()
          .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
          .forEach(e -> {
            out.printf(Locale.ROOT, "| `%s` | %,d bytes | %.2f MiB |%n",
                e.getKey(), e.getValue(), e.getValue() / (1024.0 * 1024.0));
          });
      out.println();
      out.println("### 5. Top Classes In Heap (Post-Flood Histogram)");
      out.println();
      out.println("```");
      for (String line : topHistogram) {
        out.println(line);
      }
      out.println("```");
      out.println();
      out.println("### 6. Memory Pools Usage");
      out.println();
      out.println("| Memory Pool | Used (MiB) | Max (MiB) |");
      out.println("| :--- | :--- | :--- |");
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
        MemoryUsage usage = pool.getUsage();
        if (usage != null) {
          out.printf(Locale.ROOT, "| `%s` | %.2f MiB | %s |%n",
              pool.getName(), usage.getUsed() / (1024.0 * 1024.0), formatMemoryPoolMax(usage.getMax()));
        }
      }
      out.println();
    } catch (Exception e) {
      System.err.println("Failed to write markdown report: " + e.getMessage());
    }
  }
}
