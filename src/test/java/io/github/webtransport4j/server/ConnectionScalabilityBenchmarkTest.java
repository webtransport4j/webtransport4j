package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance benchmark test: N parallel QUIC connections × M streams each.
 * Reports CPU time, JVM heap, GC pause count, and throughput for
 * 1, 10, 100 and 1000 concurrent connections.
 */
public class ConnectionScalabilityBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(ConnectionScalabilityBenchmarkTest.class);

  // Each connection sends this many bidirectional ping-pong exchanges per stream
  private static final int STREAMS_PER_CONNECTION = 4;
  private static final int MESSAGES_PER_STREAM = 50;   // total messages per stream
  private static final int PIPELINE_DEPTH = 16;        // in-flight messages before waiting for ACK
  private static final String PAYLOAD = "BENCH-PING-1234567890"; // 21 bytes

  private WebTransportServer server;
  private int port;
  private Thread serverThread;

  // ── Echo server handler ─────────────────────────────────────────────────
  private static class EchoHandler implements WebTransportHandler {
    @Override
    public void onIncomingStream(@NonNull WebTransportSession session,
        @NonNull WebTransportStream stream) {
      if (stream.isBidirectional()) {
        stream.onData(data -> stream.write(data.readBytes()));
      }
    }
    @Override public void onSessionReady(@NonNull WebTransportSession s) {}
    @Override public void onSessionClosed(@NonNull WebTransportSession s) {}
  }

  // ── JVM metric snapshot ─────────────────────────────────────────────────
  private static final class JvmSnapshot {
    final long heapUsedBytes;
    final long gcCount;
    final long gcTimeMs;
    final long cpuNs;

    JvmSnapshot() {
      MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
      this.heapUsedBytes = mem.getHeapMemoryUsage().getUsed();
      long count = 0, time = 0;
      for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
        long c = gc.getCollectionCount();
        long t = gc.getCollectionTime();
        if (c > 0) { count += c; time += t; }
      }
      this.gcCount = count;
      this.gcTimeMs = time;
      this.cpuNs = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
    }
  }

  // ── Server lifecycle ────────────────────────────────────────────────────
  @Before
  public void setUp() throws Exception {
    System.setProperty("webtransport4j.server.port", "0");
    System.setProperty("webtransport4j.webtransport.enable_server_push", "false");
    System.setProperty("webtransport4j.webtransport.max_sessions_per_connection", "1000");
    server = new WebTransportServer();
    server.registerHandler("/bench", new EchoHandler());
    serverThread = new Thread(() -> {
      try { server.start(); } catch (Exception e) { logger.error("Server error", e); }
    });
    serverThread.start();
    long deadline = System.currentTimeMillis() + 15_000;
    while (server.getPort() == 0 && System.currentTimeMillis() < deadline) Thread.sleep(50);
    port = server.getPort();
    logger.info("✅ Benchmark server started on port {}", port);
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) server.stop();
    if (serverThread != null) serverThread.join(3_000);
  }

  // ── Main benchmark entry-point ───────────────────────────────────────────
  @Test
  public void benchmarkConnectionScalability() throws Exception {
    // Probe OS open-file-descriptor limit
    long maxFd = probeMaxOpenFiles();
    System.out.println();
    System.out.println("================================================================");
    System.out.println("⚡ WEBTRANSPORT4J CONNECTION SCALABILITY BENCHMARK ⚡");
    System.out.printf("   %d streams/conn × %d messages/stream × pipeline depth %d%n",
        STREAMS_PER_CONNECTION, MESSAGES_PER_STREAM, PIPELINE_DEPTH);
    System.out.printf("   OS fd limit: %s%n",
        maxFd == Long.MAX_VALUE ? "unlimited" : String.valueOf(maxFd));
    System.out.println("================================================================");
    printTableHeader();

    // Each QUIC connection needs ~3 fds (UDP socket + pipe + epoll).
    // Leave 512 headroom for JVM internals.
    long headroom = 512;
    long availFd = maxFd == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0, maxFd - headroom);

    for (int n : new int[]{1, 10, 100, 1000, 10_000, 100_000}) {
      long required = (long) n * 3;
      if (required > availFd) {
        System.out.printf("%-8d %-10s %-8s %-8s %-10s %-12s %-12s %-8s %-10s  ⚠️ SKIPPED (OS fd limit %d < %d required)%n",
            n, "-", "-", "-", "-", "-", "-", "-", "-", maxFd, required);
        continue;
      }
      runTier(n);
    }

    System.out.println("================================================================");
  }

  /** Returns the process fd limit, or Long.MAX_VALUE if unlimited / undetectable. */
  private static long probeMaxOpenFiles() {
    try {
      // Works on macOS and Linux
      java.lang.management.OperatingSystemMXBean os =
          ManagementFactory.getOperatingSystemMXBean();
      if (os instanceof com.sun.management.UnixOperatingSystemMXBean) {
        long lim = ((com.sun.management.UnixOperatingSystemMXBean) os).getMaxFileDescriptorCount();
        return lim > 0 ? lim : Long.MAX_VALUE;
      }
    } catch (Exception ignored) {}
    return Long.MAX_VALUE;
  }

  // ── One tier: N parallel connections ────────────────────────────────────
  private void runTier(int numConnections) throws Exception {
    // Scale thread pool: generous for small counts, capped for large
    int poolSize  = Math.min(numConnections, 256);
    int groupSize = Math.min(numConnections, 64);
    NioEventLoopGroup group = new NioEventLoopGroup(groupSize);

    // Trigger a GC cycle before measuring to get a clean baseline
    System.gc();
    Thread.sleep(200);

    JvmSnapshot before = new JvmSnapshot();
    long wallStart = System.nanoTime();

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger errorCount   = new AtomicInteger(0);
    AtomicLong    totalMsgRx   = new AtomicLong(0);

    // Use a thread pool so connections start in parallel
    ExecutorService pool = Executors.newFixedThreadPool(poolSize);
    CountDownLatch allDone = new CountDownLatch(numConnections);

    for (int i = 0; i < numConnections; i++) {
      final int idx = i;
      pool.submit(() -> {
        try {
          long rx = runSingleConnection(group, idx, numConnections);
          totalMsgRx.addAndGet(rx);
          successCount.incrementAndGet();
        } catch (Exception e) {
          errorCount.incrementAndGet();
          logger.debug("Connection {} failed: {}", idx, e.getMessage());
        } finally {
          allDone.countDown();
        }
      });
    }

    // Timeout scales: 60 s base + 100 ms per connection
    long timeoutSecs = 60L + numConnections / 10;
    boolean finished = allDone.await(timeoutSecs, TimeUnit.SECONDS);

    long wallMs = (System.nanoTime() - wallStart) / 1_000_000;
    JvmSnapshot after = new JvmSnapshot();

    group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    pool.shutdownNow();

    long heapDeltaMb = (after.heapUsedBytes - before.heapUsedBytes) / (1024 * 1024);
    long gcCountDelta = after.gcCount  - before.gcCount;
    long gcTimeDelta  = after.gcTimeMs - before.gcTimeMs;
    double throughput = totalMsgRx.get() * 1000.0 / Math.max(wallMs, 1);

    printTableRow(numConnections, wallMs, successCount.get(), errorCount.get(),
        totalMsgRx.get(), throughput, heapDeltaMb, gcCountDelta, gcTimeDelta, finished);
  }

  // ── One QUIC connection with STREAMS_PER_CONNECTION bidi streams ─────────
  private long runSingleConnection(NioEventLoopGroup group, int idx, int totalConns)
      throws Exception {

    QuicSslContext sslCtx = QuicSslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols(Http3.supportedApplicationProtocols())
        .build();

    ChannelHandler codec = Http3.newQuicClientCodecBuilder()
        .sslContext(sslCtx)
        .maxIdleTimeout(20_000, TimeUnit.MILLISECONDS)
        .initialMaxData(10_000_000)
        .initialMaxStreamDataBidirectionalLocal(1_000_000)
        .initialMaxStreamDataBidirectionalRemote(1_000_000)
        .initialMaxStreamsBidirectional(STREAMS_PER_CONNECTION + 4)
        .initialMaxStreamsUnidirectional(STREAMS_PER_CONNECTION + 4)
        .initialMaxStreamDataUnidirectional(1_000_000)
        .build();

    Channel udpChannel = new Bootstrap()
        .group(group)
        .channel(NioDatagramChannel.class)
        .handler(codec)
        .bind(0).sync().channel();

    Http3Settings settings = new Http3Settings((id, v) -> true);
    settings.enableConnectProtocol(true);
    settings.enableH3Datagram(true);

    QuicChannel quic = QuicChannel.newBootstrap(udpChannel)
        .handler(new ChannelInitializer<QuicChannel>() {
          @Override protected void initChannel(QuicChannel ch) {
            ch.pipeline().addLast(new Http3ClientConnectionHandler(
                null, null, new UnknownStreamHandlerFactory(),
                new DefaultHttp3SettingsFrame(settings), false, (id, v) -> true));
          }
        })
        .remoteAddress(new InetSocketAddress("127.0.0.1", port))
        .connect().get(5, TimeUnit.SECONDS);

    // Send CONNECT to establish WebTransport session
    CountDownLatch sessionReady = new CountDownLatch(1);
    QuicStreamChannel[] connectHolder = new QuicStreamChannel[1];

    QuicStreamChannel connectStream = Http3.newRequestStream(quic,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override protected void initChannel(QuicStreamChannel ch) {
            ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
              @Override protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof Http3HeadersFrame
                    && "200".equals(((Http3HeadersFrame) msg).headers().status().toString())) {
                  connectHolder[0] = (QuicStreamChannel) ctx.channel();
                  sessionReady.countDown();
                }
              }
            });
          }
        }).sync().getNow();

    Http3Headers h = new DefaultHttp3Headers();
    h.method("CONNECT");
    h.scheme("https");
    h.path("/bench");
    h.authority("127.0.0.1:" + port);
    h.set(":protocol", "webtransport");
    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(h)).sync();

    if (!sessionReady.await(5, TimeUnit.SECONDS)) {
      quic.close().sync();
      udpChannel.close().sync();
      throw new Exception("Session CONNECT timeout for conn " + idx);
    }

    long sessionId = connectHolder[0].streamId();
    AtomicLong rxBytes = new AtomicLong(0);

    // Total expected bytes across all streams
    int payloadLen = PAYLOAD.length();
    long expectedBytes = (long) STREAMS_PER_CONNECTION * MESSAGES_PER_STREAM * payloadLen;
    CountDownLatch allDone = new CountDownLatch(1);

    // Open STREAMS_PER_CONNECTION bidi streams — pipelined, not sequential
    for (int s = 0; s < STREAMS_PER_CONNECTION; s++) {
      QuicStreamChannel bidi = quic.createStream(QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override protected void initChannel(QuicStreamChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override protected void channelRead0(ChannelHandlerContext cx, ByteBuf msg) {
                  // Count bytes — server may coalesce replies into one big chunk
                  long total = rxBytes.addAndGet(msg.readableBytes());
                  if (total >= expectedBytes) {
                    allDone.countDown();
                  }
                }
              });
            }
          }).sync().getNow();

      // Write stream type header (0x41 = WT_STREAM_BI) + session id varint
      ByteBuf header = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(header, 0x41L);   // WT_STREAM bidi
      WebTransportUtils.writeVarInt(header, sessionId);
      bidi.writeAndFlush(header);

      // Blast all MESSAGES_PER_STREAM messages without waiting for replies
      for (int m = 0; m < MESSAGES_PER_STREAM; m++) {
        bidi.write(Unpooled.copiedBuffer(PAYLOAD, StandardCharsets.UTF_8));
        if ((m + 1) % PIPELINE_DEPTH == 0) {
          bidi.flush();
        }
      }
      bidi.flush(); // flush any remainder
    }

    // Wait until all expected bytes are received
    allDone.await(30, TimeUnit.SECONDS);
    // Compute message count from bytes (payload is fixed size)
    long rxMessages = rxBytes.get() / payloadLen;

    quic.close().sync();
    udpChannel.close().sync();
    return rxMessages;
  }

  // ── Table formatting ─────────────────────────────────────────────────────
  private void printTableHeader() {
    System.out.printf("%-8s %-10s %-8s %-8s %-10s %-12s %-12s %-8s %-10s%n",
        "Conns", "Time(ms)", "OK", "ERR", "Msgs RX",
        "Msgs/sec", "Heap Δ(MB)", "GC Count", "GC Time(ms)");
    StringBuilder sep = new StringBuilder(92);
    for (int i = 0; i < 92; i++) sep.append('─');
    System.out.println(sep.toString());
  }

  private void printTableRow(int conns, long wallMs, int ok, int err, long msgs,
      double tput, long heapMb, long gcCount, long gcMs, boolean finished) {
    System.out.printf("%-8d %-10d %-8d %-8d %-10d %-12.0f %-12d %-8d %-10d%s%n",
        conns, wallMs, ok, err, msgs, tput, heapMb, gcCount, gcMs,
        finished ? "" : " ⚠️ TIMEOUT");
  }
}
