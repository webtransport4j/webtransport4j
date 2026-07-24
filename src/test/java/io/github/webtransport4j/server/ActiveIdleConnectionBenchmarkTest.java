package io.github.webtransport4j.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import java.lang.reflect.Method;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ClientConnectionHandler;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side benchmark testing N total open connections, where Y connections actively send and
 * receive messages every second, while (N - Y) connections remain idle and open.
 *
 * <p>Example scenario: 40,000 total connections opened, 15,000 active connections sending/receiving
 * 1 msg/sec continuously.
 *
 * <p>Target server is {@link BenchmarkServerRunner}. Start the runner in a separate JVM and run this
 * test with:
 * <pre>
 * mvn test -Dtest=ActiveIdleConnectionBenchmarkTest \
 *   -Dtarget.port=&lt;port&gt; \
 *   -Dbenchmark.connections=40000 \
 *   -Dbenchmark.active.connections=15000 \
 *   -Dbenchmark.duration.seconds=30
 * </pre>
 */
public class ActiveIdleConnectionBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(ActiveIdleConnectionBenchmarkTest.class);

  private static final String PING_PAYLOAD = "BENCH-ACTIVE-PING-1234567890";
  private static final byte[] PING_BYTES = PING_PAYLOAD.getBytes(StandardCharsets.UTF_8);
  private static final int PING_LENGTH = PING_BYTES.length;
  private static final ByteBuf PING_BUF = Unpooled.unreleasableBuffer(
      Unpooled.directBuffer(PING_BYTES.length).writeBytes(PING_BYTES));
  private static final int[] DEFAULT_CONNECTION_TIERS = {10, 100, 1_000, 10_000, 40_000};
  private static final double DEFAULT_ACTIVE_RATIO = 0.375; // e.g. 15,000 / 40,000
  private static final int MAX_CLIENT_THREADS = 128;
  private static final int MAX_CLIENT_UDP_CHANNELS = 1024;
  private static final long CONNECTION_TIMEOUT_SECONDS = 30;
  private static final long IDLE_TIMEOUT_SECONDS = positiveProperty("benchmark.idle.timeout.seconds", 600);
  private static final long DURATION_SECONDS = nonNegativeProperty("benchmark.duration.seconds", 30);

  private String host;
  private int port;

  @Before
  public void setUp() {
    String configuredPort = System.getProperty("target.port");
    Assume.assumeTrue(
        "Start BenchmarkServerRunner and set -Dtarget.port=<port> to run this benchmark.",
        configuredPort != null && !configuredPort.trim().isEmpty());

    try {
      port = Integer.parseInt(configuredPort);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("target.port must be a valid TCP/UDP port: " + configuredPort, e);
    }
    if (port < 1 || port > 65_535) {
      throw new IllegalArgumentException("target.port must be between 1 and 65535: " + port);
    }

    host = System.getProperty("target.host", "127.0.0.1").trim();
    if (host.isEmpty()) {
      throw new IllegalArgumentException("target.host must not be empty");
    }
    logger.info("Benchmarking Active/Idle connections against BenchmarkServerRunner at {}:{}", host, port);
  }

  @Test
  public void benchmarkActiveIdleConnections() throws Exception {
    System.out.printf("========================================================================%n");
    System.out.printf("⚡ ACTIVE / IDLE WEBTRANSPORT CONNECTION SCALABILITY BENCHMARK ⚡%n");
    System.out.printf("   Target: %s:%d%n", host, port);
    System.out.printf("========================================================================%n");
    System.out.printf(
        "%10s %10s %10s %12s %14s %14s %14s %10s%n",
        "Total Conns",
        "Active",
        "Idle",
        "Duration(s)",
        "Sent Msgs",
        "Recv Msgs",
        "Msgs/sec",
        "Status");

    int[] tiers = connectionTiers();
    for (int tier : tiers) {
      int activeCount = calculateActiveCount(tier);
      runActiveIdleTier(tier, activeCount, DURATION_SECONDS);
    }
    System.out.printf("========================================================================%n");
  }

  private void runActiveIdleTier(int totalConnections, int activeConnections, long durationSeconds)
      throws Exception {
    int channelCount = Math.min(totalConnections, MAX_CLIENT_UDP_CHANNELS);
    int cpus = Math.max(2, Runtime.getRuntime().availableProcessors());
    int ioThreadCount = positiveProperty("benchmark.client.threads", Math.min(channelCount, cpus * 2));
    int workerThreadCount = positiveProperty("benchmark.worker.threads", Math.min(channelCount, cpus * 4));

    ClientTransport transport = ClientTransport.create(ioThreadCount);
    EventLoopGroup eventLoopGroup = transport.group;
    ExecutorService connectWorkers = Executors.newFixedThreadPool(workerThreadCount);
    ScheduledExecutorService activeMessageScheduler = Executors.newScheduledThreadPool(workerThreadCount);

    List<Throwable> failures = Collections.synchronizedList(new ArrayList<Throwable>());
    AtomicInteger successfulConnections = new AtomicInteger();
    CountDownLatch connectComplete = new CountDownLatch(totalConnections);
    List<ClientDatagramChannel> udpChannels = new ArrayList<ClientDatagramChannel>();
    List<BenchmarkSession> allSessions =
        Collections.synchronizedList(new ArrayList<BenchmarkSession>());

    AtomicLong totalSentMsgs = new AtomicLong();
    AtomicLong totalRecvMsgs = new AtomicLong();
    AtomicLong activeFailures = new AtomicLong();

    long startNanos = System.nanoTime();
    boolean connectFinished = false;

    try {
      // 1. Bind UDP channel pool
      for (int i = 0; i < channelCount; i++) {
        udpChannels.add(bindClientChannel(transport));
      }

      // 2. Open N total WebTransport connections asynchronously
      logger.info(
          "Opening {} total connections ({} active sending 1 msg/sec, {} idle)...",
          totalConnections,
          activeConnections,
          totalConnections - activeConnections);

      for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
        final ClientDatagramChannel channel = udpChannels.get(channelIndex);
        final int firstConnection = channelIndex;
        connectWorkers.execute(
            () -> {
              long connectPacingMs = Long.getLong("benchmark.connect.pacing.ms", 1L);
              for (int connection = firstConnection;
                  connection < totalConnections;
                  connection += channelCount) {
                if (connectPacingMs > 0 && (connection / channelCount) % 5 == 0) {
                  try {
                    TimeUnit.MILLISECONDS.sleep(connectPacingMs);
                  } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                  }
                }
                try {
                  BenchmarkSession session = openSession(channel.channel, connection, totalRecvMsgs);
                  allSessions.add(session);
                  successfulConnections.incrementAndGet();
                } catch (Throwable failure) {
                  failures.add(failure);
                } finally {
                  connectComplete.countDown();
                }
              }
            });
      }

      connectFinished =
          connectComplete.await(tierTimeoutSeconds(totalConnections), TimeUnit.SECONDS);

      if (!failures.isEmpty() || !connectFinished || successfulConnections.get() < totalConnections) {
        throw new IllegalStateException(
            "Failed to establish "
                + totalConnections
                + " connections. Successful: "
                + successfulConnections.get()
                + ", Failures: "
                + failures.size(),
            failures.isEmpty() ? null : failures.get(0));
      }

      logger.info(
          "✅ All {} connections opened successfully in {} ms.",
          totalConnections,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));

      // 3. Mark the first 'activeConnections' as ACTIVE, remainder as IDLE
      List<BenchmarkSession> activeSessions = new ArrayList<BenchmarkSession>();
      for (int i = 0; i < totalConnections; i++) {
        BenchmarkSession session = allSessions.get(i);
        if (i < activeConnections) {
          session.setActive(true);
          activeSessions.add(session);
        } else {
          session.setActive(false);
        }
      }

      // 4. Start sustained 1 msg/sec active loop for the active connections
      if (durationSeconds > 0 && !activeSessions.isEmpty()) {
        logger.info(
            "🚀 Launching 1 msg/sec loop on {} active connections for {} seconds...",
            activeConnections,
            durationSeconds);

        long activeStartNanos = System.nanoTime();

        // Divide active sessions into 10 micro-slice buckets (100ms intervals) to smooth UDP NIC traffic bursts
        int sliceCount = 10;
        int sliceSize = (int) Math.ceil((double) activeSessions.size() / sliceCount);
        List<java.util.concurrent.ScheduledFuture<?>> slicePingTasks =
            new ArrayList<java.util.concurrent.ScheduledFuture<?>>();

        for (int s = 0; s < sliceCount; s++) {
          final int startIdx = s * sliceSize;
          final int endIdx = Math.min(startIdx + sliceSize, activeSessions.size());
          if (startIdx >= activeSessions.size()) {
            break;
          }
          final List<BenchmarkSession> sliceList = activeSessions.subList(startIdx, endIdx);

          slicePingTasks.add(
              activeMessageScheduler.scheduleAtFixedRate(
                  () -> {
                    for (BenchmarkSession activeSession : sliceList) {
                      if (activeSession.isHealthy()) {
                        try {
                          totalSentMsgs.incrementAndGet();
                          activeSession.sendPing();
                        } catch (Throwable t) {
                          activeFailures.incrementAndGet();
                          activeSession.recordFailure(t);
                        }
                      }
                    }
                  },
                  s * 100L,
                  1000L,
                  TimeUnit.MILLISECONDS));
        }

        TimeUnit.SECONDS.sleep(durationSeconds);
        long activeElapsedMs =
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - activeStartNanos);

        for (java.util.concurrent.ScheduledFuture<?> task : slicePingTasks) {
          task.cancel(false);
        }
        // Allow in-flight responses to drain before closing sockets
        TimeUnit.MILLISECONDS.sleep(2000);

        // Assert health of all connections
        assertAllSessionsHealthy(allSessions, totalConnections);

        long sent = totalSentMsgs.get();
        long recv = totalRecvMsgs.get();
        double deliveryRatio = sent > 0 ? (double) recv / sent : 1.0;
        boolean passed = deliveryRatio >= 0.95;

        double tput = recv * 1_000.0 / Math.max(activeElapsedMs, 1L);

        System.out.printf(
            "%10d %10d %10d %12d %14d %14d %14.0f %10s%n",
            totalConnections,
            activeConnections,
            totalConnections - activeConnections,
            durationSeconds,
            sent,
            recv,
            tput,
            passed ? "PASSED" : "FAILED");

        if (!passed) {
          throw new IllegalStateException(
              String.format(
                  "Message delivery accuracy threshold failed: received %d of %d sent (%.2f%%)",
                  recv, sent, deliveryRatio * 100.0));
        }
      } else {
        System.out.printf(
            "%10d %10d %10d %12d %14d %14d %14s %10s%n",
            totalConnections,
            activeConnections,
            totalConnections - activeConnections,
            0,
            0,
            0,
            "-",
            "OPENED");
      }

    } catch (Throwable error) {
      System.out.printf(
          "%10d %10d %10d %12d %14d %14d %14s %10s (%s)%n",
          totalConnections,
          activeConnections,
          totalConnections - activeConnections,
          durationSeconds,
          totalSentMsgs.get(),
          totalRecvMsgs.get(),
          "-",
          "FAILED",
          error.getMessage());
      throw error;
    } finally {
      activeMessageScheduler.shutdownNow();
      closeSessions(allSessions);
      connectWorkers.shutdownNow();
      awaitWorkerShutdown(connectWorkers);
      for (ClientDatagramChannel channel : udpChannels) {
        channel.close();
      }
      eventLoopGroup.shutdownGracefully(0, 3, TimeUnit.SECONDS).syncUninterruptibly();
    }
  }

  private ClientDatagramChannel bindClientChannel(ClientTransport transport)
      throws InterruptedException {
    QuicSslContext sslContext = createClientSslContext();
    Channel channel =
        new Bootstrap()
            .group(transport.group)
            .channel(transport.channelClass)
            .option(ChannelOption.SO_RCVBUF, 16 * 1024 * 1024)
            .option(ChannelOption.SO_SNDBUF, 16 * 1024 * 1024)
            .handler(newClientCodec(sslContext))
            .bind(0)
            .sync()
            .channel();
    return new ClientDatagramChannel(channel, sslContext);
  }

  private static final class ClientTransport {
    final EventLoopGroup group;
    final Class<? extends Channel> channelClass;

    ClientTransport(EventLoopGroup group, Class<? extends Channel> channelClass) {
      this.group = group;
      this.channelClass = channelClass;
    }

    static ClientTransport create(int threadCount) {
      try {
        Class<?> epollClass = Class.forName("io.netty.channel.epoll.Epoll");
        Method isAvailable = epollClass.getMethod("isAvailable");
        if ((Boolean) isAvailable.invoke(null)) {
          Class<?> groupClass = Class.forName("io.netty.channel.epoll.EpollEventLoopGroup");
          Class<?> channelClass = Class.forName("io.netty.channel.epoll.EpollDatagramChannel");
          EventLoopGroup group = (EventLoopGroup) groupClass.getConstructor(int.class).newInstance(threadCount);
          @SuppressWarnings("unchecked")
          Class<? extends Channel> castChannel = (Class<? extends Channel>) channelClass;
          logger.info("⚡ Client using Linux Epoll native transport");
          return new ClientTransport(group, castChannel);
        }
      } catch (Throwable ignored) {
      }

      try {
        Class<?> kqueueClass = Class.forName("io.netty.channel.kqueue.KQueue");
        Method isAvailable = kqueueClass.getMethod("isAvailable");
        if ((Boolean) isAvailable.invoke(null)) {
          Class<?> groupClass = Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup");
          Class<?> channelClass = Class.forName("io.netty.channel.kqueue.KQueueDatagramChannel");
          EventLoopGroup group = (EventLoopGroup) groupClass.getConstructor(int.class).newInstance(threadCount);
          @SuppressWarnings("unchecked")
          Class<? extends Channel> castChannel = (Class<? extends Channel>) channelClass;
          logger.info("⚡ Client using macOS KQueue native transport");
          return new ClientTransport(group, castChannel);
        }
      } catch (Throwable ignored) {
      }

      logger.info("⚡ Client using Java NIO transport");
      return new ClientTransport(new NioEventLoopGroup(threadCount), NioDatagramChannel.class);
    }
  }

  private ChannelHandler newClientCodec(QuicSslContext sslContext) {
    return Http3.newQuicClientCodecBuilder()
        .sslContext(sslContext)
        .maxIdleTimeout(IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .initialMaxData(100_000_000)
        .initialMaxStreamDataBidirectionalLocal(10_000_000)
        .initialMaxStreamDataBidirectionalRemote(10_000_000)
        .initialMaxStreamsBidirectional(100)
        .initialMaxStreamsUnidirectional(100)
        .initialMaxStreamDataUnidirectional(10_000_000)
        .build();
  }

  private BenchmarkSession openSession(Channel udpChannel, int connectionIndex, AtomicLong totalRecvMsgs) throws Exception {
    QuicChannel quicChannel = null;
    QuicStreamChannel connectStream = null;
    AtomicBoolean closing = new AtomicBoolean();
    AtomicReference<Throwable> lifecycleFailure = new AtomicReference<Throwable>();
    BenchmarkSession session = null;

    try {
      quicChannel = connect(udpChannel);
      CompletableFuture<Long> sessionIdFuture = new CompletableFuture<Long>();
      connectStream = openConnectStream(quicChannel, sessionIdFuture, lifecycleFailure, closing);
      sendConnectRequest(connectStream);

      long sessionId = sessionIdFuture.get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // Open initial bidirectional echo stream for messaging with non-blocking echo handler
      QuicStreamChannel bidiStream = openBidiStream(quicChannel, sessionId, totalRecvMsgs, lifecycleFailure, closing);

      session =
          new BenchmarkSession(
              connectionIndex,
              quicChannel,
              connectStream,
              bidiStream,
              sessionId,
              lifecycleFailure,
              closing);
      return session;
    } finally {
      if (session == null) {
        closing.set(true);
        closeChannel(connectStream);
        closeChannel(quicChannel);
      }
    }
  }

  private QuicChannel connect(Channel udpChannel) throws Exception {
    Http3Settings settings = new Http3Settings((id, value) -> true);
    settings.enableConnectProtocol(true);
    settings.enableH3Datagram(true);

    return QuicChannel.newBootstrap(udpChannel)
        .handler(
            new ChannelInitializer<QuicChannel>() {
              @Override
              protected void initChannel(QuicChannel channel) {
                channel.pipeline().addLast(
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
        .get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private QuicStreamChannel openConnectStream(
      QuicChannel quicChannel,
      CompletableFuture<Long> sessionIdFuture,
      AtomicReference<Throwable> lifecycleFailure,
      AtomicBoolean closing)
      throws InterruptedException {
    return Http3.newRequestStream(
            quicChannel,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel channel) {
                channel.pipeline().addLast(
                    new SimpleChannelInboundHandler<Object>() {
                      @Override
                      protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                        if (msg instanceof Http3HeadersFrame) {
                          Http3HeadersFrame resp = (Http3HeadersFrame) msg;
                          if ("200".equals(resp.headers().status().toString())) {
                            sessionIdFuture.complete(((QuicStreamChannel) ctx.channel()).streamId());
                          } else {
                            Exception ex = new IllegalStateException("CONNECT failed: " + resp.headers().status());
                            recordFailure(lifecycleFailure, closing, ex);
                            sessionIdFuture.completeExceptionally(ex);
                          }
                        }
                      }

                      @Override
                      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                        recordFailure(lifecycleFailure, closing, cause);
                        sessionIdFuture.completeExceptionally(cause);
                        ctx.close();
                      }
                    });
              }
            })
        .sync()
        .getNow();
  }

  private void sendConnectRequest(QuicStreamChannel connectStream) throws InterruptedException {
    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.path("/bench");
    headers.authority(host + ':' + port);
    headers.set(":protocol", "webtransport");
    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
  }

  private QuicStreamChannel openBidiStream(
      QuicChannel quicChannel,
      long sessionId,
      AtomicLong totalRecvMsgs,
      AtomicReference<Throwable> lifecycleFailure,
      AtomicBoolean closing)
      throws Exception {
    QuicStreamChannel stream =
        quicChannel
            .createStream(
                QuicStreamType.BIDIRECTIONAL,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel ch) {
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                      private int bytesRead = 0;

                      @Override
                      protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                        ByteBuf buf = null;
                        if (msg instanceof Http3DataFrame) {
                          buf = ((Http3DataFrame) msg).content();
                        } else if (msg instanceof ByteBuf) {
                          buf = (ByteBuf) msg;
                        }
                        if (buf == null) {
                          return;
                        }
                        int len = buf.readableBytes();
                        bytesRead += len;
                        while (bytesRead >= PING_LENGTH) {
                          bytesRead -= PING_LENGTH;
                          totalRecvMsgs.incrementAndGet();
                        }
                      }

                      @Override
                      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                        recordFailure(lifecycleFailure, closing, cause);
                      }
                    });
                  }
                })
            .sync()
            .getNow();

    ByteBuf streamHeader = Unpooled.buffer(16);
    WebTransportUtils.writeVarInt(streamHeader, 0x41L); // WT bidi stream
    WebTransportUtils.writeVarInt(streamHeader, sessionId);
    stream.writeAndFlush(streamHeader).sync();
    return stream;
  }

  private static void assertAllSessionsHealthy(List<BenchmarkSession> sessions, int expectedCount) {
    Assert.assertEquals("Session count match", expectedCount, sessions.size());
    for (BenchmarkSession s : sessions) {
      Assert.assertTrue("Session #" + s.index + " must be healthy and active", s.isHealthy());
    }
  }

  private static void closeSessions(List<BenchmarkSession> sessions) {
    for (BenchmarkSession s : sessions) {
      s.close();
    }
  }

  private int calculateActiveCount(int totalConnections) {
    String configuredActive = System.getProperty("benchmark.active.connections");
    if (configuredActive != null && !configuredActive.trim().isEmpty()) {
      int active = Integer.parseInt(configuredActive.trim());
      return Math.min(totalConnections, Math.max(1, active));
    }
    double ratio = Double.parseDouble(System.getProperty("benchmark.active.ratio", String.valueOf(DEFAULT_ACTIVE_RATIO)));
    return Math.min(totalConnections, Math.max(1, (int) Math.round(totalConnections * ratio)));
  }

  private static int[] connectionTiers() {
    String configuredTiers = System.getProperty("benchmark.connections");
    if (configuredTiers == null || configuredTiers.trim().isEmpty()) {
      return DEFAULT_CONNECTION_TIERS.clone();
    }

    String[] values = configuredTiers.split(",");
    int[] tiers = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      tiers[i] = Integer.parseInt(values[i].trim());
    }
    return tiers;
  }

  private static long tierTimeoutSeconds(int connections) {
    return Math.min(600L, Math.max(60L, 30L + connections / 20));
  }

  private static int positiveProperty(String property, int defaultValue) {
    int value = Integer.getInteger(property, defaultValue);
    return Math.max(1, value);
  }

  private static long nonNegativeProperty(String property, long defaultValue) {
    String val = System.getProperty(property);
    if (val == null) return defaultValue;
    return Math.max(0, Long.parseLong(val));
  }

  private static QuicSslContext createClientSslContext() {
    try {
      return QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static void awaitWorkerShutdown(ExecutorService workers) {
    try {
      if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.warn("Worker threads did not stop within 5 seconds");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void closeChannel(Channel channel) {
    if (channel != null && channel.isOpen()) {
      channel.close().syncUninterruptibly();
    }
  }

  private static void recordFailure(
      AtomicReference<Throwable> lifecycleFailure, AtomicBoolean closing, Throwable failure) {
    if (!closing.get()) {
      lifecycleFailure.compareAndSet(null, failure);
    }
  }

  private static final class ClientDatagramChannel {
    private final Channel channel;
    private final QuicSslContext sslContext;

    private ClientDatagramChannel(Channel channel, QuicSslContext sslContext) {
      this.channel = channel;
      this.sslContext = sslContext;
    }

    private void close() {
      ActiveIdleConnectionBenchmarkTest.closeChannel(channel);
    }
  }

  private static final class BenchmarkSession {
    private final int index;
    private final QuicChannel quicChannel;
    private final QuicStreamChannel connectStream;
    private final QuicStreamChannel bidiStream;
    private final long sessionId;
    private final AtomicReference<Throwable> lifecycleFailure;
    private final AtomicBoolean closing;
    private volatile boolean active;

    private BenchmarkSession(
        int index,
        QuicChannel quicChannel,
        QuicStreamChannel connectStream,
        QuicStreamChannel bidiStream,
        long sessionId,
        AtomicReference<Throwable> lifecycleFailure,
        AtomicBoolean closing) {
      this.index = index;
      this.quicChannel = quicChannel;
      this.connectStream = connectStream;
      this.bidiStream = bidiStream;
      this.sessionId = sessionId;
      this.lifecycleFailure = lifecycleFailure;
      this.closing = closing;

      quicChannel.closeFuture().addListener(
          f -> ActiveIdleConnectionBenchmarkTest.recordFailure(lifecycleFailure, closing, new IllegalStateException("Connection #" + index + " closed unexpectedly")));
    }

    private void setActive(boolean active) {
      this.active = active;
    }

    private boolean isHealthy() {
      return lifecycleFailure.get() == null && quicChannel.isActive();
    }

    private void sendPing() {
      if (isHealthy()) {
        bidiStream.writeAndFlush(PING_BUF.duplicate());
      }
    }

    private void recordFailure(Throwable cause) {
      ActiveIdleConnectionBenchmarkTest.recordFailure(lifecycleFailure, closing, cause);
    }

    private java.util.concurrent.ScheduledFuture<?> schedulePeriodicPing(
        AtomicLong totalSentMsgs, AtomicLong activeFailures) {
      return bidiStream
          .eventLoop()
          .scheduleAtFixedRate(
              () -> {
                if (isHealthy()) {
                  try {
                    totalSentMsgs.incrementAndGet();
                    bidiStream.writeAndFlush(PING_BUF.duplicate());
                  } catch (Throwable t) {
                    activeFailures.incrementAndGet();
                    recordFailure(t);
                  }
                }
              },
              java.util.concurrent.ThreadLocalRandom.current().nextInt(1000),
              1000,
              TimeUnit.MILLISECONDS);
    }

    private void close() {
      closing.set(true);
      closeChannel(bidiStream);
      closeChannel(connectStream);
      closeChannel(quicChannel);
    }
  }

}
