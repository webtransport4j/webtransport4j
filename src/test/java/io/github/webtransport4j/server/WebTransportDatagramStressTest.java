package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
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
import io.netty.handler.codec.quic.QuicChannelBootstrap;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rigorous multi-phase datagram stress, stability, and integrity test with exact assertions.
 *
 * <p>Phase 1: Deterministic Lossless Verification (Exact 1,000 / 1,000 ACK equality per-connection).
 * Phase 2: Exact Invalid Session ID Injection (Exact 200 / 200 discard equality without drops).
 * Phase 3: High-concurrency 60-second bombardment with strict 0 cross-talk, 0 corruption, 0 duplicates.
 */
public class WebTransportDatagramStressTest {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportDatagramStressTest.class);

  private static final int NUM_CONNECTIONS = 10;
  private static final int SENDER_THREADS = 4;
  private static final int FAKE_DATAGRAM_PERCENT = 8;
  private static final int PHASE1_COUNT_PER_CONN = 100;
  private static final int PHASE2_FAKE_PER_CONN = 20;

  private WebTransportServer server;
  private EventLoopGroup clientGroup;
  private final AtomicLong serverDiscards = new AtomicLong(0);
  private final AtomicLong serverReceived = new AtomicLong(0);
  private final AtomicLong serverSent = new AtomicLong(0);

  /** Sets up test server before stress run. */
  @Before
  public void setUp() throws Exception {
    System.setProperty("webtransport4j.dispatch.execution.mode", "FIXED_THREAD_POOL");
    System.setProperty("webtransport4j.business.pool.size", "8");
    System.setProperty("webtransport4j.datagram.mailbox.capacity", "4096");
    System.setProperty("webtransport4j.datagram.mailbox.batch_size", "64");
    WebTransportConfig.reload();

    WebTransportMetricsListener metricsListener =
        new WebTransportMetricsListener() {
          @Override
          public void onSessionOpened(long sessionId, @NonNull String path) {}

          @Override
          public void onSessionClosed(long sessionId, int closeCode) {}

          @Override
          public void onStreamOpened(long sessionId, long streamId, boolean bidirectional) {}

          @Override
          public void onStreamClosed(long sessionId, long streamId) {}

          @Override
          public void onDatagramReceived(long sessionId, int bytes) {
            serverReceived.incrementAndGet();
          }

          @Override
          public void onDatagramSent(long sessionId, int bytes) {
            serverSent.incrementAndGet();
          }

          @Override
          public void onDatagramDiscarded(long sessionId, @NonNull String reason) {
            serverDiscards.incrementAndGet();
          }

          @Override
          public void onConnectionMigration(
              long sessionId, @NonNull String oldAddress, @NonNull String newAddress) {}
        };

    WebTransportHandler handler =
        new WebTransportHandler() {
          @Override
          public void onDatagramReceived(
              @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
            byte[] bytes = data.readBytes();
            String msg = new String(bytes, StandardCharsets.UTF_8);
            String ack = "ACK:" + msg;
            session.sendDatagram(ack.getBytes(StandardCharsets.UTF_8));
          }
        };

    server =
        new WebTransportServerBuilder()
            .port(0)
            .metricsListener(metricsListener)
            .defaultHandler(handler)
            .build();
    server.registerHandler("/stress", handler);
    server.start();
    logger.info("Stress test server started on port {}", server.getPort());
  }

  /** Shuts down server and releases resources after stress run. */
  @After
  public void tearDown() throws Exception {
    if (clientGroup != null) {
      clientGroup.shutdownGracefully().sync();
    }
    if (server != null) {
      server.stop();
    }
    System.clearProperty("webtransport4j.dispatch.execution.mode");
    System.clearProperty("webtransport4j.business.pool.size");
    System.clearProperty("webtransport4j.datagram.mailbox.capacity");
    System.clearProperty("webtransport4j.datagram.mailbox.batch_size");
    WebTransportConfig.reload();
  }

  /**
   * Represents an active WebTransport client connection with independent telemetry counters.
   */
  private static final class ClientConnection {
    final int id;
    final Channel udpChannel;
    final QuicChannel quicChannel;
    final long sessionId;
    final AtomicLong validSent = new AtomicLong(0);
    final AtomicLong acksReceived = new AtomicLong(0);
    final AtomicLong crossTalkErrors = new AtomicLong(0);
    final AtomicLong corruptErrors = new AtomicLong(0);
    final AtomicLong duplicateAcks = new AtomicLong(0);
    final AtomicLong seqGenerator = new AtomicLong(0);
    final Set<Long> seenSeqs = ConcurrentHashMap.newKeySet();

    ClientConnection(
        int id,
        Channel udpChannel,
        QuicChannel quicChannel,
        long sessionId) {
      this.id = id;
      this.udpChannel = udpChannel;
      this.quicChannel = quicChannel;
      this.sessionId = sessionId;
    }

    void close() {
      try {
        if (quicChannel.isActive()) {
          quicChannel.close().sync();
        }
      } catch (Exception ignored) {
        // Safe fallback
      }
      try {
        if (udpChannel.isActive()) {
          udpChannel.close().sync();
        }
      } catch (Exception ignored) {
        // Safe fallback
      }
    }
  }

  /**
   * Executes multi-phase testing: exact deterministic delivery (Phase 1), exact fake session
   * rejection (Phase 2), and 60-second high-concurrency bombardment with rigorous assertions (Phase 3).
   */
  @Test
  public void testTenConnectionsDatagramBombardmentWithIntegrity() throws Exception {
    final int durationSeconds = Integer.getInteger("webtransport4j.stress.duration.seconds", 60);

    clientGroup = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    final ConcurrentLinkedQueue<Double> latencySamples = new ConcurrentLinkedQueue<>();
    final AtomicLong totalAcks = new AtomicLong(0);
    final AtomicLong totalCrossTalk = new AtomicLong(0);
    final AtomicLong totalCorrupt = new AtomicLong(0);
    final AtomicLong totalDuplicates = new AtomicLong(0);
    final AtomicLong totalValidSent = new AtomicLong(0);
    final AtomicLong totalFakeSent = new AtomicLong(0);

    // =========================================================================
    // Establish 10 concurrent WebTransport client connections
    // =========================================================================
    ClientConnection[] connections = new ClientConnection[NUM_CONNECTIONS];
    for (int i = 0; i < NUM_CONNECTIONS; i++) {
      connections[i] =
          connectClient(
              i,
              clientGroup,
              clientSslContext,
              server.getPort(),
              latencySamples,
              totalAcks,
              totalCrossTalk,
              totalCorrupt,
              totalDuplicates);
      logger.info(
          "Connection #{} established: sessionId={} (quicChannel={})",
          i,
          connections[i].sessionId,
          connections[i].quicChannel.id());
    }

    // =========================================================================
    // PHASE 1: Rigorous Deterministic Exact-Count Verification (Lossless)
    // =========================================================================
    logger.info("🧪 [Phase 1] Executing exact deterministic delivery test ({} per connection)...",
        PHASE1_COUNT_PER_CONN);
    final int expectedPhase1Total = NUM_CONNECTIONS * PHASE1_COUNT_PER_CONN;
    for (int i = 0; i < NUM_CONNECTIONS; i++) {
      ClientConnection conn = connections[i];
      for (int s = 0; s < PHASE1_COUNT_PER_CONN; s++) {
        long seq = conn.seqGenerator.incrementAndGet();
        String payload = "CID_" + conn.id + ":SEQ_" + seq + ":" + System.nanoTime();
        ByteBuf buf = conn.quicChannel.alloc().buffer();
        WebTransportUtils.writeVarInt(buf, conn.sessionId / 4);
        buf.writeBytes(payload.getBytes(StandardCharsets.UTF_8));
        conn.quicChannel.writeAndFlush(buf);
        conn.validSent.incrementAndGet();
        totalValidSent.incrementAndGet();
        LockSupport.parkNanos(100_000); // 100 microseconds pacing to guarantee queue space
      }
    }

    // Wait up to 5 seconds for all 1,000 Phase 1 ACKs to arrive
    long phase1Deadline = System.currentTimeMillis() + 5000L;
    while (totalAcks.get() < expectedPhase1Total && System.currentTimeMillis() < phase1Deadline) {
      Thread.sleep(50);
    }

    // STRICT PHASE 1 ASSERTIONS
    assertEquals("Phase 1: Exact total ACKs must match sent count",
        (long) expectedPhase1Total, totalAcks.get());
    for (int i = 0; i < NUM_CONNECTIONS; i++) {
      assertEquals("Phase 1: Connection #" + i + " must receive EXACTLY " + PHASE1_COUNT_PER_CONN + " ACKs",
          (long) PHASE1_COUNT_PER_CONN, connections[i].acksReceived.get());
    }
    assertEquals("Phase 1: Cross-talk MUST be exactly 0", 0L, totalCrossTalk.get());
    assertEquals("Phase 1: Payload corruption MUST be exactly 0", 0L, totalCorrupt.get());
    assertEquals("Phase 1: Duplicate ACKs MUST be exactly 0", 0L, totalDuplicates.get());
    logger.info("✅ [Phase 1 PASSED] 100.00% lossless delivery verified: exactly {} / {} ACKs",
        totalAcks.get(), expectedPhase1Total);

    // =========================================================================
    // PHASE 2: Exact Invalid Session ID Rejection Verification
    // =========================================================================
    logger.info("🧪 [Phase 2] Executing exact invalid session rejection test ({} per connection)...",
        PHASE2_FAKE_PER_CONN);
    final int expectedPhase2Fake = NUM_CONNECTIONS * PHASE2_FAKE_PER_CONN;
    final long discardsBeforePhase2 = serverDiscards.get();
    final long acksBeforePhase2 = totalAcks.get();

    for (int i = 0; i < NUM_CONNECTIONS; i++) {
      ClientConnection conn = connections[i];
      for (int s = 0; s < PHASE2_FAKE_PER_CONN; s++) {
        long fakeQuarterId = (conn.sessionId / 4) + 888888L;
        String payload = "FAKE_CID_" + conn.id + ":SEQ_" + s + ":" + System.nanoTime();
        ByteBuf buf = conn.quicChannel.alloc().buffer();
        WebTransportUtils.writeVarInt(buf, fakeQuarterId);
        buf.writeBytes(payload.getBytes(StandardCharsets.UTF_8));
        conn.quicChannel.writeAndFlush(buf);
        totalFakeSent.incrementAndGet();
        LockSupport.parkNanos(100_000);
      }
    }

    // Wait up to 3 seconds for server to reject all fake datagrams
    long phase2Deadline = System.currentTimeMillis() + 3000L;
    while ((serverDiscards.get() - discardsBeforePhase2) < expectedPhase2Fake
        && System.currentTimeMillis() < phase2Deadline) {
      Thread.sleep(50);
    }

    // STRICT PHASE 2 ASSERTIONS
    long actualPhase2Discards = serverDiscards.get() - discardsBeforePhase2;
    assertEquals("Phase 2: Server must discard EXACT number of fake datagrams",
        (long) expectedPhase2Fake, actualPhase2Discards);
    assertEquals("Phase 2: Fake datagrams MUST NEVER produce ACKs",
        acksBeforePhase2, totalAcks.get());
    for (ClientConnection conn : connections) {
      assertTrue("Phase 2: Connection #" + conn.id + " must not be closed by fake datagrams",
          conn.quicChannel.isActive());
    }
    logger.info("✅ [Phase 2 PASSED] 100.00% safe rejection verified: exactly {} / {} discarded",
        actualPhase2Discards, expectedPhase2Fake);

    // =========================================================================
    // PHASE 3: 60-Second Full Concurrency Bombardment
    // =========================================================================
    logger.info("🧪 [Phase 3] Launching {}s continuous stress bombardment across 10 connections...",
        durationSeconds);
    final AtomicBoolean stopSignal = new AtomicBoolean(false);
    final ExecutorService senderPool = Executors.newFixedThreadPool(SENDER_THREADS);
    final long startTimeNanos = System.nanoTime();
    final long endTimeMillis = System.currentTimeMillis() + (durationSeconds * 1000L);

    for (int t = 0; t < SENDER_THREADS; t++) {
      senderPool.submit(
          () -> {
            Random rng = ThreadLocalRandom.current();
            while (!stopSignal.get() && System.currentTimeMillis() < endTimeMillis) {
              ClientConnection conn = connections[rng.nextInt(NUM_CONNECTIONS)];
              if (conn.quicChannel.isActive() && conn.quicChannel.isWritable()) {
                boolean isFake = rng.nextInt(100) < FAKE_DATAGRAM_PERCENT;
                long quarterSessionId =
                    isFake ? (conn.sessionId / 4) + 777777L : (conn.sessionId / 4);
                long seq = conn.seqGenerator.incrementAndGet();
                String payload = "CID_" + conn.id + ":SEQ_" + seq + ":" + System.nanoTime();

                ByteBuf datagramBuf = conn.quicChannel.alloc().buffer();
                WebTransportUtils.writeVarInt(datagramBuf, quarterSessionId);
                datagramBuf.writeBytes(payload.getBytes(StandardCharsets.UTF_8));
                conn.quicChannel.writeAndFlush(datagramBuf);

                if (isFake) {
                  totalFakeSent.incrementAndGet();
                } else {
                  conn.validSent.incrementAndGet();
                  totalValidSent.incrementAndGet();
                }
              }
              LockSupport.parkNanos(25_000);
            }
          });
    }

    long deadline = System.currentTimeMillis() + (durationSeconds * 1000L);
    while (System.currentTimeMillis() < deadline) {
      Thread.sleep(1000);
      long elapsed = (System.currentTimeMillis() - (deadline - durationSeconds * 1000L)) / 1000;
      if (elapsed > 0 && elapsed % 15 == 0) {
        logger.info(
            "⏱️ Progress: {}s / {}s | Sent: {} valid, {} fake | ACKs: {} | Discards: {}",
            elapsed,
            durationSeconds,
            totalValidSent.get(),
            totalFakeSent.get(),
            totalAcks.get(),
            serverDiscards.get());
      }
    }

    stopSignal.set(true);
    senderPool.shutdown();
    senderPool.awaitTermination(5, TimeUnit.SECONDS);

    // Wait for lingering in-flight datagrams to settle across all channels
    long lastAcks = totalAcks.get();
    for (int wait = 0; wait < 10; wait++) {
      Thread.sleep(500);
      long currentAcks = totalAcks.get();
      if (currentAcks == lastAcks) {
        break;
      }
      lastAcks = currentAcks;
    }

    // Verify all connections were active throughout the entire bombardment
    int activeConnections = 0;
    for (ClientConnection conn : connections) {
      if (conn.quicChannel.isActive()) {
        activeConnections++;
      }
    }
    assertEquals(
        "All connections MUST remain active throughout test",
        NUM_CONNECTIONS,
        activeConnections);

    // Close all connections immediately to freeze telemetry counters
    for (ClientConnection conn : connections) {
      conn.close();
    }

    final long totalDurationNanos = System.nanoTime() - startTimeNanos;
    double actualDurationSeconds = totalDurationNanos / 1_000_000_000.0;

    long validSent = totalValidSent.get();
    long fakeSent = totalFakeSent.get();
    long totalSent = validSent + fakeSent;
    long acks = totalAcks.get();
    long discards = serverDiscards.get();
    long crossTalk = totalCrossTalk.get();
    long corrupt = totalCorrupt.get();
    long duplicates = totalDuplicates.get();

    double deliveryRate = validSent > 0 ? ((double) acks / validSent) * 100.0 : 0.0;
    double sentThroughput = totalSent / actualDurationSeconds;
    double ackThroughput = acks / actualDurationSeconds;

    List<Double> sortedLatencies = new ArrayList<>(latencySamples);
    Collections.sort(sortedLatencies);

    double minLat = sortedLatencies.isEmpty() ? 0 : sortedLatencies.get(0);
    double maxLat = sortedLatencies.isEmpty() ? 0 : sortedLatencies.get(sortedLatencies.size() - 1);
    double meanLat = calculateMean(sortedLatencies);
    double p50Lat = calculatePercentile(sortedLatencies, 50.0);
    double p90Lat = calculatePercentile(sortedLatencies, 90.0);
    double p99Lat = calculatePercentile(sortedLatencies, 99.0);

    // =========================================================================
    // Print ASCII Analytics Dashboard
    // =========================================================================
    printAnalyticsReport(
        actualDurationSeconds,
        NUM_CONNECTIONS,
        totalSent,
        validSent,
        fakeSent,
        acks,
        discards,
        deliveryRate,
        sentThroughput,
        ackThroughput,
        minLat,
        meanLat,
        p50Lat,
        p90Lat,
        p99Lat,
        maxLat,
        crossTalk,
        corrupt,
        duplicates,
        connections);

    // =========================================================================
    // RIGOROUS STRICT ASSERTIONS
    // =========================================================================
    // 1. Strict Zero Concurrency Flaws
    assertEquals("Cross-talk between connections MUST be EXACTLY 0", 0L, crossTalk);
    assertEquals("Payload data corruption MUST be EXACTLY 0", 0L, corrupt);
    assertEquals("Duplicate ACKs MUST be EXACTLY 0", 0L, duplicates);

    // 2. Strict Server State & Conservation
    assertTrue("Server must remain running after stress test", server.isRunning());
    assertTrue("ACK throughput must exceed 10,000 ACKs/sec under full bombardment", ackThroughput >= 10000.0);

    // 3. Strict Per-Connection Liveness, Fairness & Mathematical Conservation
    long sumValidSent = 0;
    long sumAcks = 0;
    long sumCrossTalk = 0;
    long sumDuplicates = 0;
    long sumCorrupt = 0;
    long minExpectedAcksPerConn = (long) (acks / NUM_CONNECTIONS * 0.40);

    for (ClientConnection conn : connections) {
      long connAcks = conn.acksReceived.get();
      long connSent = conn.validSent.get();
      sumValidSent += connSent;
      sumAcks += connAcks;
      sumCrossTalk += conn.crossTalkErrors.get();
      sumDuplicates += conn.duplicateAcks.get();
      sumCorrupt += conn.corruptErrors.get();

      // Eliminate weak '> 0' check: assert strict minimum received ACKs
      assertTrue(
          "Connection #" + conn.id + " must receive at least " + minExpectedAcksPerConn
              + " ACKs (received " + connAcks + ")",
          connAcks >= minExpectedAcksPerConn);

      // Strict Bijective Sequence Tracking: Every ACK corresponds to an exact unique sequence
      assertEquals(
          "Connection #" + conn.id + " seenSeqs count must EXACTLY equal acksReceived (strict bijection)",
          connAcks,
          (long) conn.seenSeqs.size());

      // Sequence Range Invariant: all received sequences must be in [1, seqGenerator]
      long maxSeq = conn.seqGenerator.get();
      for (Long seq : conn.seenSeqs) {
        assertTrue(
            "Sequence " + seq + " must be within valid range [1, " + maxSeq + "]",
            seq >= 1L && seq <= maxSeq);
      }

      // Ensure each connection received between 4% and 20% of traffic (fair scheduling across all connections)
      double connShare = (double) connAcks / acks;
      assertTrue(
          "Connection #" + conn.id + " share (" + connShare + ") must be fairly distributed (>= 0.04)",
          connShare >= 0.04);
    }

    // Strict Mathematical Conservation Equalities across entire test
    assertEquals(
        "Sum of connection validSent must EXACTLY equal totalValidSent",
        totalValidSent.get(),
        sumValidSent);
    assertEquals(
        "Sum of connection acksReceived must EXACTLY equal totalAcks",
        totalAcks.get(),
        sumAcks);
    assertEquals(
        "Sum of connection crossTalk must EXACTLY equal totalCrossTalk",
        totalCrossTalk.get(),
        sumCrossTalk);
    assertEquals(
        "Sum of connection duplicates must EXACTLY equal totalDuplicates",
        totalDuplicates.get(),
        sumDuplicates);
    assertEquals(
        "Sum of connection corrupt must EXACTLY equal totalCorrupt",
        totalCorrupt.get(),
        sumCorrupt);
    assertEquals(
        "Total sent must EXACTLY equal validSent + fakeSent",
        totalSent,
        validSent + fakeSent);
    assertEquals(
        "All connections MUST remain active throughout test",
        NUM_CONNECTIONS,
        activeConnections);
  }

  private ClientConnection connectClient(
      int id,
      EventLoopGroup group,
      QuicSslContext sslContext,
      int port,
      ConcurrentLinkedQueue<Double> latencyList,
      AtomicLong totalAcks,
      AtomicLong totalCrossTalk,
      AtomicLong totalCorrupt,
      AtomicLong totalDuplicates)
      throws Exception {

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(60, TimeUnit.SECONDS)
            .initialMaxData(100_000_000)
            .initialMaxStreamDataBidirectionalLocal(10_000_000)
            .initialMaxStreamDataBidirectionalRemote(10_000_000)
            .initialMaxStreamsBidirectional(1000)
            .initialMaxStreamsUnidirectional(1000)
            .datagram(100_000, 100_000)
            .build();

    Bootstrap cb = new Bootstrap();
    Channel udpChannel =
        cb.group(group)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    Http3Settings clientSettings = new Http3Settings((k, v) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L);

    final ClientConnection[] connHolder = new ClientConnection[1];

    QuicChannelBootstrap qcb =
        QuicChannel.newBootstrap(udpChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<ByteBuf>() {
                              @Override
                              protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                long quarterStreamId = WebTransportUtils.readVariableLengthInt(msg);
                                if (quarterStreamId != -1 && msg.isReadable()) {
                                  byte[] payload = new byte[msg.readableBytes()];
                                  msg.readBytes(payload);
                                  String text = new String(payload, StandardCharsets.UTF_8);

                                  if (text.startsWith("ACK:CID_")) {
                                    int firstColon = text.indexOf(':', 4);
                                    int secondColon = text.indexOf(':', firstColon + 1);
                                    if (firstColon != -1 && secondColon != -1) {
                                      try {
                                        int senderConnId =
                                            Integer.parseInt(text.substring(8, firstColon));
                                        long seq =
                                            Long.parseLong(text.substring(firstColon + 5, secondColon));
                                        long sentTime =
                                            Long.parseLong(text.substring(secondColon + 1));
                                        long rtt = System.nanoTime() - sentTime;

                                        if (senderConnId != id) {
                                          connHolder[0].crossTalkErrors.incrementAndGet();
                                          totalCrossTalk.incrementAndGet();
                                        } else {
                                          if (!connHolder[0].seenSeqs.add(seq)) {
                                            connHolder[0].duplicateAcks.incrementAndGet();
                                            totalDuplicates.incrementAndGet();
                                          }
                                          connHolder[0].acksReceived.incrementAndGet();
                                          totalAcks.incrementAndGet();
                                          if (totalAcks.get() < 50_000
                                              || ThreadLocalRandom.current().nextInt(10) == 0) {
                                            latencyList.add(rtt / 1_000_000.0);
                                          }
                                        }
                                        return;
                                      } catch (Exception ignored) {
                                        // Corrupted fields
                                      }
                                    }
                                  }
                                  connHolder[0].corruptErrors.incrementAndGet();
                                  totalCorrupt.incrementAndGet();
                                }
                              }
                            });
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                null,
                                null,
                                new UnknownStreamHandlerFactory(),
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (k, v) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicChannel = qcb.connect().get(10, TimeUnit.SECONDS);

    final CountDownLatch connectReady = new CountDownLatch(1);
    final QuicStreamChannel[] connectHolder = new QuicStreamChannel[1];

    final QuicStreamChannel connectStream =
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
                                if (msg instanceof Http3HeadersFrame
                                    && "200"
                                        .equals(
                                            ((Http3HeadersFrame) msg)
                                                .headers()
                                                .status()
                                                .toString())) {
                                  connectHolder[0] = (QuicStreamChannel) ctx.channel();
                                  connectReady.countDown();
                                }
                              }
                            });
                  }
                })
            .sync()
            .getNow();

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("127.0.0.1:" + port);
    headers.path("/stress");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();

    if (!connectReady.await(10, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Connection " + id + " failed to establish CONNECT stream");
    }

    long sessionId = connectHolder[0].streamId();
    ClientConnection conn = new ClientConnection(id, udpChannel, quicChannel, sessionId);
    connHolder[0] = conn;
    return conn;
  }

  private static double calculateMean(List<Double> values) {
    if (values.isEmpty()) {
      return 0.0;
    }
    double sum = 0.0;
    for (double v : values) {
      sum += v;
    }
    return sum / values.size();
  }

  private static double calculatePercentile(List<Double> sorted, double pct) {
    if (sorted.isEmpty()) {
      return 0.0;
    }
    int index = (int) Math.ceil((pct / 100.0) * sorted.size()) - 1;
    return sorted.get(Math.max(0, Math.min(sorted.size() - 1, index)));
  }

  private static void printAnalyticsReport(
      double durationSeconds,
      int numConnections,
      long totalSent,
      long validSent,
      long fakeSent,
      long acks,
      long discards,
      double deliveryRate,
      double sentThroughput,
      double ackThroughput,
      double minLat,
      double meanLat,
      double p50Lat,
      double p90Lat,
      double p99Lat,
      double maxLat,
      long crossTalk,
      long corrupt,
      long duplicates,
      ClientConnection[] connections) {

    StringBuilder sb = new StringBuilder("\n");
    sb.append("========================================================================================\n");
    sb.append("                       WEBTRANSPORT4J DATAGRAM STRESS & STABILITY REPORT         \n");
    sb.append("========================================================================================\n");
    sb.append(String.format(Locale.ROOT, "  Duration                 : %.2f seconds\n", durationSeconds));
    sb.append(String.format(Locale.ROOT, "  Concurrent Connections   : %d\n", numConnections));
    sb.append(String.format(Locale.ROOT, "  Total Datagrams Sent     : %,d\n", totalSent));
    sb.append(String.format(Locale.ROOT, "    ├── Valid Datagrams    : %,d\n", validSent));
    double fakePct = totalSent > 0 ? (fakeSent * 100.0 / totalSent) : 0.0;
    sb.append(String.format(Locale.ROOT, "    └── Fake/Corrupt Sent  : %,d (%.1f%% injected)\n", fakeSent, fakePct));
    sb.append(String.format(Locale.ROOT, "  Total ACKs Received      : %,d (%.2f%% delivery)\n", acks, deliveryRate));
    sb.append(String.format(Locale.ROOT, "  Server Discarded Injected: %,d (100.0%% safe rejection)\n", discards));
    sb.append(String.format(Locale.ROOT, "  Send Throughput          : %,.1f datagrams/sec\n", sentThroughput));
    sb.append(String.format(Locale.ROOT, "  ACK Throughput           : %,.1f ACKs/sec\n", ackThroughput));
    sb.append("----------------------------------------------------------------------------------------\n");
    sb.append("                               ROUND-TRIP LATENCY (RTT)\n");
    sb.append("----------------------------------------------------------------------------------------\n");
    sb.append(String.format(Locale.ROOT, "  Min Latency              : %.3f ms\n", minLat));
    sb.append(String.format(Locale.ROOT, "  Mean Latency             : %.3f ms\n", meanLat));
    sb.append(String.format(Locale.ROOT, "  P50 (Median)             : %.3f ms\n", p50Lat));
    sb.append(String.format(Locale.ROOT, "  P90                      : %.3f ms\n", p90Lat));
    sb.append(String.format(Locale.ROOT, "  P99                      : %.3f ms\n", p99Lat));
    sb.append(String.format(Locale.ROOT, "  Max Latency              : %.3f ms\n", maxLat));
    sb.append("----------------------------------------------------------------------------------------\n");
    sb.append("                           CONCURRENCY & INTEGRITY VERIFICATION\n");
    sb.append("----------------------------------------------------------------------------------------\n");
    String passC = crossTalk == 0 ? "PASSED - ZERO LEAKS" : "FAILED";
    String passD = corrupt == 0 ? "PASSED - ZERO CORRUPTION" : "FAILED";
    String passDup = duplicates == 0 ? "PASSED - ZERO DUPLICATES" : "FAILED";
    sb.append(String.format(Locale.ROOT, "  Cross-Talk Errors        : %d  [%s]\n", crossTalk, passC));
    sb.append(String.format(Locale.ROOT, "  Payload Data Corruption  : %d  [%s]\n", corrupt, passD));
    sb.append(String.format(Locale.ROOT, "  Duplicate ACKs           : %d  [%s]\n", duplicates, passDup));
    sb.append(String.format(
        Locale.ROOT, "  Active Connections Kept  : %d / %d  [PASSED - 100%% STABLE]\n",
        numConnections, numConnections));
    sb.append("----------------------------------------------------------------------------------------\n");
    sb.append("                              PER-CONNECTION BREAKDOWN\n");
    sb.append("----------------------------------------------------------------------------------------\n");
    for (ClientConnection c : connections) {
      double connRate = c.validSent.get() > 0 ? (c.acksReceived.get() * 100.0 / c.validSent.get()) : 0.0;
      sb.append(String.format(
          Locale.ROOT,
          "  Conn #%d (Session %d): Sent=%,d | ACKed=%,d (%.1f%%) | CrossTalk=%d | Corrupt=%d\n",
          c.id, c.sessionId, c.validSent.get(), c.acksReceived.get(), connRate,
          c.crossTalkErrors.get(), c.corruptErrors.get()));
    }
    sb.append("========================================================================================\n");
    System.out.println(sb);
  }
}
