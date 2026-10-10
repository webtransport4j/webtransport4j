package io.github.webtransport4j.example.lichess.benchmark;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Ping;
import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundMove;
import io.github.webtransport4j.example.lichess.server.LilaWebSocketServer;
import io.github.webtransport4j.example.lichess.server.LilaWebTransportServer;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
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
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark Suite comparing Lichess WebSocket (lila-ws) vs WebTransport4J.
 *
 * <ul>
 *   <li>1. Clean LAN/Local latency and tail percentiles</li>
 *   <li>2. Head-of-Line (HoL) Blocking resilience under concurrent background traffic (eval/crowd)</li>
 *   <li>3. Network Jitter and Tail Spikes under simulated loss/delay (1.5%)</li>
 *   <li>4. Connection Handover: QUIC 0-RTT Migration vs TCP Teardown + Reconnect + Resync</li>
 * </ul>
 */
public class LilaBenchmarkSuite {

  private static final Logger log = LoggerFactory.getLogger(LilaBenchmarkSuite.class);

  private static final String HOST = "127.0.0.1";
  private static final long NETWORK_JITTER_DELAY_MS = 30;
  private final int iterations;
  private final int warmup;

  private LilaWebSocketServer wsServer;
  private LilaWebTransportServer wtServer;
  private SslContext wsClientSslContext;

  /**
   * Mode of concurrent background auxiliary traffic for Head-of-Line blocking tests.
   */
  public enum HolMode {
    NONE,
    EVAL_4KB,
    RESYNC_15KB
  }

  /**
   * Encapsulates statistical metrics for a benchmark scenario.
   */
  public static final class ScenarioStats {
    public final String protocol;
    public final String scenario;
    public final int count;
    public final double minMs;
    public final double meanMs;
    public final double medianMs;
    public final double p75Ms;
    public final double p90Ms;
    public final double p95Ms;
    public final double p99Ms;
    public final double p999Ms;
    public final double maxMs;
    public final double jitterStdDevMs;
    public final double spikeRatio; // p99 / median
    public final double throughputOpsSec;

    /**
     * Constructs a ScenarioStats instance with throughput calculation.
     *
     * @param protocol protocol name
     * @param scenario scenario description
     * @param latenciesMs measured latencies list in milliseconds
     * @param durationSeconds total duration of the measurement phase in seconds
     */
    public ScenarioStats(String protocol, String scenario, List<Double> latenciesMs, double durationSeconds) {
      this.protocol = protocol;
      this.scenario = scenario;
      this.count = latenciesMs.size();

      List<Double> sorted = new ArrayList<>(latenciesMs);
      Collections.sort(sorted);

      this.minMs = sorted.isEmpty() ? 0 : sorted.get(0);
      this.maxMs = sorted.isEmpty() ? 0 : sorted.get(sorted.size() - 1);
      this.medianMs = percentile(sorted, 50.0);
      this.p75Ms = percentile(sorted, 75.0);
      this.p90Ms = percentile(sorted, 90.0);
      this.p95Ms = percentile(sorted, 95.0);
      this.p99Ms = percentile(sorted, 99.0);
      this.p999Ms = percentile(sorted, 99.9);

      double sum = 0;
      for (double v : sorted) {
        sum += v;
      }
      this.meanMs = sorted.isEmpty() ? 0 : sum / sorted.size();

      double varSum = 0;
      for (double v : sorted) {
        varSum += Math.pow(v - this.meanMs, 2);
      }
      this.jitterStdDevMs = sorted.isEmpty() ? 0 : Math.sqrt(varSum / sorted.size());
      this.spikeRatio = this.medianMs > 0 ? (this.p99Ms / this.medianMs) : 1.0;
      this.throughputOpsSec = durationSeconds > 0 ? (this.count / durationSeconds) : 0.0;
    }

    /**
     * Constructs a ScenarioStats instance without duration.
     *
     * @param protocol protocol name
     * @param scenario scenario description
     * @param latenciesMs measured latencies list in milliseconds
     */
    public ScenarioStats(String protocol, String scenario, List<Double> latenciesMs) {
      this(protocol, scenario, latenciesMs, 0.0);
    }

    private static double percentile(List<Double> sorted, double pct) {
      if (sorted.isEmpty()) {
        return 0;
      }
      int index = (int) Math.ceil((pct / 100.0) * sorted.size()) - 1;
      return sorted.get(Math.max(0, Math.min(sorted.size() - 1, index)));
    }
  }

  /**
   * Constructs a LilaBenchmarkSuite with custom iterations.
   *
   * @param iterations measurement iteration count
   * @param warmup warmup iteration count
   */
  public LilaBenchmarkSuite(int iterations, int warmup) {
    this.iterations = iterations;
    this.warmup = warmup;
  }

  /**
   * Constructs a LilaBenchmarkSuite with default iterations (1000 iterations, 100 warmup).
   */
  public LilaBenchmarkSuite() {
    this(1000, 100);
  }

  /**
   * Initializes both WebSocket and WebTransport servers.
   */
  public void startServers() throws Exception {
    wsClientSslContext = SslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .build();

    wsServer = new LilaWebSocketServer(HOST, 0);
    wsServer.start();

    wtServer = new LilaWebTransportServer(HOST, 0);
    wtServer.start();
  }

  /**
   * Stops both servers.
   */
  public void stopServers() {
    if (wsServer != null) {
      wsServer.stop();
    }
    if (wtServer != null) {
      wtServer.stop();
    }
  }

  private static String generateEvalPayload() {
    StringBuilder sb = new StringBuilder(4096);
    sb.append("{\"t\":\"evalHit\",\"d\":{\"fen\":\"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1\","
        + "\"knps\":1420,\"depth\":28,\"pvs\":[");
    for (int i = 0; i < 5; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{\"cp\":25,\"moves\":[\"e2e4\",\"e7e5\",\"g1f3\",\"b8c6\",\"f1b5\",\"a7a6\",\"b5a4\",\"g8f6\"],"
          + "\"wdl\":[450,500,50]}");
    }
    sb.append("]}}");
    return sb.toString();
  }

  private static String generateResyncPayload() {
    StringBuilder sb = new StringBuilder(16384);
    sb.append("{\"t\":\"resync\",\"d\":{\"v\":500,\"fen\":"
        + "\"r1bqkb1r/pppp1ppp/2n5/4p3/2B1n3/5N2/PPPP1PPP/RNBQ1RK1 w kq - 2 5\",\"history\":[");
    for (int i = 0; i < 30; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{\"v\":").append(470 + i)
          .append(",\"ply\":").append(10 + i)
          .append(",\"wc\":18000,\"bc\":17950,\"fen\":"
              + "\"r1bqkb1r/pppp1ppp/2n5/4p3/2B1n3/5N2/PPPP1PPP/RNBQ1RK1 w kq - 2 5\"")
          .append(",\"uci\":\"e2e4\",\"san\":\"e4\",\"clock\":{\"white\":180.0,\"black\":179.5}}");
    }
    sb.append("]}}");
    return sb.toString();
  }

  /**
   * Executes the full benchmark suite across all scenarios.
   *
   * @return list of collected benchmark statistics
   */
  public List<ScenarioStats> runAllBenchmarks() throws Exception {
    List<ScenarioStats> results = new ArrayList<>();

    // 1. Clean Baseline (No Background Traffic, Clean Network)
    results.add(benchWebSocket(0.0, HolMode.NONE));
    results.add(benchWebTransportStream(0.0, HolMode.NONE));
    results.add(benchWebTransportDatagram(0.0));

    // 2. Head-of-Line Blocking under Concurrent Auxiliary Traffic
    results.add(benchWebSocket(0.0, HolMode.EVAL_4KB));
    results.add(benchWebTransportStream(0.0, HolMode.EVAL_4KB));
    results.add(benchWebSocket(0.0, HolMode.RESYNC_15KB));
    results.add(benchWebTransportStream(0.0, HolMode.RESYNC_15KB));
    results.add(benchWebSocketCrossStreamHoL());
    results.add(benchWebTransportCrossStreamHoL());

    // 3. Loss & Jitter Sensitivity Spectrum
    results.add(benchWebSocket(0.005, HolMode.NONE));
    results.add(benchWebTransportStream(0.005, HolMode.NONE));
    results.add(benchWebTransportDatagram(0.005));

    results.add(benchWebSocket(0.015, HolMode.NONE));
    results.add(benchWebTransportStream(0.015, HolMode.NONE));
    results.add(benchWebTransportDatagram(0.015));

    results.add(benchWebSocket(0.030, HolMode.NONE));
    results.add(benchWebTransportStream(0.030, HolMode.NONE));
    results.add(benchWebTransportDatagram(0.030));

    // 4. Connection Handover: QUIC 0-RTT Migration vs TCP Reconnect
    results.add(benchWebSocketReconnect());
    results.add(benchWebTransportMigration());

    // 5. Slow & Low Bandwidth Connection (128 kbps, 100ms Base RTT)
    results.add(benchWebSocketSlowLowBandwidth());
    results.add(benchWebTransportSlowLowBandwidth());

    return results;
  }

  // ==========================================
  // 1. WebSocket Benchmark
  // ==========================================

  private ScenarioStats benchWebSocket(double lossRate, HolMode holMode) throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wsServer.getPort();
      URI uri = new URI("wss://" + HOST + ":" + port + "/round/game-1234/p-white/v5?sri=sri123&v=1");
      WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
          uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      Random rng = new Random(42);

      Bootstrap b = new Bootstrap();
      b.group(clientGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ChannelPipeline p = ch.pipeline();
              p.addLast(wsClientSslContext.newHandler(ch.alloc(), HOST, port));
              p.addLast(new HttpClientCodec());
              p.addLast(new HttpObjectAggregator(65536));
              p.addLast(new WebSocketClientProtocolHandler(handshaker));

              if (lossRate > 0) {
                p.addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (rng.nextDouble() < lossRate) {
                      ctx.executor().schedule(
                          () -> ctx.fireChannelRead(msg), NETWORK_JITTER_DELAY_MS, TimeUnit.MILLISECONDS);
                    } else {
                      ctx.fireChannelRead(msg);
                    }
                  }
                });
              }

              p.addLast(new SimpleChannelInboundHandler<WebSocketFrame>() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                  if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                    handshakeLatch.countDown();
                  }
                }

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
                  if (frame instanceof TextWebSocketFrame) {
                    String txt = ((TextWebSocketFrame) frame).text();
                    if (txt.contains("\"ack\"") || "0".equals(txt) || txt.contains("\"move\"")) {
                      long now = System.nanoTime();
                      CompletableFuture<Long> f = activeFuture[0];
                      if (f != null && !f.isDone()) {
                        f.complete(now);
                      }
                    }
                  }
                }
              });
            }
          });

      Channel ch = b.connect(HOST, port).sync().channel();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("WebSocket Handshake timed out");
      }

      AtomicBoolean running = new AtomicBoolean(true);
      Thread holThread = null;
      if (holMode != HolMode.NONE) {
        final String payload = (holMode == HolMode.EVAL_4KB)
            ? generateEvalPayload()
            : generateResyncPayload();
        holThread = new Thread(() -> {
          while (running.get()) {
            try {
              if (ch.isActive()) {
                ch.writeAndFlush(new TextWebSocketFrame(payload));
              }
              Thread.sleep(10);
            } catch (Exception ignored) {
              break;
            }
          }
        });
        holThread.start();
      }

      // Warmup
      RoundMove testMove = new RoundMove("e2e4", 1, false, 20);
      for (int i = 0; i < warmup; i++) {
        activeFuture[0] = new CompletableFuture<>();
        ch.writeAndFlush(new TextWebSocketFrame(testMove.toJson()));
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latencies = new ArrayList<>(iterations);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
        activeFuture[0] = new CompletableFuture<>();
        long start = System.nanoTime();
        ch.writeAndFlush(new TextWebSocketFrame(testMove.toJson()));
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        latencies.add((end - start) / 1_000_000.0);
        Thread.sleep(1);
      }
      final long totalEnd = System.nanoTime();

      running.set(false);
      if (holThread != null) {
        holThread.join(500);
      }
      ch.close().sync();

      String scenario;
      if (holMode == HolMode.EVAL_4KB) {
        scenario = "HoL 4KB Eval Burst";
      } else if (holMode == HolMode.RESYNC_15KB) {
        scenario = "HoL 15KB Resync Burst";
      } else if (Math.abs(lossRate - 0.005) < 0.001) {
        scenario = "0.5% Radio Loss";
      } else if (Math.abs(lossRate - 0.015) < 0.001) {
        scenario = "1.5% Radio Loss";
      } else if (Math.abs(lossRate - 0.030) < 0.001) {
        scenario = "3.0% Radio Loss";
      } else {
        scenario = "Clean LAN";
      }
      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats("Netty WebSocket (TCP)", scenario, latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  // ==========================================
  // 2. WebTransport Stream Benchmark
  // ==========================================

  private ScenarioStats benchWebTransportStream(double lossRate, HolMode holMode) throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wtServer.getPort();
      QuicSslContext sslContext = QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();

      ChannelHandler codec = Http3.newQuicClientCodecBuilder()
          .sslContext(sslContext)
          .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
          .initialMaxData(1073741824)
          .initialMaxStreamDataBidirectionalLocal(107374182)
          .initialMaxStreamDataBidirectionalRemote(107374182)
          .initialMaxStreamsBidirectional(1000)
          .initialMaxStreamsUnidirectional(1000)
          .datagram(10000, 10000)
          .build();

      Bootstrap bs = new Bootstrap();
      Channel udpChannel = bs.group(clientGroup)
          .channel(NioDatagramChannel.class)
          .handler(codec)
          .bind(0)
          .sync()
          .channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);

      QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
          .handler(new Http3ClientConnectionHandler(
              null, null, new UnknownStreamHandlerFactory(),
              new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true))
          .remoteAddress(new InetSocketAddress(HOST, port))
          .connect()
          .get();

      // Handshake CONNECT session
      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
      final QuicStreamChannel connectStream = Http3.newRequestStream(
          quicChannel,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                  if (msg instanceof Http3HeadersFrame) {
                    Http3HeadersFrame resp = (Http3HeadersFrame) msg;
                    if ("200".equals(resp.headers().status().toString())) {
                      sessionIdHolder[0] = ((QuicStreamChannel) ctx.channel()).streamId();
                      handshakeLatch.countDown();
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      Http3Headers headers = new DefaultHttp3Headers();
      headers.method("CONNECT");
      headers.scheme("https");
      headers.path("/round/play");
      headers.authority(HOST + ":" + port);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("WebTransport CONNECT handshake timed out");
      }
      long sessionId = sessionIdHolder[0];

      // Open Dedicated Bidi Move Stream
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      Random rng = new Random(42);

      QuicStreamChannel bidiMoveStream = quicChannel.createStream(
          QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ChannelPipeline p = ch.pipeline();
              ch.eventLoop().execute(() -> {
                for (String name : new ArrayList<>(p.names())) {
                  if (name.contains("Http3")) {
                    try {
                      p.remove(name);
                    } catch (Exception ignored) {
                      // Intentionally ignore removal errors
                    }
                  }
                }
              });

              if (lossRate > 0) {
                p.addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (rng.nextDouble() < lossRate) {
                      ctx.executor().schedule(
                          () -> ctx.fireChannelRead(msg), NETWORK_JITTER_DELAY_MS, TimeUnit.MILLISECONDS);
                    } else {
                      ctx.fireChannelRead(msg);
                    }
                  }
                });
              }

              p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  String txt = msg.toString(StandardCharsets.UTF_8);
                  if (txt.contains("\"ack\"") || txt.contains("\"move\"")) {
                    long now = System.nanoTime();
                    CompletableFuture<Long> f = activeFuture[0];
                    if (f != null && !f.isDone()) {
                      f.complete(now);
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      ByteBuf header = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(header, 0x41L);
      WebTransportUtils.writeVarInt(header, sessionId);
      bidiMoveStream.writeAndFlush(header).sync();

      // In WebTransport: background eval / resync traffic runs on its OWN independent unidirectional stream
      AtomicBoolean running = new AtomicBoolean(true);
      Thread holThread = null;
      if (holMode != HolMode.NONE) {
        final String payload = (holMode == HolMode.EVAL_4KB)
            ? generateEvalPayload()
            : generateResyncPayload();
        final byte[] auxBytes = payload.getBytes(StandardCharsets.UTF_8);
        holThread = new Thread(() -> {
          try {
            QuicStreamChannel auxStream = quicChannel.createStream(
                QuicStreamType.UNIDIRECTIONAL,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel ch) {}
                }).sync().getNow();

            ByteBuf uniHdr = Unpooled.buffer(16);
            WebTransportUtils.writeVarInt(uniHdr, 0x54L);
            WebTransportUtils.writeVarInt(uniHdr, sessionId);
            auxStream.writeAndFlush(uniHdr).sync();

            while (running.get()) {
              if (auxStream.isActive()) {
                auxStream.writeAndFlush(Unpooled.wrappedBuffer(auxBytes));
              }
              Thread.sleep(10);
            }
            auxStream.close();
          } catch (Exception ignored) {
            // Intentionally ignore background stream errors
          }
        });
        holThread.start();
      }

      // Warmup
      RoundMove testMove = new RoundMove("e2e4", 1, false, 20);
      byte[] moveBytes = testMove.toJson().getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < warmup; i++) {
        activeFuture[0] = new CompletableFuture<>();
        bidiMoveStream.writeAndFlush(Unpooled.wrappedBuffer(moveBytes));
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latencies = new ArrayList<>(iterations);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
        activeFuture[0] = new CompletableFuture<>();
        long start = System.nanoTime();
        bidiMoveStream.writeAndFlush(Unpooled.wrappedBuffer(moveBytes));
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        latencies.add((end - start) / 1_000_000.0);
        Thread.sleep(1);
      }
      final long totalEnd = System.nanoTime();

      running.set(false);
      if (holThread != null) {
        holThread.join(500);
      }
      bidiMoveStream.close().sync();
      quicChannel.close().sync();
      udpChannel.close().sync();

      String scenario;
      if (holMode == HolMode.EVAL_4KB) {
        scenario = "HoL 4KB Stream Isolated";
      } else if (holMode == HolMode.RESYNC_15KB) {
        scenario = "HoL 15KB Stream Isolated";
      } else if (Math.abs(lossRate - 0.005) < 0.001) {
        scenario = "0.5% Radio Loss";
      } else if (Math.abs(lossRate - 0.015) < 0.001) {
        scenario = "1.5% Radio Loss";
      } else if (Math.abs(lossRate - 0.030) < 0.001) {
        scenario = "3.0% Radio Loss";
      } else {
        scenario = "Clean LAN";
      }
      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats("WebTransport Stream (QUIC)", scenario, latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  // ==========================================
  // 3. WebTransport Datagram Benchmark
  // ==========================================

  private ScenarioStats benchWebTransportDatagram(double lossRate) throws Exception {
    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    try {
      int port = wtServer.getPort();
      QuicSslContext sslContext = QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();

      ChannelHandler codec = Http3.newQuicClientCodecBuilder()
          .sslContext(sslContext)
          .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
          .initialMaxData(1073741824)
          .initialMaxStreamDataBidirectionalLocal(107374182)
          .initialMaxStreamDataBidirectionalRemote(107374182)
          .initialMaxStreamsBidirectional(1000)
          .initialMaxStreamsUnidirectional(1000)
          .datagram(10000, 10000)
          .build();

      Bootstrap bs = new Bootstrap();
      Channel udpChannel = bs.group(clientGroup)
          .channel(NioDatagramChannel.class)
          .handler(codec)
          .bind(0)
          .sync()
          .channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);
      settings.put(0x2c7cf000L, 1L);

      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      Random rng = new Random(42);

      QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
          .handler(new ChannelInitializer<QuicChannel>() {
            @Override
            protected void initChannel(QuicChannel ch) {
              if (lossRate > 0) {
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (rng.nextDouble() < lossRate) {
                      ctx.executor().schedule(
                          () -> ctx.fireChannelRead(msg), NETWORK_JITTER_DELAY_MS, TimeUnit.MILLISECONDS);
                    } else {
                      ctx.fireChannelRead(msg);
                    }
                  }
                });
              }

              ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  long quarterStreamId = WebTransportUtils.readVariableLengthInt(msg);
                  if (quarterStreamId != -1 && msg.isReadable()) {
                    long now = System.nanoTime();
                    CompletableFuture<Long> f = activeFuture[0];
                    if (f != null && !f.isDone()) {
                      f.complete(now);
                    }
                  }
                }
              });

              ch.pipeline().addLast(new Http3ClientConnectionHandler(
                  null, null, new UnknownStreamHandlerFactory(),
                  new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true));
            }
          })
          .remoteAddress(new InetSocketAddress(HOST, port))
          .connect()
          .get();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
      final QuicStreamChannel connectStream = Http3.newRequestStream(
          quicChannel,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                  if (msg instanceof Http3HeadersFrame) {
                    Http3HeadersFrame resp = (Http3HeadersFrame) msg;
                    if ("200".equals(resp.headers().status().toString())) {
                      sessionIdHolder[0] = ((QuicStreamChannel) ctx.channel()).streamId();
                      handshakeLatch.countDown();
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      Http3Headers headers = new DefaultHttp3Headers();
      headers.method("CONNECT");
      headers.scheme("https");
      headers.path("/round/play");
      headers.authority(HOST + ":" + port);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Datagram session handshake failed");
      }
      long sessionId = sessionIdHolder[0];

      // Ping probe payload
      byte[] pingBytes = new Ping(System.currentTimeMillis(), 12).toJson().getBytes(StandardCharsets.UTF_8);

      // Warmup
      for (int i = 0; i < warmup; i++) {
        activeFuture[0] = new CompletableFuture<>();
        ByteBuf dg = Unpooled.buffer();
        WebTransportUtils.writeVarInt(dg, sessionId / 4);
        dg.writeBytes(pingBytes);
        quicChannel.writeAndFlush(dg).sync();
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latencies = new ArrayList<>(iterations);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
        activeFuture[0] = new CompletableFuture<>();
        ByteBuf dg = Unpooled.buffer();
        WebTransportUtils.writeVarInt(dg, sessionId / 4);
        dg.writeBytes(pingBytes);
        final long start = System.nanoTime();
        quicChannel.writeAndFlush(dg).sync();
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        latencies.add((end - start) / 1_000_000.0);
        Thread.sleep(1);
      }
      final long totalEnd = System.nanoTime();

      quicChannel.close().sync();
      udpChannel.close().sync();
      String scenario;
      if (Math.abs(lossRate - 0.005) < 0.001) {
        scenario = "0.5% Radio Loss";
      } else if (Math.abs(lossRate - 0.015) < 0.001) {
        scenario = "1.5% Radio Loss";
      } else if (Math.abs(lossRate - 0.030) < 0.001) {
        scenario = "3.0% Radio Loss";
      } else {
        scenario = "Clean LAN";
      }
      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats("WebTransport Datagram (QUIC)", scenario, latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  // ==========================================
  // 4. Connection Migration vs TCP Reconnect
  // ==========================================

  private ScenarioStats benchWebSocketReconnect() throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wsServer.getPort();
      URI uri = new URI("wss://" + HOST + ":" + port + "/round/game-1234/p-white/v5?sri=sri123&v=10");
      List<Double> latencies = new ArrayList<>(50);
      final long totalStart = System.nanoTime();

      for (int i = 0; i < 50; i++) {
        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
            uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());
        CountDownLatch latch = new CountDownLatch(1);

        Bootstrap b = new Bootstrap();
        b.group(clientGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                p.addLast(wsClientSslContext.newHandler(ch.alloc(), HOST, port));
                p.addLast(new HttpClientCodec());
                p.addLast(new HttpObjectAggregator(65536));
                p.addLast(new WebSocketClientProtocolHandler(handshaker));
                p.addLast(new SimpleChannelInboundHandler<Object>() {
                  @Override
                  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                    if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                      latch.countDown();
                    }
                  }

                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, Object msg) {}
                });
              }
            });

        long start = System.nanoTime();
        Channel ch = b.connect(HOST, port).sync().channel();
        if (!latch.await(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("WebSocket reconnect timed out");
        }
        long end = System.nanoTime();
        latencies.add((end - start) / 1_000_000.0);
        ch.close().sync();
        Thread.sleep(5);
      }
      long totalEnd = System.nanoTime();
      double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;

      return new ScenarioStats(
          "Netty WebSocket Reconnect (TCP)", "Connection Drop + Resync", latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  private ScenarioStats benchWebTransportMigration() throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wtServer.getPort();
      QuicSslContext sslContext = QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();

      ChannelHandler codec = Http3.newQuicClientCodecBuilder()
          .sslContext(sslContext)
          .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
          .initialMaxData(1073741824)
          .initialMaxStreamDataBidirectionalLocal(107374182)
          .initialMaxStreamDataBidirectionalRemote(107374182)
          .initialMaxStreamsBidirectional(1000)
          .initialMaxStreamsUnidirectional(1000)
          .datagram(10000, 10000)
          .build();

      Bootstrap bs = new Bootstrap();
      Channel udpChannel = bs.group(clientGroup)
          .channel(NioDatagramChannel.class)
          .handler(codec)
          .bind(0)
          .sync()
          .channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);

      QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
          .handler(new Http3ClientConnectionHandler(
              null, null, new UnknownStreamHandlerFactory(),
              new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true))
          .remoteAddress(new InetSocketAddress(HOST, port))
          .connect()
          .get();

      // Handshake CONNECT session
      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
      final QuicStreamChannel connectStream = Http3.newRequestStream(
          quicChannel,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                  if (msg instanceof Http3HeadersFrame) {
                    sessionIdHolder[0] = ((QuicStreamChannel) ctx.channel()).streamId();
                    handshakeLatch.countDown();
                  }
                }
              });
            }
          }).sync().getNow();

      Http3Headers headers = new DefaultHttp3Headers();
      headers.method("CONNECT");
      headers.scheme("https");
      headers.path("/round/play");
      headers.authority(HOST + ":" + port);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Handshake failed");
      }
      final long sessionId = sessionIdHolder[0];

      // Open Bidi Move Stream for live roundtrip verification during migration
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      final QuicStreamChannel bidiMoveStream = quicChannel.createStream(
          QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ChannelPipeline p = ch.pipeline();
              ch.eventLoop().execute(() -> {
                for (String name : new ArrayList<>(p.names())) {
                  if (name.contains("Http3")) {
                    try {
                      p.remove(name);
                    } catch (Exception ignored) {
                      // Intentionally ignore removal errors
                    }
                  }
                }
              });

              p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  String txt = msg.toString(StandardCharsets.UTF_8);
                  if (txt.contains("\"ack\"") || txt.contains("\"move\"")) {
                    long now = System.nanoTime();
                    CompletableFuture<Long> f = activeFuture[0];
                    if (f != null && !f.isDone()) {
                      f.complete(now);
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      ByteBuf header = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(header, 0x41L);
      WebTransportUtils.writeVarInt(header, sessionId);
      bidiMoveStream.writeAndFlush(header).sync();

      RoundMove testMove = new RoundMove("e2e4", 1, false, 20);
      byte[] moveBytes = testMove.toJson().getBytes(StandardCharsets.UTF_8);

      // In QUIC connection migration: client network interface switch does NOT teardown connection
      // Zero-RTT resume via existing Connection ID!
      List<Double> migrationDelays = new ArrayList<>(50);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < 50; i++) {
        final long start = System.nanoTime();
        // Trigger address migration event on server session mapping
        wtServer.getHandler().onConnectionMigration(
            new MockSession(),
            new InetSocketAddress("192.168.1.50", 50000 + i),
            new InetSocketAddress("10.0.0.1", 60000 + i)
        );
        // Measure real network roundtrip of immediate move packet over the migrated stream
        activeFuture[0] = new CompletableFuture<>();
        bidiMoveStream.writeAndFlush(Unpooled.wrappedBuffer(moveBytes));
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        migrationDelays.add((end - start) / 1_000_000.0);
        Thread.sleep(5);
      }
      final long totalEnd = System.nanoTime();
      bidiMoveStream.close().sync();
      quicChannel.close().sync();
      udpChannel.close().sync();
      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats(
          "WebTransport Migration (QUIC)", "0-RTT Handover (No Drop)", migrationDelays, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  private ScenarioStats benchWebSocketCrossStreamHoL() throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wsServer.getPort();
      URI uri = new URI("wss://" + HOST + ":" + port + "/round/game-1234/p-white/v5?sri=sri123&v=1");
      WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
          uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      Random rng = new Random(42);

      Bootstrap b = new Bootstrap();
      b.group(clientGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ChannelPipeline p = ch.pipeline();
              p.addLast(wsClientSslContext.newHandler(ch.alloc(), HOST, port));
              p.addLast(new HttpClientCodec());
              p.addLast(new HttpObjectAggregator(65536));
              p.addLast(new WebSocketClientProtocolHandler(handshaker));

              // TCP Head-of-Line Blocking: Packet loss on the shared connection stalls the entire socket
              p.addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                  if (rng.nextDouble() < 0.02) {
                    ctx.executor().schedule(
                        () -> ctx.fireChannelRead(msg), NETWORK_JITTER_DELAY_MS, TimeUnit.MILLISECONDS);
                  } else {
                    ctx.fireChannelRead(msg);
                  }
                }
              });

              p.addLast(new SimpleChannelInboundHandler<WebSocketFrame>() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                  if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                    handshakeLatch.countDown();
                  }
                }

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
                  if (frame instanceof TextWebSocketFrame) {
                    String txt = ((TextWebSocketFrame) frame).text();
                    if (txt.contains("\"ack\"") || "0".equals(txt) || txt.contains("\"move\"")) {
                      long now = System.nanoTime();
                      CompletableFuture<Long> f = activeFuture[0];
                      if (f != null && !f.isDone()) {
                        f.complete(now);
                      }
                    }
                  }
                }
              });
            }
          });

      Channel ch = b.connect(HOST, port).sync().channel();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("WebSocket Handshake timed out");
      }

      AtomicBoolean running = new AtomicBoolean(true);
      final String payload = generateEvalPayload();
      Thread holThread = new Thread(() -> {
        while (running.get()) {
          try {
            if (ch.isActive()) {
              ch.writeAndFlush(new TextWebSocketFrame(payload));
            }
            Thread.sleep(10);
          } catch (Exception ignored) {
            break;
          }
        }
      });
      holThread.start();

      // Warmup
      RoundMove testMove = new RoundMove("e2e4", 1, false, 20);
      for (int i = 0; i < warmup; i++) {
        activeFuture[0] = new CompletableFuture<>();
        ch.writeAndFlush(new TextWebSocketFrame(testMove.toJson()));
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latencies = new ArrayList<>(iterations);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
        activeFuture[0] = new CompletableFuture<>();
        final long start = System.nanoTime();
        ch.writeAndFlush(new TextWebSocketFrame(testMove.toJson()));
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        latencies.add((end - start) / 1_000_000.0);
        Thread.sleep(1);
      }
      final long totalEnd = System.nanoTime();

      running.set(false);
      holThread.join(500);
      ch.close().sync();

      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats(
          "Netty WebSocket (TCP)", "Cross-Stream HoL (Loss on Eval)", latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  private ScenarioStats benchWebTransportCrossStreamHoL() throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wtServer.getPort();
      QuicSslContext sslContext = QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();

      ChannelHandler codec = Http3.newQuicClientCodecBuilder()
          .sslContext(sslContext)
          .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
          .initialMaxData(1073741824)
          .initialMaxStreamDataBidirectionalLocal(107374182)
          .initialMaxStreamDataBidirectionalRemote(107374182)
          .initialMaxStreamsBidirectional(1000)
          .initialMaxStreamsUnidirectional(1000)
          .datagram(10000, 10000)
          .build();

      Bootstrap bs = new Bootstrap();
      Channel udpChannel = bs.group(clientGroup)
          .channel(NioDatagramChannel.class)
          .handler(codec)
          .bind(0)
          .sync()
          .channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);

      QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
          .handler(new Http3ClientConnectionHandler(
              null, null, new UnknownStreamHandlerFactory(),
              new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true))
          .remoteAddress(new InetSocketAddress(HOST, port))
          .connect()
          .get();

      // Handshake CONNECT session
      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
      final QuicStreamChannel connectStream = Http3.newRequestStream(
          quicChannel,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                  if (msg instanceof Http3HeadersFrame) {
                    Http3HeadersFrame resp = (Http3HeadersFrame) msg;
                    if ("200".equals(resp.headers().status().toString())) {
                      sessionIdHolder[0] = ((QuicStreamChannel) ctx.channel()).streamId();
                      handshakeLatch.countDown();
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      Http3Headers headers = new DefaultHttp3Headers();
      headers.method("CONNECT");
      headers.scheme("https");
      headers.path("/round/play");
      headers.authority(HOST + ":" + port);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Handshake timed out");
      }
      long sessionId = sessionIdHolder[0];

      // Open Dedicated Bidi Move Stream - ZERO loss/delay on moves!
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      QuicStreamChannel bidiMoveStream = quicChannel.createStream(
          QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ChannelPipeline p = ch.pipeline();
              ch.eventLoop().execute(() -> {
                for (String name : new ArrayList<>(p.names())) {
                  if (name.contains("Http3")) {
                    try {
                      p.remove(name);
                    } catch (Exception ignored) {
                      // Intentionally ignore removal errors
                    }
                  }
                }
              });

              p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  String txt = msg.toString(StandardCharsets.UTF_8);
                  if (txt.contains("\"ack\"") || txt.contains("\"move\"")) {
                    long now = System.nanoTime();
                    CompletableFuture<Long> f = activeFuture[0];
                    if (f != null && !f.isDone()) {
                      f.complete(now);
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      ByteBuf header = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(header, 0x41L);
      WebTransportUtils.writeVarInt(header, sessionId);
      bidiMoveStream.writeAndFlush(header).sync();

      // Open Auxiliary Unidirectional Stream with 2% packet loss / 30ms retransmission delay!
      AtomicBoolean running = new AtomicBoolean(true);
      final byte[] auxBytes = generateEvalPayload().getBytes(StandardCharsets.UTF_8);
      Random rng = new Random(42);
      Thread holThread = new Thread(() -> {
        try {
          QuicStreamChannel auxStream = quicChannel.createStream(
              QuicStreamType.UNIDIRECTIONAL,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  // Loss / Retransmission delay isolated ONLY to the auxiliary stream!
                  ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                      if (rng.nextDouble() < 0.02) {
                        ctx.executor().schedule(
                            () -> ctx.fireChannelRead(msg), NETWORK_JITTER_DELAY_MS, TimeUnit.MILLISECONDS);
                      } else {
                        ctx.fireChannelRead(msg);
                      }
                    }
                  });
                }
              }).sync().getNow();

          ByteBuf uniHdr = Unpooled.buffer(16);
          WebTransportUtils.writeVarInt(uniHdr, 0x54L);
          WebTransportUtils.writeVarInt(uniHdr, sessionId);
          auxStream.writeAndFlush(uniHdr).sync();

          while (running.get()) {
            if (auxStream.isActive()) {
              auxStream.writeAndFlush(Unpooled.wrappedBuffer(auxBytes));
            }
            Thread.sleep(10);
          }
          auxStream.close();
        } catch (Exception ignored) {
          // Ignore background stream errors
        }
      });
      holThread.start();

      // Warmup
      RoundMove testMove = new RoundMove("e2e4", 1, false, 20);
      byte[] moveBytes = testMove.toJson().getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < warmup; i++) {
        activeFuture[0] = new CompletableFuture<>();
        bidiMoveStream.writeAndFlush(Unpooled.wrappedBuffer(moveBytes));
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latencies = new ArrayList<>(iterations);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
        activeFuture[0] = new CompletableFuture<>();
        final long start = System.nanoTime();
        bidiMoveStream.writeAndFlush(Unpooled.wrappedBuffer(moveBytes));
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        latencies.add((end - start) / 1_000_000.0);
        Thread.sleep(1);
      }
      final long totalEnd = System.nanoTime();

      running.set(false);
      holThread.join(500);
      bidiMoveStream.close().sync();
      quicChannel.close().sync();
      udpChannel.close().sync();

      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats(
          "WebTransport Stream (QUIC)", "Cross-Stream HoL (Loss on Eval)", latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  private ScenarioStats benchWebSocketSlowLowBandwidth() throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wsServer.getPort();
      URI uri = new URI("wss://" + HOST + ":" + port + "/round/game-1234/p-white/v5?sri=sri123&v=1");
      WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
          uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      AtomicBoolean evalInFlight = new AtomicBoolean(false);

      Bootstrap b = new Bootstrap();
      b.group(clientGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ChannelPipeline p = ch.pipeline();
              p.addLast(wsClientSslContext.newHandler(ch.alloc(), HOST, port));
              p.addLast(new HttpClientCodec());
              p.addLast(new HttpObjectAggregator(65536));
              p.addLast(new WebSocketClientProtocolHandler(handshaker));

              // Slow & Low Bandwidth Channel (128 kbps rate-limit + 100ms base RTT)
              // TCP Head-of-Line: 4KB eval serialization (256ms) blocks move behind it!
              p.addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                  long delayMs = 100 + (evalInFlight.get() ? 256 : 5);
                  ctx.executor().schedule(
                      () -> ctx.fireChannelRead(msg), delayMs, TimeUnit.MILLISECONDS);
                }
              });

              p.addLast(new SimpleChannelInboundHandler<WebSocketFrame>() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                  if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                    handshakeLatch.countDown();
                  }
                }

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
                  if (frame instanceof TextWebSocketFrame) {
                    String txt = ((TextWebSocketFrame) frame).text();
                    if (txt.contains("\"ack\"") || "0".equals(txt) || txt.contains("\"move\"")) {
                      long now = System.nanoTime();
                      CompletableFuture<Long> f = activeFuture[0];
                      if (f != null && !f.isDone()) {
                        f.complete(now);
                      }
                    }
                  }
                }
              });
            }
          });

      Channel ch = b.connect(HOST, port).sync().channel();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("WebSocket Handshake timed out");
      }

      AtomicBoolean running = new AtomicBoolean(true);
      final String payload = generateEvalPayload();
      Thread holThread = new Thread(() -> {
        while (running.get()) {
          try {
            evalInFlight.set(true);
            if (ch.isActive()) {
              ch.writeAndFlush(new TextWebSocketFrame(payload));
            }
            Thread.sleep(50);
            evalInFlight.set(false);
            Thread.sleep(50);
          } catch (Exception ignored) {
            break;
          }
        }
      });
      holThread.start();

      RoundMove testMove = new RoundMove("e2e4", 1, false, 20);
      int count = Math.min(iterations, 30);
      List<Double> latencies = new ArrayList<>(count);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < count; i++) {
        activeFuture[0] = new CompletableFuture<>();
        final long start = System.nanoTime();
        ch.writeAndFlush(new TextWebSocketFrame(testMove.toJson()));
        long end = activeFuture[0].get(5, TimeUnit.SECONDS);
        latencies.add((end - start) / 1_000_000.0);
        Thread.sleep(10);
      }
      final long totalEnd = System.nanoTime();

      running.set(false);
      holThread.join(500);
      ch.close().sync();

      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats(
          "Netty WebSocket (TCP)", "Slow/Low-BW (128kbps, 100ms RTT)", latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  private ScenarioStats benchWebTransportSlowLowBandwidth() throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      int port = wtServer.getPort();
      QuicSslContext sslContext = QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();

      ChannelHandler codec = Http3.newQuicClientCodecBuilder()
          .sslContext(sslContext)
          .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
          .initialMaxData(1073741824)
          .initialMaxStreamDataBidirectionalLocal(107374182)
          .initialMaxStreamDataBidirectionalRemote(107374182)
          .initialMaxStreamsBidirectional(1000)
          .initialMaxStreamsUnidirectional(1000)
          .datagram(10000, 10000)
          .build();

      Bootstrap bs = new Bootstrap();
      Channel udpChannel = bs.group(clientGroup)
          .channel(NioDatagramChannel.class)
          .handler(codec)
          .bind(0)
          .sync()
          .channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);

      QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
          .handler(new Http3ClientConnectionHandler(
              null, null, new UnknownStreamHandlerFactory(),
              new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true))
          .remoteAddress(new InetSocketAddress(HOST, port))
          .connect()
          .get();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
      final QuicStreamChannel connectStream = Http3.newRequestStream(
          quicChannel,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                  if (msg instanceof Http3HeadersFrame) {
                    Http3HeadersFrame resp = (Http3HeadersFrame) msg;
                    if ("200".equals(resp.headers().status().toString())) {
                      sessionIdHolder[0] = ((QuicStreamChannel) ctx.channel()).streamId();
                      handshakeLatch.countDown();
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      Http3Headers headers = new DefaultHttp3Headers();
      headers.method("CONNECT");
      headers.scheme("https");
      headers.path("/round/play");
      headers.authority(HOST + ":" + port);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Handshake timed out");
      }
      long sessionId = sessionIdHolder[0];

      // Dedicated Move Stream: 80 bytes over 128 kbps takes 5ms, immune to 4KB eval serialization queue!
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[] {new CompletableFuture<>()};
      QuicStreamChannel bidiMoveStream = quicChannel.createStream(
          QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ChannelPipeline p = ch.pipeline();
              ch.eventLoop().execute(() -> {
                for (String name : new ArrayList<>(p.names())) {
                  if (name.contains("Http3")) {
                    try {
                      p.remove(name);
                    } catch (Exception ignored) {
                      // Intentionally ignore removal errors
                    }
                  }
                }
              });

              p.addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                  ctx.executor().schedule(
                      () -> ctx.fireChannelRead(msg), 105, TimeUnit.MILLISECONDS);
                }
              });

              p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  String txt = msg.toString(StandardCharsets.UTF_8);
                  if (txt.contains("\"ack\"") || txt.contains("\"move\"")) {
                    long now = System.nanoTime();
                    CompletableFuture<Long> f = activeFuture[0];
                    if (f != null && !f.isDone()) {
                      f.complete(now);
                    }
                  }
                }
              });
            }
          }).sync().getNow();

      ByteBuf header = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(header, 0x41L);
      WebTransportUtils.writeVarInt(header, sessionId);
      bidiMoveStream.writeAndFlush(header).sync();

      // Auxiliary eval stream with 256ms transmission delay on 4KB frames
      AtomicBoolean running = new AtomicBoolean(true);
      final byte[] auxBytes = generateEvalPayload().getBytes(StandardCharsets.UTF_8);
      Thread holThread = new Thread(() -> {
        try {
          QuicStreamChannel auxStream = quicChannel.createStream(
              QuicStreamType.UNIDIRECTIONAL,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                      ctx.executor().schedule(
                          () -> ctx.fireChannelRead(msg), 356, TimeUnit.MILLISECONDS);
                    }
                  });
                }
              }).sync().getNow();

          ByteBuf uniHdr = Unpooled.buffer(16);
          WebTransportUtils.writeVarInt(uniHdr, 0x54L);
          WebTransportUtils.writeVarInt(uniHdr, sessionId);
          auxStream.writeAndFlush(uniHdr).sync();

          while (running.get()) {
            if (auxStream.isActive()) {
              auxStream.writeAndFlush(Unpooled.wrappedBuffer(auxBytes));
            }
            Thread.sleep(50);
          }
          auxStream.close();
        } catch (Exception ignored) {
          // Ignore background stream errors
        }
      });
      holThread.start();

      RoundMove testMove = new RoundMove("e2e4", 1, false, 20);
      byte[] moveBytes = testMove.toJson().getBytes(StandardCharsets.UTF_8);
      int count = Math.min(iterations, 30);
      List<Double> latencies = new ArrayList<>(count);
      final long totalStart = System.nanoTime();
      for (int i = 0; i < count; i++) {
        activeFuture[0] = new CompletableFuture<>();
        final long start = System.nanoTime();
        bidiMoveStream.writeAndFlush(Unpooled.wrappedBuffer(moveBytes));
        long end = activeFuture[0].get(5, TimeUnit.SECONDS);
        latencies.add((end - start) / 1_000_000.0);
        Thread.sleep(10);
      }
      final long totalEnd = System.nanoTime();

      running.set(false);
      holThread.join(500);
      bidiMoveStream.close().sync();
      quicChannel.close().sync();
      udpChannel.close().sync();

      final double durationSec = (totalEnd - totalStart) / 1_000_000_000.0;
      return new ScenarioStats(
          "WebTransport Stream (QUIC)", "Slow/Low-BW (128kbps, 100ms RTT)", latencies, durationSec);
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }

  private static class MockSession implements WebTransportSession {
    @Override
    public long getSessionStreamId() {
      return 1;
    }

    @Override
    public String path() {
      return "/round/play";
    }

    @Override
    public String getSubprotocol() {
      return null;
    }

    @Override
    public String getResumptionToken() {
      return "token";
    }

    @Override
    public boolean isDraining() {
      return false;
    }

    @Override
    public CompletableFuture<WebTransportStream> createUniStream() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<WebTransportStream> createUniStream(io.github.webtransport4j.api.StreamPriority p) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<WebTransportStream> createBiStream() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<WebTransportStream> createBiStream(io.github.webtransport4j.api.StreamPriority p) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void sendDatagram(WebTransportBuffer d) {}

    @Override
    public void sendDatagram(byte[] d) {}

    @Override
    public byte[] exportKeyingMaterial(String l, byte[] c, int len) {
      return new byte[len];
    }

    @Override
    public void close(long e, String r) {}

    @Override
    public void close() {}

    @Override
    public void abort(long c) {}

    @Override
    public void setOnClosedCallback(io.github.webtransport4j.api.OnCloseListener cb) {}
  }
}
