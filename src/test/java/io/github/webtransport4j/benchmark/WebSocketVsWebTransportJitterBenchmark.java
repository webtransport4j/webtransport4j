package io.github.webtransport4j.benchmark;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
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
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NonNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark comparing latency, tail percentiles, and jitter between:
 * 1. Netty WebSocket (TCP) - standard architecture used by Lichess lila-ws
 * 2. WebTransport Streams (QUIC)
 * 3. WebTransport Datagrams (QUIC)
 *
 * <p>Evaluates both Clean Network conditions and Real-World Network Jitter/Loss conditions.
 */
public class WebSocketVsWebTransportJitterBenchmark {

  private static final Logger log = LoggerFactory.getLogger(WebSocketVsWebTransportJitterBenchmark.class);

  private static final int ITERATIONS = 1000;
  private static final int WARMUP_ITERATIONS = 100;
  private static final byte[] CHESS_MOVE_PAYLOAD = "{\"t\":\"move\",\"d\":{\"u\":\"e2e4\",\"b\":1,\"l\":120}}".getBytes(StandardCharsets.UTF_8);
  private static final String HOST = "localhost";
  private static final String IPV6_HOST = HOST;

  // WebSocket Server
  private static EventLoopGroup wsBossGroup;
  private static EventLoopGroup wsWorkerGroup;
  private static Channel wsServerChannel;
  private static int wsPort;
  private static SslContext wsServerSslContext;
  private static SslContext wsClientSslContext;

  // WebTransport Server
  private static WebTransportServer wtServer;
  private static int wtPort;

  @BeforeClass
  public static void startServers() throws Exception {
    System.setProperty("webtransport4j.server.port", "0");
    System.setProperty("webtransport4j.dev_mode", "true");
    System.setProperty("webtransport4j.dispatch.execution.mode", "NETTY_EVENT_LOOP");
    System.setProperty("webtransport4j.quic.active.migration.enabled", "true");

    // 1. Start Netty Secure WebSocket Server (as in lila-ws with WSS on IPv6)
    SelfSignedCertificate wsSsc = new SelfSignedCertificate();
    wsServerSslContext = SslContextBuilder.forServer(wsSsc.certificate(), wsSsc.privateKey()).build();
    wsClientSslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

    wsBossGroup = new NioEventLoopGroup(1);
    wsWorkerGroup = new NioEventLoopGroup(2);
    ServerBootstrap wsBootstrap = new ServerBootstrap();
    wsBootstrap.group(wsBossGroup, wsWorkerGroup)
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(wsServerSslContext.newHandler(ch.alloc()));
            p.addLast(new HttpServerCodec());
            p.addLast(new HttpObjectAggregator(65536));
            p.addLast(new WebSocketServerProtocolHandler("/ws"));
            p.addLast(new SimpleChannelInboundHandler<WebSocketFrame>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
                // Echo binary frame back immediately (simulating game move ack / clock sync)
                ctx.writeAndFlush(new BinaryWebSocketFrame(frame.content().retainedDuplicate()));
              }
            });
          }
        });
    wsServerChannel = wsBootstrap.bind(HOST, 0).sync().channel();
    wsPort = ((InetSocketAddress) wsServerChannel.localAddress()).getPort();
    log.info("Started Netty Secure WebSocket (WSS) Server on port {}", wsPort);

    // 2. Start WebTransport Server
    WebTransportHandler handler = new WebTransportHandler() {
      @Override
      public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
        stream.onData(buffer -> {
          // Echo stream payload immediately
          stream.write(buffer.readBytes());
        });
      }

      @Override
      public void onDatagramReceived(@NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
        // Echo datagram immediately
        session.sendDatagram(data.readBytes());
      }
    };

    wtServer = new WebTransportServerBuilder()
        .host(HOST)
        .port(0)
        .defaultHandler(handler)
        .build();
    wtServer.registerHandler("/chess", handler);

    wtServer.start();
    wtPort = wtServer.getPort();
    log.info("Started WebTransport Server on port {}", wtPort);
  }

  @AfterClass
  public static void stopServers() throws Exception {
    if (wsServerChannel != null) {
      wsServerChannel.close().sync();
    }
    if (wsBossGroup != null) {
      wsBossGroup.shutdownGracefully();
    }
    if (wsWorkerGroup != null) {
      wsWorkerGroup.shutdownGracefully();
    }
    if (wtServer != null) {
      wtServer.stop();
    }
  }

  /** Benchmark statistics summary. */
  public static class LatencyStats {
    public final String name;
    public final String scenario;
    public final int count;
    public final double minMs;
    public final double meanMs;
    public final double medianMs;
    public final double p90Ms;
    public final double p95Ms;
    public final double p99Ms;
    public final double p999Ms;
    public final double maxMs;
    public final double jitterStdDevMs;
    public final double spikeRatio; // p99 / p50

    public LatencyStats(String name, String scenario, List<Double> latenciesMs) {
      this.name = name;
      this.scenario = scenario;
      this.count = latenciesMs.size();

      List<Double> sorted = new ArrayList<>(latenciesMs);
      Collections.sort(sorted);

      this.minMs = sorted.get(0);
      this.maxMs = sorted.get(sorted.size() - 1);
      this.medianMs = percentile(sorted, 50.0);
      this.p90Ms = percentile(sorted, 90.0);
      this.p95Ms = percentile(sorted, 95.0);
      this.p99Ms = percentile(sorted, 99.0);
      this.p999Ms = percentile(sorted, 99.9);

      double sum = 0;
      for (double val : sorted) {
        sum += val;
      }
      this.meanMs = sum / sorted.size();

      double varSum = 0;
      for (double val : sorted) {
        varSum += Math.pow(val - this.meanMs, 2);
      }
      this.jitterStdDevMs = Math.sqrt(varSum / sorted.size());
      this.spikeRatio = this.medianMs > 0 ? (this.p99Ms / this.medianMs) : 1.0;
    }

    private static double percentile(List<Double> sorted, double pct) {
      if (sorted.isEmpty()) return 0;
      int index = (int) Math.ceil((pct / 100.0) * sorted.size()) - 1;
      return sorted.get(Math.max(0, Math.min(sorted.size() - 1, index)));
    }
  }

  // ==========================================
  // Junit Test Runner & Reporter
  // ==========================================

  @Test
  public void runComprehensiveBenchmark() throws Exception {
    List<LatencyStats> allStats = new ArrayList<>();

    System.out.println("\n=========================================================================================");
    System.out.println("  LICHESS CHESS REAL-TIME LATENCY & JITTER BENCHMARK: NETTY WEBSOCKET vs WEBTRANSPORT   ");
    System.out.println("=========================================================================================");

    // 1. WebSocket - Clean Network
    LatencyStats wsClean = benchWebSocket(false);
    allStats.add(wsClean);

    // 2. WebTransport Streams - Clean Network
    LatencyStats wtStreamClean = benchWebTransportStream(false);
    allStats.add(wtStreamClean);

    // 3. WebTransport Datagrams - Clean Network
    LatencyStats wtDatagramClean = benchWebTransportDatagram(false);
    allStats.add(wtDatagramClean);

    // 4. WebSocket - Simulated Network Jitter / Packet Delay (1.5% packet delay/drop spike)
    LatencyStats wsJitter = benchWebSocket(true);
    allStats.add(wsJitter);

    // 5. WebTransport Streams - Simulated Network Jitter
    LatencyStats wtStreamJitter = benchWebTransportStream(true);
    allStats.add(wtStreamJitter);

    // 6. WebTransport Datagrams - Simulated Network Jitter
    LatencyStats wtDatagramJitter = benchWebTransportDatagram(true);
    allStats.add(wtDatagramJitter);

    // 7. WebSocket - Connection Drop & Full Reconnect (TCP + HTTP Upgrade)
    LatencyStats wsReconnect = benchWebSocketReconnect();
    allStats.add(wsReconnect);

    // 8. WebTransport - Connection Drop & Full Reconnect (QUIC + Extended CONNECT)
    LatencyStats wtReconnect = benchWebTransportReconnect();
    allStats.add(wtReconnect);

    printResults(allStats);
  }

  public static void main(String[] args) throws Exception {
    startServers();
    try {
      new WebSocketVsWebTransportJitterBenchmark().runComprehensiveBenchmark();
    } finally {
      stopServers();
    }
  }

  private void printResults(List<LatencyStats> statsList) {
    System.out.println("\n----------------------------------------------------------------------------------------------------------------------------------");
    System.out.printf("%-26s | %-12s | %-7s | %-7s | %-7s | %-7s | %-7s | %-8s | %-8s | %-7s%n",
        "Protocol & Transport", "Scenario", "Min(ms)", "p50(ms)", "Mean(ms)", "p95(ms)", "p99(ms)", "Max(ms)", "Jitterσ", "Spike (p99/p50)");
    System.out.println("----------------------------------------------------------------------------------------------------------------------------------");
    for (LatencyStats s : statsList) {
      System.out.printf("%-26s | %-12s | %7.3f | %7.3f | %7.3f | %7.3f | %7.3f | %8.3f | %8.3f | %6.1fx%n",
          s.name, s.scenario, s.minMs, s.medianMs, s.meanMs, s.p95Ms, s.p99Ms, s.maxMs, s.jitterStdDevMs, s.spikeRatio);
    }
    System.out.println("----------------------------------------------------------------------------------------------------------------------------------\n");
  }

  // ==========================================
  // WebSocket Benchmark Execution
  // ==========================================

  private LatencyStats benchWebSocket(boolean simulateJitter) throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      URI uri = new URI("wss://" + HOST + ":" + wsPort + "/ws");
      WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
          uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      CompletableFuture<Long> rttFuture = new CompletableFuture<>();
      final CompletableFuture<Long>[] activeFuture = new CompletableFuture[]{rttFuture};

      Random rng = new Random(42);

      Bootstrap b = new Bootstrap();
      b.group(clientGroup)
          .channel(NioSocketChannel.class)
          .option(ChannelOption.TCP_NODELAY, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ChannelPipeline p = ch.pipeline();
              p.addLast(wsClientSslContext.newHandler(ch.alloc(), HOST, wsPort));
              p.addLast(new HttpClientCodec());
              p.addLast(new HttpObjectAggregator(65536));
              p.addLast(new WebSocketClientProtocolHandler(handshaker));

              // If jitter simulation is active on TCP: simulate TCP head-of-line stall on 1.5% packets
              if (simulateJitter) {
                p.addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (rng.nextDouble() < 0.015) {
                      // Simulate 40ms TCP HOL retransmission delay spike
                      ctx.executor().schedule(() -> ctx.fireChannelRead(msg), 40, TimeUnit.MILLISECONDS);
                    } else {
                      ctx.fireChannelRead(msg);
                    }
                  }
                });
              }

              p.addLast(new SimpleChannelInboundHandler<Object>() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                  if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                    handshakeLatch.countDown();
                  }
                }

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                  if (msg instanceof BinaryWebSocketFrame) {
                    long now = System.nanoTime();
                    CompletableFuture<Long> f = activeFuture[0];
                    if (f != null && !f.isDone()) {
                      f.complete(now);
                    }
                  }
                }
              });
            }
          });

      Channel ch = b.connect(HOST, wsPort).sync().channel();
      handshakeLatch.await(5, TimeUnit.SECONDS);

      // Warmup
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        activeFuture[0] = new CompletableFuture<>();
        ch.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(CHESS_MOVE_PAYLOAD)));
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latenciesMs = new ArrayList<>(ITERATIONS);
      for (int i = 0; i < ITERATIONS; i++) {
        activeFuture[0] = new CompletableFuture<>();
        long start = System.nanoTime();
        ch.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(CHESS_MOVE_PAYLOAD)));
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        double rttMs = (end - start) / 1_000_000.0;
        latenciesMs.add(rttMs);

        // Small pacing (1ms) between chess ticks
        Thread.sleep(1);
      }

      ch.close().sync();
      return new LatencyStats("Netty WebSocket (WSS)", simulateJitter ? "1.5% Jitter" : "Clean LAN", latenciesMs);
    } finally {
      clientGroup.shutdownGracefully();
    }
  }

  // ==========================================
  // WebTransport Stream Benchmark Execution
  // ==========================================

  private LatencyStats benchWebTransportStream(boolean simulateJitter) throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
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
          .bind(0).sync().channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);

      CountDownLatch dummyLatch = new CountDownLatch(1);
      QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
          .handler(new Http3ClientConnectionHandler(null, null, new UnknownStreamHandlerFactory(),
              new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true))
          .remoteAddress(new InetSocketAddress(HOST, wtPort))
          .connect()
          .get();

      // Handshake CONNECT stream
      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
      QuicStreamChannel connectStream = Http3.newRequestStream(
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
      headers.path("/chess");
      headers.authority(HOST + ":" + wtPort);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      handshakeLatch.await(5, TimeUnit.SECONDS);
      long sessionId = sessionIdHolder[0];

      // Open a Bidirectional WebTransport Stream for moves
      CompletableFuture<Long>[] activeFuture = new CompletableFuture[]{new CompletableFuture<>()};
      Random rng = new Random(42);

      QuicStreamChannel bidiStream = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ChannelPipeline p = ch.pipeline();
              // Clean http3 handlers
              ch.eventLoop().execute(() -> {
                for (String name : new ArrayList<>(p.names())) {
                  if (name.contains("Http3")) {
                    try { p.remove(name); } catch (Exception ignored) {}
                  }
                }
              });

              if (simulateJitter) {
                p.addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (rng.nextDouble() < 0.015) {
                      // QUIC stream packet loss recovery (typical 20ms quick retransmit vs 40ms TCP HOL)
                      ctx.executor().schedule(() -> ctx.fireChannelRead(msg), 20, TimeUnit.MILLISECONDS);
                    } else {
                      ctx.fireChannelRead(msg);
                    }
                  }
                });
              }

              p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  long now = System.nanoTime();
                  CompletableFuture<Long> f = activeFuture[0];
                  if (f != null && !f.isDone()) {
                    f.complete(now);
                  }
                }
              });
            }
          }).sync().getNow();

      // Write WT Bidi header (0x41 + sessionId)
      ByteBuf header = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(header, 0x41L);
      WebTransportUtils.writeVarInt(header, sessionId);
      bidiStream.writeAndFlush(header).sync();

      // Warmup
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        activeFuture[0] = new CompletableFuture<>();
        bidiStream.writeAndFlush(Unpooled.wrappedBuffer(CHESS_MOVE_PAYLOAD));
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latenciesMs = new ArrayList<>(ITERATIONS);
      for (int i = 0; i < ITERATIONS; i++) {
        activeFuture[0] = new CompletableFuture<>();
        long start = System.nanoTime();
        bidiStream.writeAndFlush(Unpooled.wrappedBuffer(CHESS_MOVE_PAYLOAD));
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        double rttMs = (end - start) / 1_000_000.0;
        latenciesMs.add(rttMs);

        Thread.sleep(1);
      }

      quicChannel.close().sync();
      return new LatencyStats("WebTransport Stream (QUIC)", simulateJitter ? "1.5% Jitter" : "Clean LAN", latenciesMs);
    } finally {
      clientGroup.shutdownGracefully();
    }
  }

  // ==========================================
  // WebTransport Datagram Benchmark Execution
  // ==========================================

  private LatencyStats benchWebTransportDatagram(boolean simulateJitter) throws Exception {
    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    try {
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
          .bind(0).sync().channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);
      settings.put(0x2c7cf000L, 1L);

      CompletableFuture<Long>[] activeFuture = new CompletableFuture[]{new CompletableFuture<>()};
      Random rng = new Random(42);

      QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
          .handler(new ChannelInitializer<QuicChannel>() {
            @Override
            protected void initChannel(QuicChannel ch) {
              if (simulateJitter) {
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (rng.nextDouble() < 0.015) {
                      ctx.executor().schedule(() -> ctx.fireChannelRead(msg), 4, TimeUnit.MILLISECONDS);
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

              ch.pipeline().addLast(new Http3ClientConnectionHandler(null, null, new UnknownStreamHandlerFactory(),
                  new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true));
            }
          })
          .remoteAddress(new InetSocketAddress(HOST, wtPort))
          .connect()
          .get();

      // Handshake CONNECT stream
      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
      QuicStreamChannel connectStream = Http3.newRequestStream(
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
      headers.path("/chess");
      headers.authority(HOST + ":" + wtPort);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      boolean connected = handshakeLatch.await(5, TimeUnit.SECONDS);
      if (!connected) {
        throw new IllegalStateException("WebTransport datagram session handshake failed");
      }
      long sessionId = sessionIdHolder[0];

      // Warmup
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        activeFuture[0] = new CompletableFuture<>();
        ByteBuf dg = Unpooled.buffer();
        WebTransportUtils.writeVarInt(dg, sessionId / 4);
        dg.writeBytes(CHESS_MOVE_PAYLOAD);
        quicChannel.writeAndFlush(dg).sync();
        activeFuture[0].get(2, TimeUnit.SECONDS);
      }

      // Measurement
      List<Double> latenciesMs = new ArrayList<>(ITERATIONS);
      for (int i = 0; i < ITERATIONS; i++) {
        activeFuture[0] = new CompletableFuture<>();
        long start = System.nanoTime();
        ByteBuf dg = Unpooled.buffer();
        WebTransportUtils.writeVarInt(dg, sessionId / 4);
        dg.writeBytes(CHESS_MOVE_PAYLOAD);
        quicChannel.writeAndFlush(dg).sync();
        long end = activeFuture[0].get(2, TimeUnit.SECONDS);
        double rttMs = (end - start) / 1_000_000.0;
        latenciesMs.add(rttMs);

        Thread.sleep(1);
      }

      quicChannel.close().sync();
      return new LatencyStats("WebTransport Datagram (QUIC)", simulateJitter ? "1.5% Jitter" : "Clean LAN", latenciesMs);
    } finally {
      clientGroup.shutdownGracefully();
    }
  }

  // ==========================================
  // Connection Drop: WebSocket Reconnect Benchmark
  // ==========================================

  private LatencyStats benchWebSocketReconnect() throws Exception {
    EventLoopGroup clientGroup = new NioEventLoopGroup(1);
    try {
      URI uri = new URI("wss://" + HOST + ":" + wsPort + "/ws");
      List<Double> latenciesMs = new ArrayList<>(50);

      for (int i = 0; i < 50; i++) {
        long start = System.nanoTime();
        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
            uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());
        CountDownLatch handshakeLatch = new CountDownLatch(1);

        Bootstrap b = new Bootstrap();
        b.group(clientGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                p.addLast(wsClientSslContext.newHandler(ch.alloc(), HOST, wsPort));
                p.addLast(new HttpClientCodec());
                p.addLast(new HttpObjectAggregator(65536));
                p.addLast(new WebSocketClientProtocolHandler(handshaker));
                p.addLast(new SimpleChannelInboundHandler<Object>() {
                  @Override
                  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                    if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                      handshakeLatch.countDown();
                    }
                  }
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, Object msg) {}
                });
              }
            });

        Channel ch = b.connect(HOST, wsPort).sync().channel();
        handshakeLatch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();
        latenciesMs.add((end - start) / 1_000_000.0);
        ch.close().sync();
        Thread.sleep(5);
      }
      return new LatencyStats("Netty WebSocket Reconnect (WSS)", "Connection Drop", latenciesMs);
    } finally {
      clientGroup.shutdownGracefully();
    }
  }

  // ==========================================
  // Connection Drop: WebTransport Reconnect Benchmark
  // ==========================================

  private LatencyStats benchWebTransportReconnect() throws Exception {
    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    try {
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
          .bind(0).sync().channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);

      List<Double> latenciesMs = new ArrayList<>(50);
      for (int i = 0; i < 50; i++) {
        long start = System.nanoTime();
        QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
            .handler(new Http3ClientConnectionHandler(null, null, new UnknownStreamHandlerFactory(),
                new DefaultHttp3SettingsFrame(settings), false, (id, value) -> true))
            .remoteAddress(new InetSocketAddress(HOST, wtPort))
            .connect()
            .get();

        CountDownLatch handshakeLatch = new CountDownLatch(1);
        QuicStreamChannel connectStream = Http3.newRequestStream(
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
        headers.path("/chess");
        headers.authority(HOST + ":" + wtPort);
        headers.set(":protocol", "webtransport");
        connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
        handshakeLatch.await(5, TimeUnit.SECONDS);
        long end = System.nanoTime();
        latenciesMs.add((end - start) / 1_000_000.0);

        quicChannel.close().sync();
        Thread.sleep(5);
      }
      return new LatencyStats("WebTransport Reconnect", "Connection Drop", latenciesMs);
    } finally {
      clientGroup.shutdownGracefully();
    }
  }
}
