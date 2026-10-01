package io.github.webtransport4j.example;

import io.github.webtransport4j.client.WebTransportClientHandler;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3;
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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enterprise Real Traffic &amp; Chaos Generator for WebTransport4J.
 * Connects to live WebTransport4J instances over UDP/QUIC and drives genuine
 * network traffic, streams, datagrams, and chaos scenarios.
 */
public final class RealTrafficGenerator {

  private static final Logger log = LoggerFactory.getLogger(RealTrafficGenerator.class);

  private RealTrafficGenerator() {}

  /**
   * Session encapsulation holding active QUIC channel, connect stream, and session ID.
   */
  public static final class LiveSession implements AutoCloseable {
    private final QuicChannel quicChannel;
    private final QuicStreamChannel connectStream;
    private final long sessionId;
    private final EventLoopGroup group;

    LiveSession(
        final QuicChannel quicChannel,
        final QuicStreamChannel connectStream,
        final long sessionId,
        final EventLoopGroup group) {
      this.quicChannel = quicChannel;
      this.connectStream = connectStream;
      this.sessionId = sessionId;
      this.group = group;
    }

    public QuicChannel getQuicChannel() {
      return quicChannel;
    }

    public QuicStreamChannel getConnectStream() {
      return connectStream;
    }

    public long getSessionId() {
      return sessionId;
    }

    @Override
    public void close() {
      try {
        if (quicChannel != null && quicChannel.isOpen()) {
          quicChannel.close().sync();
        }
      } catch (final Exception e) {
        log.warn("Error closing QuicChannel", e);
      } finally {
        if (group != null) {
          group.shutdownGracefully();
        }
      }
    }
  }

  /**
   * Establishes a genuine WebTransport session with the specified target server.
   */
  public static LiveSession connect(final String rawUrl, final String traceparent)
      throws Exception {
    final URI uri = new URI(rawUrl);
    final String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
    final int port = uri.getPort() == -1 ? 4433 : uri.getPort();
    final String path = uri.getPath() == null || uri.getPath().isEmpty() ? "/echo" : uri.getPath();

    final EventLoopGroup group = new NioEventLoopGroup(1);
    final QuicSslContext sslCtx =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    final ChannelHandler codec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(sslCtx)
            .maxIdleTimeout(30000, TimeUnit.MILLISECONDS)
            .initialMaxData(1073741824)
            .initialMaxStreamDataBidirectionalLocal(107374182)
            .initialMaxStreamDataBidirectionalRemote(107374182)
            .initialMaxStreamsUnidirectional(1000)
            .initialMaxStreamDataUnidirectional(107374182)
            .initialMaxStreamsBidirectional(1000)
            .datagram(10000, 10000)
            .build();

    final Channel nettyChannel =
        new Bootstrap()
            .group(group)
            .channel(NioDatagramChannel.class)
            .handler(codec)
            .bind(0)
            .sync()
            .channel();

    final Http3Settings settings = new Http3Settings((id, value) -> true);
    settings.enableConnectProtocol(true);
    settings.enableH3Datagram(true);
    settings.put(0x2b64L, 1000L);
    settings.put(0x2b65L, 1000L);
    settings.put(0x2b61L, 10737418240L);

    final QuicChannel quicChannel =
        QuicChannel.newBootstrap(nettyChannel)
            .handler(new WebTransportClientHandler(new DefaultHttp3SettingsFrame(settings), true))
            .remoteAddress(new InetSocketAddress(host, port))
            .connect()
            .get();

    final CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStreamContainer = new QuicStreamChannel[1];

    final QuicStreamChannel connectStream =
        Http3.newRequestStream(
                quicChannel,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(final QuicStreamChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<Object>() {
                              @Override
                              protected void channelRead0(
                                  final ChannelHandlerContext ctx, final Object msg) {
                                if (msg instanceof Http3HeadersFrame) {
                                  final Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                  final String status =
                                      headersFrame.headers().status() != null
                                          ? headersFrame.headers().status().toString()
                                          : "";
                                  if ("200".equals(status)) {
                                    connectStreamContainer[0] = (QuicStreamChannel) ctx.channel();
                                    handshakeLatch.countDown();
                                  }
                                }
                              }
                            });
                  }
                })
            .sync()
            .getNow();

    final Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority(host + ":" + port);
    headers.path(path);
    headers.set(":protocol", "webtransport");
    if (traceparent != null && !traceparent.isEmpty()) {
      headers.set("traceparent", traceparent);
    }

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();

    if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
      group.shutdownGracefully();
      throw new IllegalStateException("Timeout establishing WebTransport handshake with " + rawUrl);
    }

    final long sessionId = connectStreamContainer[0].streamId();
    return new LiveSession(quicChannel, connectStreamContainer[0], sessionId, group);
  }

  /**
   * Transmits real WebTransport datagrams at high velocity over the QUIC channel.
   */
  public static int sendDatagrams(
      final LiveSession session,
      final int count,
      final int payloadSize,
      final int rateLimitPps)
      throws Exception {
    final byte[] payload = new byte[Math.max(16, payloadSize)];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) ((i & 0x7F) + 33);
    }

    int sentCount = 0;
    final long intervalNanos = rateLimitPps > 0 ? 1_000_000_000L / rateLimitPps : 0L;
    long lastSend = System.nanoTime();

    for (int i = 0; i < count; i++) {
      final ByteBuf dgBuf = session.getQuicChannel().alloc().directBuffer();
      WebTransportUtils.writeVarInt(dgBuf, session.getSessionId());
      dgBuf.writeBytes(payload);

      session.getQuicChannel().writeAndFlush(dgBuf);
      sentCount++;

      if (intervalNanos > 0) {
        final long elapsed = System.nanoTime() - lastSend;
        if (elapsed < intervalNanos) {
          final long sleepNanos = intervalNanos - elapsed;
          final long sleepMillis = sleepNanos / 1_000_000L;
          final int sleepRemNanos = (int) (sleepNanos % 1_000_000L);
          if (sleepMillis > 0 || sleepRemNanos > 0) {
            Thread.sleep(sleepMillis, sleepRemNanos);
          }
        }
        lastSend = System.nanoTime();
      }
    }
    return sentCount;
  }

  /**
   * Opens genuine streams, transmits data, and verifies response.
   */
  public static int sendStreams(
      final LiveSession session,
      final boolean bidi,
      final int count,
      final String payloadText)
      throws Exception {
    final byte[] payloadBytes = payloadText.getBytes(StandardCharsets.UTF_8);
    final AtomicInteger completed = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(count);

    for (int i = 0; i < count; i++) {
      final QuicStreamType streamType =
          bidi ? QuicStreamType.BIDIRECTIONAL : QuicStreamType.UNIDIRECTIONAL;
      final QuicStreamChannel stream =
          session
              .getQuicChannel()
              .createStream(
                  streamType,
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(final QuicStreamChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(
                                    final ChannelHandlerContext ctx, final Object msg) {
                                  if (msg instanceof ByteBuf) {
                                    ((ByteBuf) msg).release();
                                    completed.incrementAndGet();
                                    latch.countDown();
                                  }
                                }
                              });
                    }
                  })
              .sync()
              .getNow();

      final ByteBuf buffer = stream.alloc().directBuffer();
      final long frameType = bidi ? 0x41L : 0x54L;
      WebTransportUtils.writeVarInt(buffer, frameType);
      WebTransportUtils.writeVarInt(buffer, session.getSessionId());
      buffer.writeBytes(payloadBytes);

      stream.writeAndFlush(buffer).sync();
      if (!bidi) {
        stream.shutdownOutput().sync();
        completed.incrementAndGet();
        latch.countDown();
      }
    }

    if (bidi) {
      latch.await(5, TimeUnit.SECONDS);
    }
    return completed.get();
  }

  /**
   * CLI entry point for executing real traffic scenarios and outputting structured JSON results.
   */
  public static void main(final String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: RealTrafficGenerator <command> <url> [options...]");
      System.err.println("Commands: handshake, datagrams, streams, chaos-burst, chaos-abrupt-close");
      System.exit(1);
    }

    final String command = args[0];
    final String url = args[1];
    final long startTime = System.currentTimeMillis();

    try {
      if ("handshake".equalsIgnoreCase(command)) {
        final String traceparent = args.length > 2 ? args[2] : "";
        try (LiveSession session = connect(url, traceparent)) {
          final long rttMs = System.currentTimeMillis() - startTime;
          System.out.printf(
              "{\"status\":\"SUCCESS\",\"command\":\"handshake\",\"sessionId\":%d,\"rttMs\":%d,\"target\":\"%s\"}%n",
              session.getSessionId(), rttMs, url);
        }
      } else if ("datagrams".equalsIgnoreCase(command)) {
        final int count = args.length > 2 ? Integer.parseInt(args[2]) : 100;
        final int size = args.length > 3 ? Integer.parseInt(args[3]) : 256;
        final int pps = args.length > 4 ? Integer.parseInt(args[4]) : 1000;
        try (LiveSession session = connect(url, null)) {
          final int sent = sendDatagrams(session, count, size, pps);
          final long durationMs = System.currentTimeMillis() - startTime;
          System.out.printf(
              "{\"status\":\"SUCCESS\",\"command\":\"datagrams\",\"sent\":%d,\"size\":%d,"
                  + "\"durationMs\":%d,\"target\":\"%s\"}%n",
              sent, size, durationMs, url);
        }
      } else if ("streams".equalsIgnoreCase(command)) {
        final boolean bidi = args.length > 2 && "bidi".equalsIgnoreCase(args[2]);
        final int count = args.length > 3 ? Integer.parseInt(args[3]) : 10;
        final String payload = args.length > 4 ? args[4] : "Real WebTransport Stream Payload";
        try (LiveSession session = connect(url, null)) {
          final int completed = sendStreams(session, bidi, count, payload);
          final long durationMs = System.currentTimeMillis() - startTime;
          System.out.printf(
              "{\"status\":\"SUCCESS\",\"command\":\"streams\",\"type\":\"%s\",\"count\":%d,"
                  + "\"durationMs\":%d,\"target\":\"%s\"}%n",
              bidi ? "bidi" : "uni", completed, durationMs, url);
        }
      } else if ("chaos-burst".equalsIgnoreCase(command)) {
        final int burstCount = args.length > 2 ? Integer.parseInt(args[2]) : 500;
        try (LiveSession session = connect(url, null)) {
          final int sent = sendDatagrams(session, burstCount, 1200, 0);
          final long durationMs = System.currentTimeMillis() - startTime;
          System.out.printf(
              "{\"status\":\"SUCCESS\",\"command\":\"chaos-burst\",\"sent\":%d,"
                  + "\"durationMs\":%d,\"target\":\"%s\"}%n",
              sent, durationMs, url);
        }
      } else if ("chaos-abrupt-close".equalsIgnoreCase(command)) {
        final LiveSession session = connect(url, null);
        session.getQuicChannel().unsafe().closeForcibly();
        System.out.printf(
            "{\"status\":\"SUCCESS\",\"command\":\"chaos-abrupt-close\","
                + "\"action\":\"FORCIBLE_DISCONNECT\",\"target\":\"%s\"}%n",
            url);
      } else {
        System.err.println("Unknown command: " + command);
        System.exit(1);
      }
    } catch (final Exception ex) {
      log.error("Traffic generation execution failed", ex);
      System.out.printf(
          "{\"status\":\"ERROR\",\"command\":\"%s\",\"error\":\"%s\",\"target\":\"%s\"}%n",
          command, ex.getMessage().replace("\"", "'"), url);
      System.exit(1);
    }
  }
}
