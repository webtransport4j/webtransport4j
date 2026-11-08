package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.api.WebTransportStreamResetException;
import io.github.webtransport4j.client.WebTransportClientHandler;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ControlStreamFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicSslSessionContext;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.codec.quic.SslSessionTicketKey;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unmocked end-to-end QUIC + HTTP/3 integration tests for {@code draft-ietf-webtrans-http3-17}
 * protocol compliance over real UDP sockets ({@link NioDatagramChannel}) and TLS 1.3.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class Draft17ProtocolIntegrationTest {

  @Test
  public void testEndToEnd_StrictDraft17NegotiationIgnoresClientWtEnabledAndEchoesBidiStream()
      throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(buildServerSslContext(cert, false, null))
            .defaultHandler(
                new WebTransportHandler() {
                  @Override
                  public void onIncomingStream(
                      @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
                    stream.onData(
                        buf -> {
                          String text = new String(buf.readBytes(), StandardCharsets.UTF_8);
                          stream.writeText("pong:" + text);
                        });
                  }
                })
            .build();

    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel udp = null;
    QuicChannel quic = null;
    try {
      server.start();
      int port = server.getPort();
      QuicSslContext clientSsl = buildClientSslContext(cert, false);
      udp = bindClientUdp(clientGroup, clientSsl, port);

      // Client sends SETTINGS_WT_ENABLED = 99 (> 1) to verify server strictly ignores it (§3.1)
      Http3Settings clientSettings = new Http3Settings((id, value) -> true);
      clientSettings.enableConnectProtocol(true);
      clientSettings.enableH3Datagram(true);
      clientSettings.put(WebTransportProtocolSpec.SETTING_WT_ENABLED, 99L);
      clientSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_DATA, 65536L);
      clientSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI, 16L);
      clientSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_UNI, 16L);

      quic =
          QuicChannel.newBootstrap(udp)
              .handler(
                  new WebTransportClientHandler(
                      new DefaultHttp3SettingsFrame(clientSettings), true))
              .remoteAddress(new InetSocketAddress("127.0.0.1", port))
              .connect()
              .get(5, TimeUnit.SECONDS);

      final QuicStreamChannel connectStream =
          openExtendedConnect(quic, port, "/draft17", "200");

      waitForPeerSettings(quic, 5, TimeUnit.SECONDS);
      assertTrue(
          Boolean.TRUE.equals(
              quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).get()));
      assertTrue(Boolean.TRUE.equals(quic.attr(WebTransportAttributeKeys.WT_ENABLED).get()));
      assertTrue(
          Boolean.TRUE.equals(quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).get()));

      CompletableFuture<String> echoFuture = new CompletableFuture<>();
      QuicStreamChannel bidi =
          quic.createStream(
                  QuicStreamType.BIDIRECTIONAL,
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(
                                    ChannelHandlerContext ctx, ByteBuf msg) {
                                  echoFuture.complete(msg.toString(StandardCharsets.UTF_8));
                                }
                              });
                    }
                  })
              .get(5, TimeUnit.SECONDS);

      ByteBuf frame = bidi.alloc().buffer();
      WebTransportUtils.writeVarInt(frame, WebTransportUtils.BI_STREAM_TYPE);
      WebTransportUtils.writeVarInt(frame, connectStream.streamId());
      frame.writeBytes("hello-draft17".getBytes(StandardCharsets.UTF_8));
      bidi.writeAndFlush(frame).get(5, TimeUnit.SECONDS);

      assertEquals("pong:hello-draft17", echoFuture.get(5, TimeUnit.SECONDS));
    } finally {
      closeQuietly(quic, udp, clientGroup, server);
    }
  }

  @Test
  public void testEndToEnd_ClientOmittingSettingsWtEnabledEstablishesSessionAndDatagram()
      throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    CompletableFuture<String> datagramReceived = new CompletableFuture<>();
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(buildServerSslContext(cert, false, null))
            .defaultHandler(
                new WebTransportHandler() {
                  @Override
                  public void onDatagramReceived(
                      @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
                    datagramReceived.complete(new String(data.readBytes(), StandardCharsets.UTF_8));
                  }
                })
            .build();

    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel udp = null;
    QuicChannel quic = null;
    try {
      server.start();
      int port = server.getPort();
      QuicSslContext clientSsl = buildClientSslContext(cert, false);
      udp = bindClientUdp(clientGroup, clientSsl, port);

      // Client omits SETTINGS_WT_ENABLED completely per Draft-17 §3.1
      Http3Settings clientSettings = new Http3Settings((id, value) -> true);
      clientSettings.enableConnectProtocol(true);
      clientSettings.enableH3Datagram(true);

      quic =
          QuicChannel.newBootstrap(udp)
              .handler(
                  new WebTransportClientHandler(
                      new DefaultHttp3SettingsFrame(clientSettings), true))
              .remoteAddress(new InetSocketAddress("127.0.0.1", port))
              .connect()
              .get(5, TimeUnit.SECONDS);

      QuicStreamChannel connect = openExtendedConnect(quic, port, "/datagram", "200");
      ByteBuf dgram = quic.alloc().buffer();
      WebTransportUtils.writeVarInt(dgram, connect.streamId() / 4L);
      dgram.writeBytes("dgram-payload".getBytes(StandardCharsets.UTF_8));
      quic.writeAndFlush(dgram).get(5, TimeUnit.SECONDS);

      assertEquals("dgram-payload", datagramReceived.get(5, TimeUnit.SECONDS));
    } finally {
      closeQuietly(quic, udp, clientGroup, server);
    }
  }

  @Test
  public void testEndToEnd_InvalidHttpsRequestTargetPathRejectedWith400() throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(buildServerSslContext(cert, false, null))
            .defaultHandler(new WebTransportHandler() {})
            .build();

    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel udp = null;
    QuicChannel quic = null;
    try {
      server.start();
      int port = server.getPort();
      QuicSslContext clientSsl = buildClientSslContext(cert, false);
      udp = bindClientUdp(clientGroup, clientSsl, port);

      Http3Settings clientSettings = new Http3Settings((id, value) -> true);
      clientSettings.enableConnectProtocol(true);
      clientSettings.enableH3Datagram(true);

      quic =
          QuicChannel.newBootstrap(udp)
              .handler(
                  new WebTransportClientHandler(
                      new DefaultHttp3SettingsFrame(clientSettings), true))
              .remoteAddress(new InetSocketAddress("127.0.0.1", port))
              .connect()
              .get(5, TimeUnit.SECONDS);

      // Invalid HTTPS URI request target: path does not start with '/' (Draft-17 §2.2 / RFC 9110)
      openExtendedConnect(quic, port, "relative-no-leading-slash", "400");
    } finally {
      closeQuietly(quic, udp, clientGroup, server);
    }
  }

  @Test
  public void testEndToEnd_ClientClosesWithSettingsErrorWhenServerSendsWtEnabledGreaterThanOne()
      throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    EventLoopGroup serverGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel serverUdp = null;
    Channel clientUdp = null;
    QuicChannel quic = null;

    CompletableFuture<QuicConnectionCloseEvent> serverObservedClose = new CompletableFuture<>();
    try {
      final QuicSslContext serverSsl = buildServerSslContext(cert, false, null);
      Http3Settings invalidServerSettings = new Http3Settings((id, value) -> true);
      invalidServerSettings.enableConnectProtocol(true);
      invalidServerSettings.enableH3Datagram(true);
      invalidServerSettings.put(WebTransportProtocolSpec.SETTING_WT_ENABLED, 2L);

      serverUdp =
          bindServerTrackingClose(
              serverGroup, serverSsl, invalidServerSettings, serverObservedClose);
      int port = ((InetSocketAddress) serverUdp.localAddress()).getPort();

      QuicSslContext clientSsl = buildClientSslContext(cert, false);
      clientUdp = bindClientUdp(clientGroup, clientSsl, port);

      Http3Settings clientSettings = new Http3Settings((id, value) -> true);
      clientSettings.enableConnectProtocol(true);
      clientSettings.enableH3Datagram(true);

      quic =
          QuicChannel.newBootstrap(clientUdp)
              .handler(
                  new WebTransportClientHandler(
                      new DefaultHttp3SettingsFrame(clientSettings), true))
              .remoteAddress(new InetSocketAddress("127.0.0.1", port))
              .connect()
              .get(5, TimeUnit.SECONDS);

      QuicConnectionCloseEvent closeEvent = serverObservedClose.get(5, TimeUnit.SECONDS);
      assertTrue(closeEvent.isApplicationClose());
      assertEquals(
          WebTransportProtocolSpec.H3_SETTINGS_ERROR,
          closeEvent.error(),
          "Client MUST close connection with H3_SETTINGS_ERROR (0x0109) when server sends > 1");
    } finally {
      closeQuietly(quic, clientUdp, clientGroup, null);
      if (serverUdp != null) {
        serverUdp.close().awaitUninterruptibly(5000);
      }
      serverGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly(5000);
    }
  }

  @Test
  public void testEndToEnd_ZeroRttResumptionAppliesReducedWebTransportSettingsWithoutError()
      throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    SslSessionTicketKey sharedTicketKey =
        new SslSessionTicketKey(
            "0123456789abcdef".getBytes(StandardCharsets.US_ASCII),
            "0123456789abcdef".getBytes(StandardCharsets.US_ASCII),
            "0123456789abcdef".getBytes(StandardCharsets.US_ASCII));

    EventLoopGroup serverGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel server1Udp = null;
    Channel server2Udp = null;
    Channel client1Udp = null;
    Channel client2Udp = null;
    QuicChannel quic1 = null;
    QuicChannel quic2 = null;

    try {
      QuicSslContext serverSsl = buildServerSslContext(cert, true, sharedTicketKey);
      QuicSslContext sharedClientSsl = buildClientSslContext(cert, true);

      // Server 1 advertises high initial WebTransport limits
      server1Udp =
          bindMinimalSettingsServer(
              serverGroup, serverSsl, new ServerLimits(500_000L, 64L, 64L));
      int port1 = ((InetSocketAddress) server1Udp.localAddress()).getPort();
      client1Udp = bindClientUdp(clientGroup, sharedClientSsl, port1);

      quic1 = verifyInitialSettingsServer1(client1Udp, port1);

      // Allow NewSessionTicket post-handshake message to arrive before closing connection 1
      Thread.sleep(100);
      quic1.close().get(5, TimeUnit.SECONDS);
      quic1 = null;

      // Server 2 (sharing the same TLS ticket key) advertises REDUCED WebTransport limits (§3.2)
      server2Udp =
          bindMinimalSettingsServer(
              serverGroup, serverSsl, new ServerLimits(4_096L, 2L, 1L));
      int port2 = ((InetSocketAddress) server2Udp.localAddress()).getPort();
      client2Udp = bindClientUdp(clientGroup, sharedClientSsl, port2);

      quic2 = verifyResumedSettingsServer2(client2Udp, port2);
    } finally {
      closeQuietly(quic1, client1Udp, null, null);
      closeQuietly(quic2, client2Udp, clientGroup, null);
      if (server1Udp != null) {
        server1Udp.close().awaitUninterruptibly(5000);
      }
      if (server2Udp != null) {
        server2Udp.close().awaitUninterruptibly(5000);
      }
      serverGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly(5000);
    }
  }

  @Test
  public void testEndToEnd_UnexpectedWtStreamFrameOnRequestAndControlStreamClosesWith0x0105()
      throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(buildServerSslContext(cert, false, null))
            .defaultHandler(new WebTransportHandler() {})
            .build();

    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel udp1 = null;
    Channel udp2 = null;
    QuicChannel quic1 = null;
    QuicChannel quic2 = null;
    try {
      server.start();
      int port = server.getPort();
      QuicSslContext clientSsl = buildClientSslContext(cert, false);

      // Subcase A: WT_STREAM (0x41) sent after HEADERS on an HTTP/3 request stream
      udp1 = bindClientUdp(clientGroup, clientSsl, port);
      CompletableFuture<QuicConnectionCloseEvent> closeOnRequestStream = new CompletableFuture<>();
      quic1 = connectCapturingCloseEvent(udp1, port, closeOnRequestStream);

      QuicStreamChannel rawRequestStream =
          quic1
              .createStream(
                  QuicStreamType.BIDIRECTIONAL,
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(
                                    ChannelHandlerContext ctx, ByteBuf msg) {}
                              });
                    }
                  })
              .get(5, TimeUnit.SECONDS);

      // Valid HTTP/3 HEADERS frame (0x01, len=2, empty QPACK prefix 0x00 0x00) + varint 0x41
      ByteBuf illegalRequestBytes = rawRequestStream.alloc().buffer(6);
      illegalRequestBytes.writeBytes(new byte[] {0x01, 0x02, 0x00, 0x00});
      WebTransportUtils.writeVarInt(illegalRequestBytes, WebTransportUtils.BI_STREAM_TYPE);
      rawRequestStream.writeAndFlush(illegalRequestBytes).get(5, TimeUnit.SECONDS);

      QuicConnectionCloseEvent close1 = closeOnRequestStream.get(5, TimeUnit.SECONDS);
      assertTrue(close1.isApplicationClose());
      assertEquals(WebTransportProtocolSpec.H3_FRAME_UNEXPECTED, close1.error());

      // Subcase B: WT_STREAM (varint 0x41) sent on the HTTP/3 unidirectional control stream
      udp2 = bindClientUdp(clientGroup, clientSsl, port);
      CompletableFuture<QuicConnectionCloseEvent> closeOnControlStream = new CompletableFuture<>();
      quic2 = connectCapturingCloseEvent(udp2, port, closeOnControlStream);

      QuicStreamChannel localControl = awaitLocalControlStream(quic2);
      assertNotNull(localControl);
      ByteBuf illegalControlByte = localControl.alloc().buffer(2);
      WebTransportUtils.writeVarInt(illegalControlByte, WebTransportUtils.BI_STREAM_TYPE);
      localControl.pipeline().firstContext().writeAndFlush(illegalControlByte).sync();

      QuicConnectionCloseEvent close2 = closeOnControlStream.get(5, TimeUnit.SECONDS);
      assertTrue(close2.isApplicationClose());
      assertEquals(WebTransportProtocolSpec.H3_FRAME_UNEXPECTED, close2.error());
    } finally {
      closeQuietly(quic1, udp1, null, null);
      closeQuietly(quic2, udp2, clientGroup, server);
    }
  }

  @Test
  public void testEndToEnd_InboundNonApplicationStreamResetAndNonIncreasingCapsule()
      throws Exception {
    SelfSignedCertificate cert = new SelfSignedCertificate("localhost");
    CompletableFuture<Throwable> streamErrorFuture = new CompletableFuture<>();
    CountDownLatch streamDataLatch = new CountDownLatch(1);
    CountDownLatch sessionClosedLatch = new CountDownLatch(1);

    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(buildServerSslContext(cert, false, null))
            .defaultHandler(
                new WebTransportHandler() {
                  @Override
                  public void onIncomingStream(
                      @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
                    stream.onError(streamErrorFuture::complete);
                    stream.onData(buf -> streamDataLatch.countDown());
                  }

                  @Override
                  public void onSessionClosed(@NonNull WebTransportSession session) {
                    sessionClosedLatch.countDown();
                  }
                })
            .build();

    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel udp = null;
    QuicChannel quic = null;
    try {
      server.start();
      int port = server.getPort();
      QuicSslContext clientSsl = buildClientSslContext(cert, false);
      udp = bindClientUdp(clientGroup, clientSsl, port);

      Http3Settings clientSettings = new Http3Settings((id, value) -> true);
      clientSettings.enableConnectProtocol(true);
      clientSettings.enableH3Datagram(true);
      clientSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_DATA, 5000L);
      clientSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI, 10L);
      clientSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_UNI, 10L);

      quic =
          QuicChannel.newBootstrap(udp)
              .handler(
                  new WebTransportClientHandler(
                      new DefaultHttp3SettingsFrame(clientSettings), true))
              .remoteAddress(new InetSocketAddress("127.0.0.1", port))
              .connect()
              .get(5, TimeUnit.SECONDS);

      CompletableFuture<QuicStreamResetException> connectResetFuture = new CompletableFuture<>();
      QuicStreamChannel connect =
          openExtendedConnectCapturingReset(quic, port, "/reset-and-fc", connectResetFuture);

      verifyNonApplicationStreamReset(quic, connect, streamDataLatch, streamErrorFuture);
      verifyNonIncreasingMaxDataCapsule(connect, sessionClosedLatch, connectResetFuture);
    } finally {
      closeQuietly(quic, udp, clientGroup, server);
    }
  }

  private static QuicSslContext buildServerSslContext(
      SelfSignedCertificate cert, boolean earlyData, SslSessionTicketKey ticketKey) {
    QuicSslContext ctx =
        QuicSslContextBuilder.forServer(cert.privateKey(), null, cert.certificate())
            .earlyData(earlyData)
            .sessionCacheSize(20480)
            .sessionTimeout(86400)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();
    if (ticketKey != null && ctx.sessionContext() instanceof QuicSslSessionContext) {
      ((QuicSslSessionContext) ctx.sessionContext())
          .setTicketKeys(new SslSessionTicketKey[] {ticketKey});
    }
    return ctx;
  }

  private static QuicSslContext buildClientSslContext(
      SelfSignedCertificate cert, boolean earlyData) {
    return QuicSslContextBuilder.forClient()
        .trustManager(cert.certificate())
        .earlyData(earlyData)
        .sessionCacheSize(20480)
        .sessionTimeout(86400)
        .applicationProtocols(Http3.supportedApplicationProtocols())
        .build();
  }

  private static Channel bindClientUdp(EventLoopGroup group, QuicSslContext ssl, int port)
      throws InterruptedException {
    return new Bootstrap()
        .group(group)
        .channel(NioDatagramChannel.class)
        .handler(
            Http3.newQuicClientCodecBuilder()
                .sslContext(ssl)
                .sslEngineProvider(ch -> ssl.newEngine(ch.alloc(), "localhost", port))
                .maxIdleTimeout(10, TimeUnit.SECONDS)
                .datagram(1024, 1024)
                .initialMaxData(1_000_000)
                .initialMaxStreamDataBidirectionalLocal(100_000)
                .initialMaxStreamDataBidirectionalRemote(100_000)
                .initialMaxStreamDataUnidirectional(100_000)
                .initialMaxStreamsBidirectional(64)
                .initialMaxStreamsUnidirectional(64)
                .build())
        .bind(0)
        .sync()
        .channel();
  }

  private static final class ServerLimits {
    final long maxData;
    final long maxBidi;
    final long maxUni;

    ServerLimits(long maxData, long maxBidi, long maxUni) {
      this.maxData = maxData;
      this.maxBidi = maxBidi;
      this.maxUni = maxUni;
    }
  }

  private static Channel bindMinimalSettingsServer(
      EventLoopGroup serverGroup,
      QuicSslContext serverSsl,
      ServerLimits limits)
      throws InterruptedException {
    Http3Settings settings = new Http3Settings((id, value) -> true);
    settings.enableConnectProtocol(true);
    settings.enableH3Datagram(true);
    settings.put(WebTransportProtocolSpec.SETTING_WT_ENABLED, 1L);
    settings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_DATA, limits.maxData);
    settings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI, limits.maxBidi);
    settings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_UNI, limits.maxUni);

    ChannelHandler codec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(serverSsl)
            .maxIdleTimeout(10, TimeUnit.SECONDS)
            .initialMaxData(1_000_000)
            .initialMaxStreamDataBidirectionalLocal(100_000)
            .initialMaxStreamDataBidirectionalRemote(100_000)
            .initialMaxStreamDataUnidirectional(100_000)
            .initialMaxStreamsBidirectional(64)
            .initialMaxStreamsUnidirectional(64)
            .datagram(1024, 1024)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                new SimpleChannelInboundHandler<Http3ControlStreamFrame>() {
                                  @Override
                                  protected void channelRead0(
                                      ChannelHandlerContext ctx, Http3ControlStreamFrame msg) {}
                                },
                                new UnknownStreamHandlerFactory(),
                                new DefaultHttp3SettingsFrame(settings),
                                true,
                                (id, value) -> true));
                  }
                })
            .build();

    return new Bootstrap()
        .group(serverGroup)
        .channel(NioDatagramChannel.class)
        .handler(codec)
        .bind(0)
        .sync()
        .channel();
  }

  private static QuicChannel connectObservingSettings(
      Channel clientUdp, int port, CountDownLatch settingsLatch, boolean seedStale0RttAttributes)
      throws Exception {
    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.enableH3Datagram(true);

    AtomicReference<QuicChannel> quicRef = new AtomicReference<>();
    QuicChannel quic =
        QuicChannel.newBootstrap(clientUdp)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    quicRef.set(ch);
                    if (seedStale0RttAttributes) {
                      ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(999_999L);
                      ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(99L);
                      ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(99L);
                    }
                    ch.pipeline()
                        .addLast(
                            new WebTransportClientHandler(
                                new DefaultHttp3SettingsFrame(clientSettings), true) {
                              @Override
                              protected void initUnidirectionalStream(
                                  ChannelHandlerContext ctx, QuicStreamChannel streamChannel) {
                                super.initUnidirectionalStream(ctx, streamChannel);
                                streamChannel
                                    .pipeline()
                                    .addLast(
                                        new ChannelInboundHandlerAdapter() {
                                          @Override
                                          public void channelRead(
                                              ChannelHandlerContext sctx, Object msg)
                                              throws Exception {
                                            boolean isSettings = msg instanceof Http3SettingsFrame;
                                            super.channelRead(sctx, msg);
                                            if (isSettings) {
                                              settingsLatch.countDown();
                                            }
                                          }
                                        });
                              }
                            });
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port))
            .connect()
            .get(5, TimeUnit.SECONDS);

    // Poll PEER_SETTINGS_RECEIVED attribute as well in case control stream handler consumed frame
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (Boolean.TRUE.equals(
          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).get())) {
        settingsLatch.countDown();
        break;
      }
      Thread.sleep(10);
    }
    return quic;
  }

  private static QuicChannel connectCapturingCloseEvent(
      Channel clientUdp,
      int port,
      CompletableFuture<QuicConnectionCloseEvent> closeFuture)
      throws Exception {
    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.enableH3Datagram(true);

    return QuicChannel.newBootstrap(clientUdp)
        .handler(
            new ChannelInitializer<QuicChannel>() {
              @Override
              protected void initChannel(QuicChannel ch) {
                ch.pipeline()
                    .addLast(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
                              throws Exception {
                            if (evt instanceof QuicConnectionCloseEvent) {
                              closeFuture.complete((QuicConnectionCloseEvent) evt);
                            }
                            super.userEventTriggered(ctx, evt);
                          }
                        });
                ch.pipeline()
                    .addLast(
                        new WebTransportClientHandler(
                            new DefaultHttp3SettingsFrame(clientSettings), true));
              }
            })
        .remoteAddress(new InetSocketAddress("127.0.0.1", port))
        .connect()
        .get(5, TimeUnit.SECONDS);
  }

  private static QuicStreamChannel awaitLocalControlStream(QuicChannel quic)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      QuicStreamChannel control = Http3.getLocalControlStream(quic);
      if (control != null) {
        return control;
      }
      Thread.sleep(10);
    }
    return Http3.getLocalControlStream(quic);
  }

  private static final class ConnectTarget {
    final int port;
    final String path;
    final String expectedStatus;

    ConnectTarget(int port, String path, String expectedStatus) {
      this.port = port;
      this.path = path;
      this.expectedStatus = expectedStatus;
    }
  }

  private static QuicStreamChannel openExtendedConnect(
      QuicChannel quic, int port, String path, String expectedStatus) throws Exception {
    return openExtendedConnectTarget(quic, new ConnectTarget(port, path, expectedStatus), null);
  }

  private static QuicStreamChannel openExtendedConnectCapturingReset(
      QuicChannel quic,
      int port,
      String path,
      CompletableFuture<QuicStreamResetException> resetFuture)
      throws Exception {
    return openExtendedConnectTarget(quic, new ConnectTarget(port, path, "200"), resetFuture);
  }

  private static QuicStreamChannel openExtendedConnectTarget(
      QuicChannel quic,
      ConnectTarget target,
      CompletableFuture<QuicStreamResetException> resetFuture)
      throws Exception {
    CompletableFuture<Http3HeadersFrame> responseFuture = new CompletableFuture<>();
    QuicStreamChannel connect =
        Http3.newRequestStream(
                quic,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<Object>() {
                              @Override
                              protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                if (msg instanceof Http3HeadersFrame) {
                                  responseFuture.complete((Http3HeadersFrame) msg);
                                }
                              }

                              @Override
                              public void exceptionCaught(
                                  ChannelHandlerContext ctx, Throwable cause) {
                                if (resetFuture != null
                                    && cause instanceof QuicStreamResetException) {
                                  resetFuture.complete((QuicStreamResetException) cause);
                                }
                                ctx.close();
                              }
                            });
                  }
                })
            .get(5, TimeUnit.SECONDS);

    Http3Headers headers =
        new DefaultHttp3Headers()
            .method("CONNECT")
            .scheme("https")
            .authority("localhost:" + target.port)
            .path(target.path)
            .set(":protocol", WebTransportHeadersHandler.UPGRADE_TOKEN_H3);
    connect.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).get(5, TimeUnit.SECONDS);

    Http3HeadersFrame resp = responseFuture.get(5, TimeUnit.SECONDS);
    assertEquals(target.expectedStatus, resp.headers().status().toString());
    return connect;
  }

  private static Channel bindServerTrackingClose(
      EventLoopGroup serverGroup,
      QuicSslContext serverSsl,
      Http3Settings settings,
      CompletableFuture<QuicConnectionCloseEvent> closeFuture)
      throws InterruptedException {
    ChannelHandler serverCodec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(serverSsl)
            .maxIdleTimeout(10, TimeUnit.SECONDS)
            .initialMaxData(1_000_000)
            .initialMaxStreamDataBidirectionalLocal(100_000)
            .initialMaxStreamDataBidirectionalRemote(100_000)
            .initialMaxStreamDataUnidirectional(100_000)
            .initialMaxStreamsBidirectional(32)
            .initialMaxStreamsUnidirectional(32)
            .datagram(1024, 1024)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new ChannelInboundHandlerAdapter() {
                              @Override
                              public void userEventTriggered(
                                  ChannelHandlerContext ctx, Object evt) throws Exception {
                                if (evt instanceof QuicConnectionCloseEvent) {
                                  closeFuture.complete((QuicConnectionCloseEvent) evt);
                                }
                                super.userEventTriggered(ctx, evt);
                              }
                            });
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                new SimpleChannelInboundHandler<Http3ControlStreamFrame>() {
                                  @Override
                                  protected void channelRead0(
                                      ChannelHandlerContext ctx, Http3ControlStreamFrame msg) {}
                                },
                                new UnknownStreamHandlerFactory(),
                                new DefaultHttp3SettingsFrame(settings),
                                true,
                                (id, value) -> true));
                  }
                })
            .build();

    return new Bootstrap()
        .group(serverGroup)
        .channel(NioDatagramChannel.class)
        .handler(serverCodec)
        .bind(0)
        .sync()
        .channel();
  }

  private static QuicChannel verifyInitialSettingsServer1(Channel client1Udp, int port1)
      throws Exception {
    CountDownLatch settings1Latch = new CountDownLatch(1);
    QuicChannel quic1 = connectObservingSettings(client1Udp, port1, settings1Latch, false);
    assertTrue(settings1Latch.await(5, TimeUnit.SECONDS));
    assertEquals(
        Long.valueOf(500_000L),
        quic1.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).get());
    assertEquals(
        Long.valueOf(64L),
        quic1.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get());
    return quic1;
  }

  private static QuicChannel verifyResumedSettingsServer2(Channel client2Udp, int port2)
      throws Exception {
    CountDownLatch settings2Latch = new CountDownLatch(1);
    QuicChannel quic2 = connectObservingSettings(client2Udp, port2, settings2Latch, true);
    assertTrue(settings2Latch.await(5, TimeUnit.SECONDS));
    assertTrue(quic2.isActive(), "Resumed connection with reduced WT settings MUST NOT error");
    assertEquals(
        Long.valueOf(4_096L),
        quic2.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).get());
    assertEquals(
        Long.valueOf(2L),
        quic2.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get());
    assertEquals(
        Long.valueOf(1L),
        quic2.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).get());
    return quic2;
  }

  private static void verifyNonApplicationStreamReset(
      QuicChannel quic,
      QuicStreamChannel connect,
      CountDownLatch streamDataLatch,
      CompletableFuture<Throwable> streamErrorFuture)
      throws Exception {
    QuicStreamChannel bidi =
        quic.createStream(
                QuicStreamType.BIDIRECTIONAL,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel ch) {}
                })
            .get(5, TimeUnit.SECONDS);
    ByteBuf wtHeaderAndPayload = bidi.alloc().buffer();
    WebTransportUtils.writeVarInt(wtHeaderAndPayload, WebTransportUtils.BI_STREAM_TYPE);
    WebTransportUtils.writeVarInt(wtHeaderAndPayload, connect.streamId());
    wtHeaderAndPayload.writeBytes("init".getBytes(StandardCharsets.UTF_8));
    bidi.writeAndFlush(wtHeaderAndPayload).get(5, TimeUnit.SECONDS);
    assertTrue(streamDataLatch.await(5, TimeUnit.SECONDS));

    bidi.shutdown(0x010c).get(5, TimeUnit.SECONDS);

    Throwable streamErr = streamErrorFuture.get(5, TimeUnit.SECONDS);
    WebTransportStreamResetException resetEx =
        assertInstanceOf(WebTransportStreamResetException.class, streamErr);
    assertFalse(
        resetEx.hasApplicationErrorCode(),
        "Non-application HTTP/3 reset code 0x010c MUST NOT fabricate an application error code");
    assertFalse(resetEx.applicationErrorCode().isPresent());
    assertEquals(0x010cL, resetEx.rawHttp3ErrorCode());
  }

  private static void verifyNonIncreasingMaxDataCapsule(
      QuicStreamChannel connect,
      CountDownLatch sessionClosedLatch,
      CompletableFuture<QuicStreamResetException> connectResetFuture)
      throws Exception {
    ByteBuf maxDataValueBuf = Unpooled.buffer(8);
    ByteBuf capsuleBuf = connect.alloc().buffer();
    try {
      WebTransportUtils.writeVarInt(maxDataValueBuf, 5000L);
      WebTransportUtils.writeVarInt(
          capsuleBuf, WebTransportProtocolSpec.CAPSULE_WT_MAX_DATA);
      WebTransportUtils.writeVarInt(capsuleBuf, maxDataValueBuf.readableBytes());
      capsuleBuf.writeBytes(maxDataValueBuf);
    } finally {
      maxDataValueBuf.release();
    }
    connect.writeAndFlush(new DefaultHttp3DataFrame(capsuleBuf)).get(5, TimeUnit.SECONDS);

    assertTrue(
        sessionClosedLatch.await(5, TimeUnit.SECONDS),
        "Server MUST close session when WT_MAX_DATA does not strictly increase initial limit");
    QuicStreamResetException connectReset = connectResetFuture.get(5, TimeUnit.SECONDS);
    assertEquals(
        (long) WebTransportUtils.WT_FLOW_CONTROL_ERROR, connectReset.applicationProtocolCode());
  }

  private static void waitForPeerSettings(QuicChannel quic, long timeout, TimeUnit unit)
      throws InterruptedException {
    long deadline = System.nanoTime() + unit.toNanos(timeout);
    while (System.nanoTime() < deadline) {
      if (Boolean.TRUE.equals(
          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).get())) {
        break;
      }
      Thread.sleep(10);
    }
  }

  private static void closeQuietly(
      QuicChannel quic, Channel udp, EventLoopGroup group, WebTransportServer server) {
    if (quic != null) {
      quic.close().awaitUninterruptibly(5000);
    }
    if (udp != null) {
      udp.close().awaitUninterruptibly(5000);
    }
    if (group != null) {
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly(5000);
    }
    if (server != null) {
      try {
        server.stop();
      } catch (Exception ignored) {
        // Best-effort cleanup
      }
    }
  }
}
