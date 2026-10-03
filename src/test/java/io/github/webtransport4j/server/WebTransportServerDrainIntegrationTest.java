package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.bootstrap.Bootstrap;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unmocked integration test verifying graceful zero-downtime server shutdown and channel drain
 * behavior.
 */
public class WebTransportServerDrainIntegrationTest {

  private static final Logger log =
      LoggerFactory.getLogger(WebTransportServerDrainIntegrationTest.class);

  private WebTransportServer server;
  private EventLoopGroup clientGroup;
  private Channel clientUdpChannel;
  private QuicChannel clientQuicChannel;
  private CountDownLatch sessionClosedLatch;
  private CountDownLatch streamAfterDrain;
  private CountDownLatch datagramAfterDrain;

  /**
   * Sets up test server before each test execution.
   *
   * @throws Exception if setup fails
   */
  @Before
  public void setUp() throws Exception {
    sessionClosedLatch = new CountDownLatch(1);
    streamAfterDrain = new CountDownLatch(1);
    datagramAfterDrain = new CountDownLatch(1);

    server =
        new WebTransportServerBuilder()
            .port(0)
            .defaultHandler(
                new WebTransportHandler() {
                  @Override
                  public void onIncomingStream(@NonNull WebTransportSession session,
                      @NonNull WebTransportStream stream) {
                    if (session.isDraining()) {
                      streamAfterDrain.countDown();
                    }
                  }

                  @Override
                  public void onDatagramReceived(@NonNull WebTransportSession session,
                      @NonNull WebTransportBuffer data) {
                    if (session.isDraining()) {
                      datagramAfterDrain.countDown();
                    }
                  }

                  @Override
                  public void onSessionClosed(@NonNull WebTransportSession session) {
                    log.info(
                        "ServerDrainTest: Session closed on server stop: {}",
                        session.getSessionStreamId());
                    sessionClosedLatch.countDown();
                  }
                })
            .build();

    server.start();
    log.info("ServerDrainTest: Server started on port {}", server.getPort());
  }

  /**
   * Tears down client channels and loops after each test execution.
   *
   * @throws Exception if teardown fails
   */
  @After
  public void tearDown() throws Exception {
    if (clientQuicChannel != null && clientQuicChannel.isActive()) {
      clientQuicChannel.close().sync();
    }
    if (clientUdpChannel != null && clientUdpChannel.isActive()) {
      clientUdpChannel.close().sync();
    }
    if (clientGroup != null) {
      clientGroup.shutdownGracefully().sync();
    }
    if (server != null && server.isStarted()) {
      server.stop();
    }
  }

  @Test
  public void testServerDrainAndStop() throws Exception {
    clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols("h3")
            .build();

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .datagram(1024, 1024)
            .maxIdleTimeout(5, TimeUnit.SECONDS)
            .initialMaxData(1000000)
            .initialMaxStreamDataBidirectionalLocal(100000)
            .initialMaxStreamDataBidirectionalRemote(100000)
            .initialMaxStreamsBidirectional(10)
            .initialMaxStreamsUnidirectional(10)
            .build();

    Bootstrap cb = new Bootstrap();
    clientUdpChannel =
        cb.group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    Http3Settings clientSettings = new Http3Settings((id, val) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);

    QuicChannelBootstrap qcb =
        QuicChannel.newBootstrap(clientUdpChannel)
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
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", server.getPort()));

    clientQuicChannel = qcb.connect().get(5, TimeUnit.SECONDS);

    // Establish WebTransport CONNECT stream
    CountDownLatch connectReady = new CountDownLatch(1);
    CountDownLatch peerDrain = new CountDownLatch(1);

    final QuicStreamChannel connectStream =
        Http3.newRequestStream(
                clientQuicChannel,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<Object>() {
                              @Override
                              protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                if (msg instanceof io.netty.handler.codec.http3.Http3DataFrame) {
                                  io.netty.buffer.ByteBuf payload =
                                      ((io.netty.handler.codec.http3.Http3DataFrame) msg).content();
                                  if (payload.readableBytes() == 5
                                      && payload.getInt(payload.readerIndex()) == 0x800078ae
                                      && payload.getByte(payload.readerIndex() + 4) == 0) {
                                    peerDrain.countDown();
                                  }
                                }
                                if (msg instanceof Http3HeadersFrame
                                    && "200"
                                        .equals(
                                            ((Http3HeadersFrame) msg)
                                                .headers()
                                                .status()
                                                .toString())) {
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
    headers.authority("127.0.0.1:" + server.getPort());
    headers.path("/");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
    assertTrue("CONNECT handshake failed", connectReady.await(5, TimeUnit.SECONDS));

    assertTrue("Server should be started", server.isStarted());

    server.drain();
    assertTrue("Peer did not receive WT_DRAIN_SESSION", peerDrain.await(5, TimeUnit.SECONDS));
    QuicStreamChannel dataStream = clientQuicChannel.createStream(
        io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel channel) {}
        }).get(5, TimeUnit.SECONDS);
    io.netty.buffer.ByteBuf streamPayload = dataStream.alloc().buffer();
    WebTransportUtils.writeVarInt(streamPayload, 0x41);
    WebTransportUtils.writeVarInt(streamPayload, connectStream.streamId());
    streamPayload.writeByte(1);
    dataStream.writeAndFlush(streamPayload).sync();
    assertTrue("Existing session rejected a new stream after server drain",
        streamAfterDrain.await(5, TimeUnit.SECONDS));
    io.netty.buffer.ByteBuf datagram = clientQuicChannel.alloc().buffer();
    WebTransportUtils.writeVarInt(datagram, connectStream.streamId() >> 2);
    datagram.writeByte(1);
    clientQuicChannel.writeAndFlush(datagram).sync();
    assertTrue("Existing session rejected a datagram after server drain",
        datagramAfterDrain.await(5, TimeUnit.SECONDS));
    assertEquals(WebTransportServer.ServerState.DRAINING, server.getState());
    assertFalse(server.isAcceptingSessions());
    assertTrue(server.isStarted());
    for (WebTransportSession session : server.getActiveSessions()) {
      assertTrue(session.isDraining());
      assertTrue(session.isOpen());
    }
    CountDownLatch rejected = new CountDownLatch(1);
    QuicStreamChannel rejectedConnect = Http3.newRequestStream(clientQuicChannel,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel stream) {
            stream.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, Object message) {
                if (message instanceof Http3HeadersFrame
                    && "503".contentEquals(((Http3HeadersFrame) message).headers().status())) {
                  rejected.countDown();
                }
              }
            });
          }
        }).sync().getNow();
    io.netty.channel.ChannelFuture rejectedWrite =
        rejectedConnect.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
    assertTrue(rejectedWrite.await(5, TimeUnit.SECONDS));
    if (rejectedWrite.isSuccess()) {
      assertTrue("Draining server admitted a new session", rejected.await(5, TimeUnit.SECONDS));
    } else {
      assertTrue(rejectedWrite.cause() instanceof io.netty.handler.codec.http3.Http3Exception);
      assertTrue("Expected GOAWAY admission rejection",
          rejectedWrite.cause().getMessage().contains("GOAWAY"));
    }

    int serverPort = server.getPort();

    // Trigger server stop
    long stopStarted = System.nanoTime();
    server.stop(200, TimeUnit.MILLISECONDS);
    assertTrue("Shutdown exceeded its bounded grace period",
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopStarted) < 2000);

    // Assert server state is STOPPED
    assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
    assertEquals(0, server.getActiveSessionCount());
    assertFalse("Server should report isStarted() == false after stop", server.isStarted());

    // Assert session closed latch was triggered
    assertTrue(
        "Active session should receive onSessionClosed on server stop",
        sessionClosedLatch.await(5, TimeUnit.SECONDS));

    // Attempt connecting a new client to the stopped port (should fail or time out)
    boolean connectFailed = false;
    try {
      QuicChannelBootstrap qcb2 =
          QuicChannel.newBootstrap(clientUdpChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {}
                  })
              .remoteAddress(new InetSocketAddress("127.0.0.1", serverPort));
      QuicChannel newClient = qcb2.connect().get(2, TimeUnit.SECONDS);
      if (!newClient.isActive()) {
        connectFailed = true;
      }
    } catch (Exception e) {
      connectFailed = true;
    }
    assertTrue("New connection attempts to stopped server should fail", connectFailed);
  }
}
