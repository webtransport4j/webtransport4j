package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
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
 * Unmocked integration test verifying graceful zero-downtime server shutdown and channel drain behavior.
 */
public class WebTransportServerDrainIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(WebTransportServerDrainIntegrationTest.class);

  private WebTransportServer server;
  private EventLoopGroup clientGroup;
  private Channel clientUdpChannel;
  private QuicChannel clientQuicChannel;
  private CountDownLatch sessionClosedLatch;

  @Before
  public void setUp() throws Exception {
    sessionClosedLatch = new CountDownLatch(1);

    server = new WebTransportServerBuilder()
        .port(0)
        .defaultHandler(new WebTransportHandler() {
          @Override
          public void onSessionClosed(@NonNull WebTransportSession session) {
            log.info("ServerDrainTest: Session closed on server stop: {}", session.getSessionStreamId());
            sessionClosedLatch.countDown();
          }
        })
        .build();

    server.start();
    log.info("ServerDrainTest: Server started on port {}", server.getPort());
  }

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

    QuicSslContext clientSslContext = QuicSslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols("h3")
        .build();

    ChannelHandler clientCodec = Http3.newQuicClientCodecBuilder()
        .sslContext(clientSslContext)
        .maxIdleTimeout(5, TimeUnit.SECONDS)
        .initialMaxData(1000000)
        .initialMaxStreamDataBidirectionalLocal(100000)
        .initialMaxStreamDataBidirectionalRemote(100000)
        .initialMaxStreamsBidirectional(10)
        .initialMaxStreamsUnidirectional(10)
        .build();

    Bootstrap cb = new Bootstrap();
    clientUdpChannel = cb.group(clientGroup)
        .channel(NioDatagramChannel.class)
        .handler(clientCodec)
        .bind(0)
        .sync()
        .channel();

    Http3Settings clientSettings = new Http3Settings((id, val) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);

    QuicChannelBootstrap qcb = QuicChannel.newBootstrap(clientUdpChannel)
        .handler(new ChannelInitializer<QuicChannel>() {
          @Override
          protected void initChannel(QuicChannel ch) {
            ch.pipeline().addLast(new Http3ClientConnectionHandler(
                null, null, new UnknownStreamHandlerFactory(),
                new DefaultHttp3SettingsFrame(clientSettings),
                false,
                (id, value) -> true));
          }
        })
        .remoteAddress(new InetSocketAddress("127.0.0.1", server.getPort()));

    clientQuicChannel = qcb.connect().get(5, TimeUnit.SECONDS);

    // Establish WebTransport CONNECT stream
    CountDownLatch connectReady = new CountDownLatch(1);

    QuicStreamChannel connectStream = Http3.newRequestStream(
        clientQuicChannel,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {
            ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof Http3HeadersFrame
                    && "200".equals(((Http3HeadersFrame) msg).headers().status().toString())) {
                  connectReady.countDown();
                }
              }
            });
          }
        }).sync().getNow();

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("127.0.0.1:" + server.getPort());
    headers.path("/");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
    assertTrue("CONNECT handshake failed", connectReady.await(5, TimeUnit.SECONDS));

    assertTrue("Server should be started", server.isStarted());

    int serverPort = server.getPort();

    // Trigger server stop
    server.stop();

    // Assert server state is STOPPED
    assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
    assertFalse("Server should report isStarted() == false after stop", server.isStarted());

    // Assert session closed latch was triggered
    assertTrue("Active session should receive onSessionClosed on server stop", sessionClosedLatch.await(5, TimeUnit.SECONDS));

    // Attempt connecting a new client to the stopped port (should fail or time out)
    boolean connectFailed = false;
    try {
      QuicChannelBootstrap qcb2 = QuicChannel.newBootstrap(clientUdpChannel)
          .handler(new ChannelInitializer<QuicChannel>() {
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
