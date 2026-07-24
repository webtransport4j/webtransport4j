package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
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
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unmocked Integration test verifying real QUIC connection teardown,
 * session unregistration, and active session count decrementing across back-to-back runs.
 */
public class SessionManagerTeardownIntegrationTest {

  private WebTransportServer server;
  private int port;
  private NioEventLoopGroup clientGroup;
  private QuicSslContext clientSslContext;
  private final AtomicInteger sessionsOpenedCount = new AtomicInteger(0);
  private final AtomicInteger sessionsClosedCount = new AtomicInteger(0);

  @Before
  public void setUp() throws Exception {
    sessionsOpenedCount.set(0);
    sessionsClosedCount.set(0);

    WebTransportHandler testHandler = new WebTransportHandler() {
      @Override
      public void onSessionReady(@NonNull WebTransportSession session) {
        sessionsOpenedCount.incrementAndGet();
      }

      @Override
      public void onSessionClosed(@NonNull WebTransportSession session) {
        sessionsClosedCount.incrementAndGet();
      }
    };

    server = WebTransportServer.builder()
        .port(0) // Random port
        .handler("/", testHandler)
        .build();

    server.start();
    port = server.getPort();

    clientGroup = new NioEventLoopGroup(2);
    clientSslContext = QuicSslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols(Http3.supportedApplicationProtocols())
        .build();
  }

  @After
  public void tearDown() {
    if (clientGroup != null) {
      clientGroup.shutdownGracefully();
    }
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testUnmockedConnectionCloseDecrementsActiveSessionCount() throws Exception {
    for (int run = 1; run <= 3; run++) {
      CountDownLatch responseLatch = new CountDownLatch(1);

      Channel udpChannel = new Bootstrap()
          .group(clientGroup)
          .channel(NioDatagramChannel.class)
          .handler(Http3.newQuicClientCodecBuilder()
              .sslContext(clientSslContext)
              .maxIdleTimeout(5, TimeUnit.SECONDS)
              .initialMaxData(10000000)
              .initialMaxStreamDataBidirectionalLocal(1000000)
              .initialMaxStreamDataBidirectionalRemote(1000000)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(100)
              .build())
          .bind(0)
          .sync()
          .channel();

      try {
        Http3Settings settings = new Http3Settings((id, value) -> true);
        settings.enableConnectProtocol(true);
        settings.enableH3Datagram(true);

        QuicChannel quicChannel = QuicChannel.newBootstrap(udpChannel)
            .handler(new ChannelInitializer<QuicChannel>() {
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
            .remoteAddress(new InetSocketAddress("127.0.0.1", port))
            .connect()
            .get(5, TimeUnit.SECONDS);

        QuicStreamChannel connectStream = Http3.newRequestStream(
            quicChannel,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel channel) {
                channel.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                    if (msg instanceof Http3HeadersFrame) {
                      responseLatch.countDown();
                    }
                  }
                });
              }
            }).get(5, TimeUnit.SECONDS);

        // Send CONNECT request headers
        Http3Headers headers = new DefaultHttp3Headers();
        headers.method("CONNECT");
        headers.scheme("https");
        headers.authority("127.0.0.1:" + port);
        headers.path("/");
        headers.set(":protocol", "webtransport");
        connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();

        // Wait for response headers (handshake)
        assertTrue("Handshake response headers must be received for run " + run, responseLatch.await(5, TimeUnit.SECONDS));

        // Verify active session count on server == 1
        long deadline = System.currentTimeMillis() + 3000;
        while (server.getActiveSessionCount() == 0 && System.currentTimeMillis() < deadline) {
          Thread.sleep(50);
        }
        assertEquals("Server active sessions must be 1 during session for run " + run, 1, server.getActiveSessionCount());

        // Forcefully close client connection
        quicChannel.close().sync();

        // Wait for server active session count to return to 0
        long closeDeadline = System.currentTimeMillis() + 5000;
        while (server.getActiveSessionCount() > 0 && System.currentTimeMillis() < closeDeadline) {
          Thread.sleep(50);
        }

        assertEquals("Server active session count must return to 0 after connection teardown for run " + run, 0, server.getActiveSessionCount());
      } finally {
        udpChannel.close();
      }
    }
  }
}
