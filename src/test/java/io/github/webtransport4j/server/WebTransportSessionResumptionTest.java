package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
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
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WebTransportSessionResumptionTest {

  private WebTransportServer webTransportServer;
  private EventLoopGroup clientGroup;
  private int port;

  @Before
  public void setUp() throws Exception {
    System.setProperty("webtransport4j.quic.token.handler", "hmac");
    System.setProperty("io.netty.handler.ssl.openssl.sessionCacheClient", "true");
    System.setProperty("webtransport4j.ssl.session.timeout.seconds", "86400");
    System.setProperty("webtransport4j.ssl.session.cache.size", "20480");

    port = 8888 + (int) (Math.random() * 1000);
    System.setProperty("webtransport4j.server.port", String.valueOf(port));

    webTransportServer = new WebTransportServer(new WebTransportHandler(){});

    CountDownLatch serverLatch = new CountDownLatch(1);
    new Thread(
            () -> {
              try {
                webTransportServer.start();
              } catch (Exception e) {
                e.printStackTrace();
              }
            })
        .start();

    // Wait for server to bind
    Thread.sleep(500);

    clientGroup = new io.netty.channel.MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty("webtransport4j.quic.token.handler");
    System.clearProperty("io.netty.handler.ssl.openssl.sessionCacheClient");
    System.clearProperty("webtransport4j.ssl.session.timeout.seconds");
    System.clearProperty("webtransport4j.ssl.session.cache.size");
    System.clearProperty("webtransport4j.server.port");

    if (clientGroup != null) {
      clientGroup.shutdownGracefully().sync();
    }
  }

  @Test
  public void testSessionResumption() throws Exception {
    final WebTransportSession[] serverSessions = new WebTransportSession[2];
    CountDownLatch session1Latch = new CountDownLatch(1);
    CountDownLatch session2Latch = new CountDownLatch(1);

    webTransportServer.registerHandler(
        "/test-0rtt",
        new WebTransportHandler() {
          @Override
          public void onSessionReady(WebTransportSession session) {
            if (serverSessions[0] == null) {
              serverSessions[0] = session;
              session1Latch.countDown();
            } else {
              serverSessions[1] = session;
              session2Latch.countDown();
            }
          }
        });

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .earlyData(true)
            .sessionCacheSize(20480)
            .sessionTimeout(86400)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L);

    ChannelHandler clientCodec1 =
        Http3.newQuicClientCodecBuilder()
            .sslEngineProvider(q -> clientSslContext.newEngine(q.alloc(), "localhost", port))
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .build();

    Channel clientChannel1 =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec1)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap1 =
        QuicChannel.newBootstrap(clientChannel1)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    // ==========================================
    // FIRST CONNECTION (Should NOT be resumed)
    // ==========================================
    QuicChannel quicClient1 = bootstrap1.connect().sync().getNow();

    final CountDownLatch latch1 = new CountDownLatch(1);
    Http3.newRequestStream(
            quicClient1,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              if ("200".equals(((Http3HeadersFrame) msg).headers().status().toString())) {
                                latch1.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT").scheme("https").path("/test-0rtt").authority("localhost").set(":protocol", "webtransport");
                f.getNow().writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Connection 1 failed", latch1.await(5, TimeUnit.SECONDS));
    assertTrue("Server Session 1 failed", session1Latch.await(5, TimeUnit.SECONDS));
    assertNotNull(serverSessions[0]);
    
    // Wait slightly to ensure handshake event completes if not already
    Thread.sleep(200);
    assertFalse("Connection 1 should NOT be resumed", serverSessions[0].isSessionResumed());

    // Allow time for NewSessionTicket to be received from server BEFORE closing!
    Thread.sleep(500L);

    quicClient1.close().sync();
    clientChannel1.close().sync();

    // Allow time for NewSessionTicket to be processed and cached
    Thread.sleep(500L);

    // ==========================================
    // SECOND CONNECTION (Should be resumed via 1-RTT Session Resumption)
    // ==========================================
    ChannelHandler clientCodec2 =
        Http3.newQuicClientCodecBuilder()
            .sslEngineProvider(q -> clientSslContext.newEngine(q.alloc(), "localhost", port)) // USING THE SAME SSL CONTEXT WITH HOST/PORT
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .build();

    Channel clientChannel2 =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec2)
            .bind(0)
            .sync()
            .channel();


    QuicChannelBootstrap bootstrap2 =
        QuicChannel.newBootstrap(clientChannel2)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient2 = bootstrap2.connect().sync().getNow();

    final CountDownLatch latch2 = new CountDownLatch(1);
    Http3.newRequestStream(
            quicClient2,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              if ("200".equals(((Http3HeadersFrame) msg).headers().status().toString())) {
                                latch2.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT").scheme("https").path("/test-0rtt").authority("localhost").set(":protocol", "webtransport");
                f.getNow().writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Connection 2 failed", latch2.await(5, TimeUnit.SECONDS));
    assertTrue("Server Session 2 failed", session2Latch.await(5, TimeUnit.SECONDS));
    assertNotNull(serverSessions[1]);

    // Wait slightly to ensure handshake event completes
    Thread.sleep(200);

    javax.net.ssl.SSLEngine clientEngine = quicClient2.sslEngine();
    java.lang.reflect.Method m = clientEngine.getClass().getDeclaredMethod("isSessionReused");
    m.setAccessible(true);
    boolean clientResumed = (Boolean) m.invoke(clientEngine);
    assertTrue("Client Connection 2 SHOULD be resumed (QUIC Level Session Resumption)", clientResumed);
    
    // Server also accurately reports resumption!
    assertTrue("Server SHOULD recognize the session resumption!", serverSessions[1].isSessionResumed());

    quicClient2.close().sync();
    clientChannel2.close().sync();
  }
}
