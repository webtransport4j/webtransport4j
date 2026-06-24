package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import static org.junit.Assert.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class WebTransportResumptionLatencyTest {

  private NioEventLoopGroup serverGroup;
  private NioEventLoopGroup clientGroup;
  private Channel serverChannel;
  private int port;
  private WebTransportServer webTransportServer;

  @Before
  public void setUp() throws Exception {
    webTransportServer = new WebTransportServer();
    webTransportServer.registerHandler(
        "/test-resumption",
        new WebTransportHandler() {
          @Override
          public void onSessionReady(WebTransportSession session) {
            System.out.println("SERVER: Session ready: " + session.getSessionStreamId());
          }
        });

    serverGroup = new NioEventLoopGroup(1);
    clientGroup = new NioEventLoopGroup(1);

    // Build Server SSL Context
    io.netty.handler.ssl.util.SelfSignedCertificate ssc = new io.netty.handler.ssl.util.SelfSignedCertificate();
    QuicSslContext serverSslContext =
        QuicSslContextBuilder.forServer(ssc.privateKey(), null, ssc.certificate())
            .earlyData(true)
            .sessionCacheSize(20480)
            .sessionTimeout(86400)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    // Enable session ticket key on server side
    if (serverSslContext.sessionContext() instanceof QuicSslSessionContext) {
      SslSessionTicketKey ticketKey = new SslSessionTicketKey(
          "1234567890123456".getBytes(), "1234567890123456".getBytes(), "1234567890123456".getBytes()
      );
      ((QuicSslSessionContext) serverSslContext.sessionContext()).setTicketKeys(
          new SslSessionTicketKey[] { ticketKey }
      );
    }

    Http3Settings serverSettings = new Http3Settings((id, value) -> true);
    serverSettings.enableH3Datagram(true);
    serverSettings.enableConnectProtocol(true);
    serverSettings.put(0x2c7cf000L, 1L); // wt_enabled
    serverSettings.put(0x2b64L, 10L);
    serverSettings.put(0x2b65L, 10L);
    serverSettings.put(0x2b61L, 100000L);

    ChannelHandler serverCodec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(serverSslContext)
            .maxIdleTimeout(15000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.attr(WebTransportAttributeKeys.SERVER_KEY).set(webTransportServer);
                    ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(new WebTransportSessionManager());
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(10L);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(10L);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(100000L);

                    ch.pipeline().addLast(new WebTransportDatagramDecoder());
                    ch.pipeline().addLast(new WebTransportCapsuleHandler());
                    ch.pipeline().addLast(new MessageDispatcher());
                    ch.pipeline().addLast(
                        new Http3ServerConnectionHandler(
                            new ChannelInitializer<QuicStreamChannel>() {
                              @Override
                              protected void initChannel(QuicStreamChannel stream) {
                                stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                stream.pipeline().addLast(new RawWebTransportHandler());
                                stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                stream.pipeline().addLast(new WebTransportHeadersHandler());
                                stream.pipeline().addLast(new WebTransportDataHandler());
                                stream.pipeline().addLast(new WebTransportCapsuleHandler());
                                stream.pipeline().addLast(new MessageDispatcher());
                              }
                            },
                            new ChannelInboundHandlerAdapter() {
                              @Override
                              public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                io.netty.util.ReferenceCountUtil.release(msg);
                              }
                            },
                            (streamType) -> null,
                            new DefaultHttp3SettingsFrame(serverSettings),
                            true));
                  }
                })
            .build();

    serverChannel =
        new Bootstrap()
            .group(serverGroup)
            .channel(NioDatagramChannel.class)
            .handler(serverCodec)
            .bind(new InetSocketAddress("127.0.0.1", 0))
            .sync()
            .channel();

    port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
  }

  @After
  public void tearDown() throws Exception {
    if (serverChannel != null) {
      serverChannel.close().sync();
    }
    serverGroup.shutdownGracefully();
    clientGroup.shutdownGracefully();
  }

  @Test
  public void testResumptionLatency() throws Exception {
    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 100000L);

    // ==========================================
    // FIRST CONNECTION: Full TLS Handshake
    // ==========================================
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
                    ch.pipeline().addLast(
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

    long startFull = System.nanoTime();
    QuicChannel quicClient1 = bootstrap1.connect().sync().getNow();
    long durationFullNs = System.nanoTime() - startFull;
    double durationFullMs = durationFullNs / 1_000_000.0;

    System.out.println("⏱️ FULL HANDSHAKE LATENCY: " + durationFullMs + " ms");

    // Establish a WebTransport session to verify connection is operational
    final CountDownLatch latch1 = new CountDownLatch(1);
    Http3.newRequestStream(
            quicClient1,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline().addLast(
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
                headers.method("CONNECT").scheme("https").path("/test-resumption").authority("localhost").set(":protocol", "webtransport");
                f.getNow().writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Session 1 establishment failed", latch1.await(5, TimeUnit.SECONDS));

    // Wait for the server to send the Session Ticket
    Thread.sleep(500L);

    quicClient1.close().sync();
    clientChannel1.close().sync();

    // Sleep slightly to let socket close and cache stabilize
    Thread.sleep(500L);

    // ==========================================
    // SECOND CONNECTION: Resumed TLS Handshake (1-RTT Resumption)
    // ==========================================
    ChannelHandler clientCodec2 =
        Http3.newQuicClientCodecBuilder()
            .sslEngineProvider(q -> clientSslContext.newEngine(q.alloc(), "localhost", port))
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
                    ch.pipeline().addLast(
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

    long startResumed = System.nanoTime();
    QuicChannel quicClient2 = bootstrap2.connect().sync().getNow();
    long durationResumedNs = System.nanoTime() - startResumed;
    double durationResumedMs = durationResumedNs / 1_000_000.0;

    System.out.println("⏱️ RESUMED HANDSHAKE LATENCY: " + durationResumedMs + " ms");

    // Establish session 2
    final CountDownLatch latch2 = new CountDownLatch(1);
    Http3.newRequestStream(
            quicClient2,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline().addLast(
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
              if (f.getNow() != null) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT").scheme("https").path("/test-resumption").authority("localhost").set(":protocol", "webtransport");
                f.getNow().writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Session 2 establishment failed", latch2.await(5, TimeUnit.SECONDS));

    // Confirm that the second connection was actually resumed at client TLS level
    javax.net.ssl.SSLEngine clientEngine = quicClient2.sslEngine();
    java.lang.reflect.Method m = clientEngine.getClass().getDeclaredMethod("isSessionReused");
    m.setAccessible(true);
    boolean clientResumed = (Boolean) m.invoke(clientEngine);
    assertTrue("Client Connection 2 SHOULD be resumed (QUIC Level Session Resumption)", clientResumed);

    System.out.println("🚀 RESUMPTION VERIFIED SUCCESSFULLY!");
    double speedup = durationFullMs / durationResumedMs;
    System.out.println("📊 Speedup: " + speedup + "x faster");
    assertTrue("Resumed connection should be faster than a full handshake (Full: " + durationFullMs + "ms, Resumed: " + durationResumedMs + "ms)", durationFullMs > durationResumedMs);

    quicClient2.close().sync();
    clientChannel2.close().sync();
  }
}
