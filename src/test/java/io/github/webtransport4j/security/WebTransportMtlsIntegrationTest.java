package io.github.webtransport4j.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
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
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Test;

/**
 * Rigorous integration tests verifying enterprise mTLS client certificate authentication
 * and strict origin validation.
 */
public class WebTransportMtlsIntegrationTest {

  private WebTransportServer server;
  private MultiThreadIoEventLoopGroup clientGroup;
  private Channel clientUdpChannel;
  private QuicChannel clientQuicChannel;

  /**
   * Cleans up server and client channels after each test execution.
   */
  @After
  public void tearDown() throws Exception {
    if (clientQuicChannel != null && clientQuicChannel.isActive()) {
      try {
        clientQuicChannel.close().sync();
      } catch (Exception ignored) {
        // Ignored
      }
    }
    if (clientUdpChannel != null && clientUdpChannel.isActive()) {
      try {
        clientUdpChannel.close().sync();
      } catch (Exception ignored) {
        // Ignored
      }
    }
    if (clientGroup != null) {
      try {
        clientGroup.shutdownGracefully().sync();
      } catch (Exception ignored) {
        // Ignored
      }
    }
    if (server != null && server.isStarted()) {
      server.stop();
    }
  }

  /**
   * Tests that mutual TLS succeeds when the client presents a trusted certificate,
   * and that peer certificates are accessible on the session.
   */
  @Test
  public void testMtlsClientAuthenticationSuccess() throws Exception {
    final SelfSignedCertificate serverCert = new SelfSignedCertificate("localhost");
    final SelfSignedCertificate clientCert = new SelfSignedCertificate("client.enterprise.internal");

    final CountDownLatch sessionReadyLatch = new CountDownLatch(1);
    final AtomicReference<WebTransportSession> sessionRef = new AtomicReference<>();

    server = new WebTransportServerBuilder()
        .port(0)
        .ssl(serverCert.privateKey().getAbsolutePath(), serverCert.certificate().getAbsolutePath())
        .clientAuth(ClientAuthMode.REQUIRE)
        .trustManager(clientCert.certificate())
        .defaultHandler(new WebTransportHandler() {
          @Override
          public void onSessionReady(@NonNull WebTransportSession session) {
            sessionRef.set(session);
            sessionReadyLatch.countDown();
          }
        })
        .build();
    server.start();

    clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    final QuicSslContext clientSslContext = QuicSslContextBuilder.forClient()
        .keyManager(clientCert.privateKey(), null, clientCert.certificate())
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols("h3")
        .build();

    final ChannelHandler clientCodec = Http3.newQuicClientCodecBuilder()
        .sslContext(clientSslContext)
        .maxIdleTimeout(5, TimeUnit.SECONDS)
        .initialMaxData(1000000)
        .initialMaxStreamDataBidirectionalLocal(100000)
        .initialMaxStreamDataBidirectionalRemote(100000)
        .initialMaxStreamsBidirectional(10)
        .initialMaxStreamsUnidirectional(10)
        .build();

    final Bootstrap cb = new Bootstrap();
    clientUdpChannel = cb.group(clientGroup)
        .channel(NioDatagramChannel.class)
        .handler(clientCodec)
        .bind(0)
        .sync()
        .channel();

    final Http3Settings clientSettings = new Http3Settings((id, val) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);

    final QuicChannelBootstrap qcb = QuicChannel.newBootstrap(clientUdpChannel)
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

    final CountDownLatch connectReady = new CountDownLatch(1);
    final QuicStreamChannel connectStream = Http3.newRequestStream(
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

    final Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("127.0.0.1:" + server.getPort());
    headers.path("/");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
    assertTrue(connectReady.await(5, TimeUnit.SECONDS));
    assertTrue(sessionReadyLatch.await(5, TimeUnit.SECONDS));

    final WebTransportSession activeSession = sessionRef.get();
    assertNotNull(activeSession);

    final Certificate[] peerCerts = activeSession.getPeerCertificates();
    assertNotNull(peerCerts);
    assertTrue(peerCerts.length > 0);
  }

  /**
   * Tests strict origin validation where requests missing the Origin header are rejected with 403.
   */
  @Test
  public void testStrictOriginValidationRejectsMissingHeader() throws Exception {
    final SelfSignedCertificate serverCert = new SelfSignedCertificate("localhost");

    server = new WebTransportServerBuilder()
        .port(0)
        .ssl(serverCert.privateKey().getAbsolutePath(), serverCert.certificate().getAbsolutePath())
        .strictOriginValidation(true)
        .allowedOrigins("https://authorized.example.com")
        .defaultHandler(new WebTransportHandler() {
          @Override
          public void onSessionReady(@NonNull WebTransportSession session) {
            // No-op
          }
        })
        .build();
    server.start();

    clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    final QuicSslContext clientSslContext = QuicSslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols("h3")
        .build();

    final ChannelHandler clientCodec = Http3.newQuicClientCodecBuilder()
        .sslContext(clientSslContext)
        .maxIdleTimeout(5, TimeUnit.SECONDS)
        .initialMaxData(1000000)
        .initialMaxStreamDataBidirectionalLocal(100000)
        .initialMaxStreamDataBidirectionalRemote(100000)
        .initialMaxStreamsBidirectional(10)
        .initialMaxStreamsUnidirectional(10)
        .build();

    final Bootstrap cb = new Bootstrap();
    clientUdpChannel = cb.group(clientGroup)
        .channel(NioDatagramChannel.class)
        .handler(clientCodec)
        .bind(0)
        .sync()
        .channel();

    final Http3Settings clientSettings = new Http3Settings((id, val) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);

    final QuicChannelBootstrap qcb = QuicChannel.newBootstrap(clientUdpChannel)
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

    final CountDownLatch responseLatch = new CountDownLatch(1);
    final AtomicReference<String> statusRef = new AtomicReference<>();

    final QuicStreamChannel connectStream = Http3.newRequestStream(
        clientQuicChannel,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {
            ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof Http3HeadersFrame) {
                  statusRef.set(((Http3HeadersFrame) msg).headers().status().toString());
                  responseLatch.countDown();
                }
              }
            });
          }
        }).sync().getNow();

    // Do NOT set origin header
    final Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("127.0.0.1:" + server.getPort());
    headers.path("/");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
    assertTrue(responseLatch.await(5, TimeUnit.SECONDS));
    assertEquals("403", statusRef.get());
  }
}
