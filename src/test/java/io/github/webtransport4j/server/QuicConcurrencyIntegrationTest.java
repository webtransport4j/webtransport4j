package io.github.webtransport4j.server;

import static io.github.webtransport4j.concurrency.ConcurrencySupport.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.client.WebTransportClientHandler;
import io.github.webtransport4j.resilience.AdaptiveOverloadProtectionPolicy;
import io.github.webtransport4j.resilience.OverloadProtectionPolicy;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
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
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Real QUIC connections exercise dispatch/drain races and TLS engine snapshots. */
public class QuicConcurrencyIntegrationTest {
  @Test(timeout = 30000)
  public void streamsAndDatagramsRemainUsableWhileSessionDrains() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    CountDownLatch ready = new CountDownLatch(1);
    CountDownLatch closed = new CountDownLatch(1);
    CountDownLatch streams = new CountDownLatch(4);
    CountDownLatch datagram = new CountDownLatch(1);
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(context(certificate))
            .defaultHandler(
                new WebTransportHandler() {
                  public void onSessionReady(WebTransportSession session) {
                    ready.countDown();
                  }

                  public void onSessionClosed(WebTransportSession session) {
                    closed.countDown();
                  }

                  public void onIncomingStream(
                      WebTransportSession session, WebTransportStream stream) {
                    stream.onData(data -> streams.countDown());
                  }

                  public void onDatagramReceived(
                      WebTransportSession session, WebTransportBuffer data) {
                    datagram.countDown();
                  }
                })
            .build();
    ExecutorService senders = Executors.newFixedThreadPool(2);
    try {
      server.start();
      try (Client client = new Client(server.getPort(), certificate)) {
        client
            .quic
            .eventLoop()
            .submit(
                () -> assertTrue(io.github.webtransport4j.internal.EventLoopSafety.inEventLoop()))
            .get(5, TimeUnit.SECONDS);
        await(ready);
        CountDownLatch go = new CountDownLatch(1);
        final Future<?> streamWork =
            senders.submit(
                () -> {
                  await(go);
                  try {
                    for (int i = 0; i < 4; i++) {
                      QuicStreamChannel stream =
                          client
                              .quic
                              .createStream(
                                  QuicStreamType.BIDIRECTIONAL,
                                  new ChannelInitializer<QuicStreamChannel>() {
                                    protected void initChannel(QuicStreamChannel ch) {}
                                  })
                              .get(5, TimeUnit.SECONDS);
                      ByteBuf data = stream.alloc().buffer();
                      WebTransportUtils.writeVarInt(data, 0x41);
                      WebTransportUtils.writeVarInt(data, client.connect.streamId());
                      data.writeBytes(new byte[] {1, 2, 3});
                      stream.writeAndFlush(data).get(5, TimeUnit.SECONDS);
                    }
                  } catch (Exception e) {
                    throw new AssertionError(e);
                  }
                });
        final Future<?> datagramWork =
            senders.submit(
                () -> {
                  await(go);
                  for (int i = 0; i < 20; i++) {
                    ByteBuf data = client.quic.alloc().buffer();
                    WebTransportUtils.writeVarInt(data, client.connect.streamId() >> 2);
                    data.writeBytes(new byte[] {4, 5, 6});
                    client.quic.writeAndFlush(data);
                  }
                });
        server.drain();
        go.countDown();
        streamWork.get(10, TimeUnit.SECONDS);
        datagramWork.get(10, TimeUnit.SECONDS);
        await(streams);
        await(datagram);
        assertTrue(server.isRunning());
        assertTrue(server.getActiveSessions().iterator().next().isDraining());
      }
      await(closed);
      assertTrue(server.getActiveSessions().isEmpty());
    } finally {
      senders.shutdownNow();
      server.close();
      certificate.delete();
    }
  }

  @Test(timeout = 30000)
  public void liveReloadChangesNewHandshakeWithoutChangingExistingEngine() throws Exception {
    SelfSignedCertificate first = new SelfSignedCertificate("localhost");
    SelfSignedCertificate second = new SelfSignedCertificate("localhost");
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(context(first))
            .defaultHandler(new WebTransportHandler() {})
            .build();
    try {
      server.start();
      try (Client oldClient = new Client(server.getPort(), first)) {
        X509Certificate oldPeer =
            (X509Certificate) oldClient.quic.sslEngine().getSession().getPeerCertificates()[0];
        Method install =
            WebTransportServer.class.getDeclaredMethod(
                "installReloadedSslContext", QuicSslContext.class);
        install.setAccessible(true);
        install.invoke(server, context(second));
        try (Client newClient = new Client(server.getPort(), second)) {
          X509Certificate newPeer =
              (X509Certificate) newClient.quic.sslEngine().getSession().getPeerCertificates()[0];
          assertTrue(!oldPeer.getSerialNumber().equals(newPeer.getSerialNumber()));
          assertEquals(
              oldPeer.getSerialNumber(),
              ((X509Certificate) oldClient.quic.sslEngine().getSession().getPeerCertificates()[0])
                  .getSerialNumber());
          assertTrue(oldClient.quic.isActive());
        }
      }
    } finally {
      server.close();
      first.delete();
      second.delete();
    }
  }

  @Test(timeout = 30000)
  public void admittedSessionReleasesOverloadPermitOnDisconnect() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    OverloadProtectionPolicy policy = org.mockito.Mockito.mock(OverloadProtectionPolicy.class);
    org.mockito.Mockito.when(policy.tryAcquire(org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(OverloadProtectionPolicy.AdmissionResult.allowed());
    try (WebTransportServer server = WebTransportServer.builder()
        .port(0)
        .sslContext(context(certificate))
        .overloadProtectionPolicy(policy)
        .defaultHandler(new WebTransportHandler() {})
        .build()) {
      server.start();
      try (Client client = new Client(server.getPort(), certificate)) {
        assertTrue(client.quic.isActive());
        org.mockito.Mockito.verify(policy).tryAcquire(0);
      }
      org.mockito.Mockito.verify(policy, org.mockito.Mockito.timeout(5000)).release();
    } finally {
      certificate.delete();
    }
  }

  @Test(timeout = 30000)
  public void adaptiveCapacityAdmitsConfiguredMaximumAndRecoversAfterDisconnect() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    CountDownLatch released = new CountDownLatch(1);
    AdaptiveOverloadProtectionPolicy policy = new AdaptiveOverloadProtectionPolicy(1.0, 1, 5);
    try (WebTransportServer server = WebTransportServer.builder()
        .port(0)
        .transportType("nio")
        .sslContext(context(certificate))
        .overloadProtectionPolicy(policy)
        .defaultHandler(new WebTransportHandler() {
          public void onSessionClosed(WebTransportSession session) {
            released.countDown();
          }
        })
        .build()) {
      server.start();
      try (Client first = new Client(server.getPort(), certificate)) {
        assertTrue(first.connect.isActive());
        try (Client rejected = new Client(server.getPort(), certificate, "503")) {
          assertEquals("5", rejected.retryAfter);
        }
      }
      await(released);
      try (Client replacement = new Client(server.getPort(), certificate)) {
        assertTrue(replacement.connect.isActive());
      }
    } finally {
      certificate.delete();
    }
  }

  private static QuicSslContext context(SelfSignedCertificate certificate) {
    return QuicSslContextBuilder.forServer(
            certificate.privateKey(), null, certificate.certificate())
        .applicationProtocols("h3")
        .build();
  }

  private static final class Client implements AutoCloseable {
    final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel udp;
    QuicChannel quic;
    QuicStreamChannel connect;
    String retryAfter;

    Client(int port, SelfSignedCertificate certificate) throws Exception {
      this(port, certificate, "200");
    }

    Client(int port, SelfSignedCertificate certificate, String expectedStatus) throws Exception {
      try {
        QuicSslContext ssl =
            QuicSslContextBuilder.forClient()
                .trustManager(certificate.certificate())
                .applicationProtocols("h3")
                .build();
        udp =
            new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .handler(
                    Http3.newQuicClientCodecBuilder()
                        .sslContext(ssl)
                        .sslEngineProvider(
                            channel -> ssl.newEngine(channel.alloc(), "localhost", port))
                        .datagram(1024, 1024)
                        .initialMaxData(1000000)
                        .initialMaxStreamDataBidirectionalLocal(100000)
                        .initialMaxStreamDataBidirectionalRemote(100000)
                        .initialMaxStreamsBidirectional(100)
                        .initialMaxStreamsUnidirectional(100)
                        .initialMaxStreamDataUnidirectional(100000)
                        .build())
                .bind(0)
                .sync()
                .channel();
        Http3Settings settings = new Http3Settings((id, value) -> true);
        settings.enableConnectProtocol(true);
        settings.enableH3Datagram(true);
        settings.put(0x2b64L, 100L);
        settings.put(0x2b65L, 100L);
        settings.put(0x2b61L, 1000000L);
        quic =
            QuicChannel.newBootstrap(udp)
                .handler(
                    new WebTransportClientHandler(new DefaultHttp3SettingsFrame(settings), true))
                .remoteAddress(new InetSocketAddress("127.0.0.1", port))
                .connect()
                .get(5, TimeUnit.SECONDS);
        CompletableFuture<Http3HeadersFrame> response = new CompletableFuture<>();
        connect =
            Http3.newRequestStream(
                    quic,
                    new ChannelInitializer<QuicStreamChannel>() {
                      protected void initChannel(QuicStreamChannel ch) {
                        ch.pipeline()
                            .addLast(
                                new SimpleChannelInboundHandler<Object>() {
                                  protected void channelRead0(
                                      ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof Http3HeadersFrame) {
                                      response.complete((Http3HeadersFrame) msg);
                                    }
                                  }
                                });
                      }
                    })
                .get(5, TimeUnit.SECONDS);
        connect
            .writeAndFlush(
                new DefaultHttp3HeadersFrame(
                    new DefaultHttp3Headers()
                        .method("CONNECT")
                        .scheme("https")
                        .authority("localhost:" + port)
                        .path("/test")
                        .set(":protocol", "webtransport")))
            .get(5, TimeUnit.SECONDS);
        Http3HeadersFrame headers = response.get(5, TimeUnit.SECONDS);
        assertEquals(expectedStatus, headers.headers().status().toString());
        CharSequence retry = headers.headers().get("retry-after");
        retryAfter = retry == null ? null : retry.toString();
      } catch (Throwable error) {
        close();
        throw error;
      }
    }

    public void close() {
      if (quic != null) {
        quic.close().awaitUninterruptibly(5000);
      }
      if (udp != null) {
        udp.close().awaitUninterruptibly(5000);
      }
      group.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly(5000);
    }
  }
}
