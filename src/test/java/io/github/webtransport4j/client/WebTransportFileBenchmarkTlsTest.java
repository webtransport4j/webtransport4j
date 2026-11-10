package io.github.webtransport4j.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import io.netty.util.concurrent.Future;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class WebTransportFileBenchmarkTlsTest {
  @Test
  void acceptsTrustedLoopbackCertificate() throws Exception {
    checkHandshake("127.0.0.1", true, true);
  }

  @Test
  void rejectsUntrustedCertificate() throws Exception {
    checkHandshake("127.0.0.1", false, false);
  }

  @Test
  void rejectsTrustedCertificateForAnotherEndpoint() throws Exception {
    checkHandshake("example.invalid", true, false);
  }

  private void checkHandshake(String identity, boolean trustServer, boolean expectedSuccess)
      throws Exception {
    X509Bundle bundle =
        new CertificateBuilder()
            .subject("CN=" + identity)
            .addSanIpAddress(identity.equals("127.0.0.1") ? "127.0.0.1" : "127.0.0.2")
            .setIsCertificateAuthority(true)
            .buildSelfSigned();
    File certificate = bundle.toTempCertChainPem();
    File privateKey = bundle.toTempPrivateKeyPem();
    MultiThreadIoEventLoopGroup group =
        new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    Channel server = null;
    Channel client = null;
    QuicChannel connection = null;
    try {
      QuicSslContext serverContext =
          QuicSslContextBuilder.forServer(privateKey, null, certificate)
              .applicationProtocols(Http3.supportedApplicationProtocols())
              .build();
      server =
          new Bootstrap()
              .group(group)
              .channel(NioDatagramChannel.class)
              .handler(
                  new QuicServerCodecBuilder()
                      .sslContext(serverContext)
                      .maxIdleTimeout(3, TimeUnit.SECONDS)
                      .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                      .handler(new ChannelInboundHandlerAdapter())
                      .build())
              .bind(new InetSocketAddress("127.0.0.1", 0))
              .sync()
              .channel();
      QuicSslContext clientContext =
          WebTransportFileBenchmark.createSslContext(
              trustServer ? certificate.getAbsolutePath() : null);
      client =
          new Bootstrap()
              .group(group)
              .channel(NioDatagramChannel.class)
              .handler(
                  Http3.newQuicClientCodecBuilder()
                      .sslEngineProvider(ch -> clientContext.newEngine(ch.alloc(), "127.0.0.1", 4433))
                      .maxIdleTimeout(3, TimeUnit.SECONDS)
                      .build())
              .bind(0)
              .sync()
              .channel();
      Future<QuicChannel> result =
          QuicChannel.newBootstrap(client)
              .handler(new ChannelInboundHandlerAdapter())
              .remoteAddress(server.localAddress())
              .connect();
      assertTrue(result.await(10, TimeUnit.SECONDS), "Handshake did not complete");
      if (result.isSuccess()) {
        connection = result.getNow();
      }
      if (expectedSuccess) {
        assertTrue(result.isSuccess(), () -> "Trusted handshake failed: " + result.cause());
      } else {
        assertFalse(result.isSuccess(), "Unauthenticated peer was accepted");
      }
    } finally {
      if (connection != null) {
        connection.close().sync();
      }
      if (client != null) {
        client.close().sync();
      }
      if (server != null) {
        server.close().sync();
      }
      group.shutdownGracefully().sync();
      certificate.delete();
      privateKey.delete();
    }
  }
}
