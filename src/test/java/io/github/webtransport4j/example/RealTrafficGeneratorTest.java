package io.github.webtransport4j.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.lang.reflect.Method;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/** Checks stream response accounting through the actual generator callbacks. */
public class RealTrafficGeneratorTest {
  @Test(timeout = 10000)
  public void fragmentedResponseCannotStandInForAnotherStream() throws Exception {
    QuicChannel quic = respondingChannel(false);
    RealTrafficGenerator.LiveSession session =
        new RealTrafficGenerator.LiveSession(quic, null, 0, null, null);
    TimeoutException failure =
        assertThrows(
            TimeoutException.class,
            () -> RealTrafficGenerator.sendStreams(session, true, 2, "hello"));
    assertTrue(failure.getMessage().contains("1/2"));
  }

  @Test(timeout = 10000)
  public void countsEachRespondingStreamExactlyOnce() throws Exception {
    QuicChannel quic = respondingChannel(true);
    RealTrafficGenerator.LiveSession session =
        new RealTrafficGenerator.LiveSession(quic, null, 0, null, null);
    assertEquals(2, RealTrafficGenerator.sendStreams(session, true, 2, "hello"));
  }

  @Test(timeout = 20000)
  public void rejectedHandshakeClosesClientTransportAndEventLoop() throws Exception {
    io.netty.handler.ssl.util.SelfSignedCertificate certificate =
        new io.netty.handler.ssl.util.SelfSignedCertificate("localhost");
    io.github.webtransport4j.server.WebTransportServer server =
        io.github.webtransport4j.server.WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(
                io.netty.handler.codec.quic.QuicSslContextBuilder.forServer(
                        certificate.privateKey(), null, certificate.certificate())
                    .applicationProtocols("h3")
                    .build())
            .build();
    io.netty.channel.EventLoopGroup group = new io.netty.channel.nio.NioEventLoopGroup(1);
    try {
      server.start();
      server.drain();
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  RealTrafficGenerator.connect(
                      "https://127.0.0.1:" + server.getPort() + "/unregistered", null, group));
      assertTrue(failure.getMessage(), failure.getMessage().contains("rejected"));
      assertTrue(
          "Client transport threads must terminate after rejection",
          group.terminationFuture().await(5, java.util.concurrent.TimeUnit.SECONDS));
      assertTrue(group.isTerminated());
    } finally {
      group
          .shutdownGracefully(0, 50, java.util.concurrent.TimeUnit.MILLISECONDS)
          .syncUninterruptibly();
      server.close();
      certificate.delete();
    }
  }

  @Test(timeout = 10000)
  public void invalidUrlAlsoReleasesOwnedEventLoop() throws Exception {
    io.netty.channel.EventLoopGroup group = new io.netty.channel.nio.NioEventLoopGroup(1);
    try {
      assertThrows(
          java.net.URISyntaxException.class,
          () -> RealTrafficGenerator.connect("https://invalid host", null, group));
      assertTrue(group.terminationFuture().await(5, java.util.concurrent.TimeUnit.SECONDS));
    } finally {
      group
          .shutdownGracefully(0, 50, java.util.concurrent.TimeUnit.MILLISECONDS)
          .syncUninterruptibly();
    }
  }

  @Test(timeout = 20000)
  public void unansweredConnectTimesOutAndReleasesClientTransport() throws Exception {
    assertConnectTimeout(false);
  }

  @Test(timeout = 20000)
  public void informationalResponseDoesNotRejectHandshake() throws Exception {
    assertConnectTimeout(true);
  }

  private void assertConnectTimeout(boolean informationalResponse) throws Exception {
    io.netty.handler.ssl.util.SelfSignedCertificate certificate =
        new io.netty.handler.ssl.util.SelfSignedCertificate("localhost");
    io.netty.channel.EventLoopGroup serverGroup = new io.netty.channel.nio.NioEventLoopGroup(1);
    io.netty.channel.EventLoopGroup clientGroup = new io.netty.channel.nio.NioEventLoopGroup(1);
    io.netty.channel.Channel listener = null;
    java.util.concurrent.CountDownLatch requestSeen = new java.util.concurrent.CountDownLatch(1);
    try {
      io.netty.handler.codec.http3.Http3Settings settings =
          new io.netty.handler.codec.http3.Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);
      io.netty.channel.ChannelHandler codec =
          io.netty.handler.codec.http3.Http3.newQuicServerCodecBuilder()
              .sslContext(
                  io.netty.handler.codec.quic.QuicSslContextBuilder.forServer(
                          certificate.privateKey(), null, certificate.certificate())
                      .applicationProtocols("h3")
                      .build())
              .maxIdleTimeout(15000, java.util.concurrent.TimeUnit.MILLISECONDS)
              .initialMaxData(1000000)
              .initialMaxStreamDataBidirectionalRemote(100000)
              .initialMaxStreamDataUnidirectional(100000)
              .initialMaxStreamsBidirectional(10)
              .initialMaxStreamsUnidirectional(10)
              .tokenHandler(io.netty.handler.codec.quic.InsecureQuicTokenHandler.INSTANCE)
              .handler(
                  new io.netty.channel.ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel channel) {
                      channel
                          .pipeline()
                          .addLast(
                              new io.netty.handler.codec.http3.Http3ServerConnectionHandler(
                                  new io.netty.channel.ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {
                                      stream
                                          .pipeline()
                                          .addLast(
                                              new io.netty.channel.SimpleChannelInboundHandler<
                                                  Object>() {
                                                @Override
                                                protected void channelRead0(
                                                    ChannelHandlerContext ctx, Object message) {
                                                  if (message
                                                      instanceof
                                                      io.netty.handler.codec.http3
                                                          .Http3HeadersFrame) {
                                                    requestSeen.countDown();
                                                    if (informationalResponse) {
                                                      ctx.writeAndFlush(
                                                          new io.netty.handler.codec.http3
                                                              .DefaultHttp3HeadersFrame(
                                                              new io.netty.handler.codec.http3
                                                                      .DefaultHttp3Headers()
                                                                  .status("100")));
                                                    }
                                                  }
                                                }
                                              });
                                    }
                                  },
                                  null,
                                  null,
                                  new io.netty.handler.codec.http3.DefaultHttp3SettingsFrame(
                                      settings),
                                  false));
                    }
                  })
              .build();
      listener =
          new io.netty.bootstrap.Bootstrap()
              .group(serverGroup)
              .channel(io.netty.channel.socket.nio.NioDatagramChannel.class)
              .handler(codec)
              .bind("127.0.0.1", 0)
              .sync()
              .channel();
      int port = ((java.net.InetSocketAddress) listener.localAddress()).getPort();
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  RealTrafficGenerator.connect(
                      "https://127.0.0.1:" + port + "/silent", null, clientGroup));
      assertEquals(0, requestSeen.getCount());
      assertTrue(failure.getMessage(), failure.getMessage().contains("Timeout establishing"));
      assertTrue(clientGroup.terminationFuture().await(5, java.util.concurrent.TimeUnit.SECONDS));
      assertTrue(clientGroup.isTerminated());
    } finally {
      if (listener != null) {
        listener.close().syncUninterruptibly();
      }
      clientGroup
          .shutdownGracefully(0, 50, java.util.concurrent.TimeUnit.MILLISECONDS)
          .syncUninterruptibly();
      serverGroup
          .shutdownGracefully(0, 50, java.util.concurrent.TimeUnit.MILLISECONDS)
          .syncUninterruptibly();
      certificate.delete();
    }
  }

  private QuicChannel respondingChannel(boolean everyStream) throws Exception {
    QuicChannel quic = mock(QuicChannel.class);
    AtomicInteger sequence = new AtomicInteger();
    when(quic.createStream(any(), any()))
        .thenAnswer(
            invocation -> {
              final int index = sequence.getAndIncrement();
              QuicStreamChannel stream = mock(QuicStreamChannel.class);
              ChannelPipeline pipeline = mock(ChannelPipeline.class);
              AtomicReference<ChannelInboundHandlerAdapter> handler = new AtomicReference<>();
              when(stream.pipeline()).thenReturn(pipeline);
              when(stream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
              when(pipeline.addLast(any(ChannelHandler[].class)))
                  .thenAnswer(
                      add -> {
                        handler.set(add.getArgument(0));
                        return pipeline;
                      });
              Object initializer = invocation.getArgument(1);
              Method initialize =
                  initializer.getClass().getDeclaredMethod("initChannel", QuicStreamChannel.class);
              initialize.setAccessible(true);
              initialize.invoke(initializer, stream);
              when(stream.writeAndFlush(any()))
                  .thenAnswer(
                      write -> {
                        ((ByteBuf) write.getArgument(0)).release();
                        if (everyStream || index == 0) {
                          for (int chunk = 0; chunk < 3; chunk++) {
                            handler
                                .get()
                                .channelRead(
                                    mock(ChannelHandlerContext.class),
                                    Unpooled.wrappedBuffer(new byte[] {1}));
                          }
                        }
                        return new io.netty.channel.DefaultChannelPromise(
                                stream, ImmediateEventExecutor.INSTANCE)
                            .setSuccess();
                      });
              return new DefaultPromise<QuicStreamChannel>(ImmediateEventExecutor.INSTANCE)
                  .setSuccess(stream);
            });
    return quic;
  }
}
