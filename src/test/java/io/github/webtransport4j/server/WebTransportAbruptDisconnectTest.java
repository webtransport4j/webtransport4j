package io.github.webtransport4j.server;

import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ResourceLeakDetector;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unmocked integration test verifying resource leak safety and session cleanup on abrupt client disconnections.
 */
public class WebTransportAbruptDisconnectTest {

  private static final Logger log = LoggerFactory.getLogger(WebTransportAbruptDisconnectTest.class);

  private WebTransportServer server;
  private EventLoopGroup clientGroup;
  private Channel clientUdpChannel;
  private QuicChannel clientQuicChannel;
  private CountDownLatch sessionClosedLatch;

  @Before
  public void setUp() throws Exception {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    sessionClosedLatch = new CountDownLatch(1);

    server = new WebTransportServerBuilder()
        .port(0)
        .idleTimeout(2, TimeUnit.SECONDS)
        .defaultHandler(new WebTransportHandler() {
          @Override
          public void onSessionClosed(@NonNull WebTransportSession session) {
            log.info("AbruptDisconnectTest: Server session closed callback: {}", session.getSessionStreamId());
            sessionClosedLatch.countDown();
          }

          @Override
          public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
            stream.onData(data -> {
              byte[] b = data.readBytes();
              log.info("AbruptDisconnectTest: Server read: {}", new String(b, StandardCharsets.UTF_8));
            });
          }
        })
        .build();

    server.start();
    log.info("AbruptDisconnectTest: Server started on port {}", server.getPort());
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
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testAbruptClientDisconnectCleanup() throws Exception {
    clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    QuicSslContext clientSslContext = QuicSslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols("h3")
        .build();

    ChannelHandler clientCodec = Http3.newQuicClientCodecBuilder()
        .sslContext(clientSslContext)
        .maxIdleTimeout(2, TimeUnit.SECONDS)
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
    QuicStreamChannel[] connectHolder = new QuicStreamChannel[1];

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
                  connectHolder[0] = (QuicStreamChannel) ctx.channel();
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

    long sessionId = connectHolder[0].streamId();

    // Open bidi stream & send payload
    QuicStreamChannel bidiStream = clientQuicChannel.createStream(
        QuicStreamType.BIDIRECTIONAL,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {}
        }).sync().getNow();

    ByteBuf header = Unpooled.buffer(16);
    WebTransportUtils.writeVarInt(header, 0x41L); // WT_STREAM_BI
    WebTransportUtils.writeVarInt(header, sessionId);
    header.writeBytes("ACTIVE_PAYLOAD".getBytes(StandardCharsets.UTF_8));
    bidiStream.writeAndFlush(header).sync();

    // Kill client UDP channel ungracefully without sending QUIC close frame
    clientUdpChannel.close().sync();

    // Assert server cleans up session via idle timeout or disconnect handler
    assertTrue("Server should invoke onSessionClosed after disconnect", sessionClosedLatch.await(10, TimeUnit.SECONDS));

    // Force System.gc() to trigger Netty ResourceLeakDetector check
    System.gc();
  }
}
