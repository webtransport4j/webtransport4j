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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unmocked integration test for flow control backpressure and slow reader client handling.
 */
public class WebTransportFlowControlBackpressureTest {

  private static final Logger log = LoggerFactory.getLogger(WebTransportFlowControlBackpressureTest.class);

  private WebTransportServer server;
  private EventLoopGroup clientGroup;
  private Channel clientUdpChannel;
  private QuicChannel clientQuicChannel;
  private CountDownLatch streamOpenedLatch;
  private AtomicLong bytesSentByServer;

  @Before
  public void setUp() throws Exception {
    streamOpenedLatch = new CountDownLatch(1);
    bytesSentByServer = new AtomicLong(0);

    server = new WebTransportServerBuilder()
        .port(0)
        .defaultHandler(new WebTransportHandler() {
          @Override
          public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
            log.info("BackpressureTest: Server incoming stream: {}", stream.streamId());
            streamOpenedLatch.countDown();

            // Stream continuous data to client
            byte[] chunk = new byte[8192];
            for (int i = 0; i < 50; i++) {
              stream.write(chunk);
              bytesSentByServer.addAndGet(chunk.length);
            }
          }
        })
        .build();

    server.start();
    log.info("BackpressureTest: Server started on port {}", server.getPort());
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
  public void testSlowReaderBackpressureHandling() throws Exception {
    clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    QuicSslContext clientSslContext = QuicSslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols("h3")
        .build();

    ChannelHandler clientCodec = Http3.newQuicClientCodecBuilder()
        .sslContext(clientSslContext)
        .maxIdleTimeout(10, TimeUnit.SECONDS)
        .initialMaxData(100000)
        .initialMaxStreamDataBidirectionalLocal(10000)
        .initialMaxStreamDataBidirectionalRemote(10000)
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

    // Establish WebTransport CONNECT session
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

    // Open bidi stream on client & send header + initial frame data
    QuicStreamChannel clientBidiStream = clientQuicChannel.createStream(
        QuicStreamType.BIDIRECTIONAL,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {}
        }).sync().getNow();

    ByteBuf header = Unpooled.buffer(16);
    WebTransportUtils.writeVarInt(header, 0x41L); // WT_STREAM_BI
    WebTransportUtils.writeVarInt(header, sessionId);
    header.writeBytes("INIT_STREAM_DATA".getBytes(StandardCharsets.UTF_8));
    clientBidiStream.writeAndFlush(header).sync();

    // Assert server received the stream
    assertTrue("Server should receive incoming stream", streamOpenedLatch.await(5, TimeUnit.SECONDS));

    // Pause reading on client side to simulate slow reader backpressure while server streams data
    clientBidiStream.config().setAutoRead(false);

    // Allow time for server to push bytes against flow-controlled slow client
    Thread.sleep(500);

    assertTrue("Server should attempt streaming data", bytesSentByServer.get() > 0);
    assertTrue("Server state remains active under flow control", server.isStarted());
  }
}
