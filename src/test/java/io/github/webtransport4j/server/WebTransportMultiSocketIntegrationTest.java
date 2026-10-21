package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end integration test validating multi-socket sharding via SO_REUSEPORT
 * and QuicCodecDispatcher packet routing.
 */
public class WebTransportMultiSocketIntegrationTest {

  private static final Logger log =
      LoggerFactory.getLogger(WebTransportMultiSocketIntegrationTest.class);

  private WebTransportServer server;
  private EventLoopGroup clientGroup;
  private Channel clientUdpChannel;
  private QuicChannel clientQuicChannel;
  private CountDownLatch serverDatagramReceivedLatch;
  private CountDownLatch clientDatagramReceivedLatch;
  private String lastReceivedServerDatagram;
  private String lastReceivedClientDatagram;

  /**
   * Sets up multi-socket test server before each test.
   *
   * @throws Exception if setup fails
   */
  @BeforeEach
  public void setUp() throws Exception {
    serverDatagramReceivedLatch = new CountDownLatch(1);
    clientDatagramReceivedLatch = new CountDownLatch(1);

    server =
        new WebTransportServerBuilder()
            .port(0)
            .socketCount(2)
            .defaultHandler(
                new WebTransportHandler() {
                  @Override
                  public void onSessionReady(@NonNull WebTransportSession session) {
                    log.info(
                        "MultiSocketTest: Server session ready: {}",
                        session.getSessionStreamId());
                  }

                  @Override
                  public void onDatagramReceived(
                      @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
                    byte[] bytes = data.readBytes();
                    lastReceivedServerDatagram = new String(bytes, StandardCharsets.UTF_8);
                    log.info(
                        "MultiSocketTest: Server received datagram: {}",
                        lastReceivedServerDatagram);
                    serverDatagramReceivedLatch.countDown();

                    // Echo datagram back to client
                    byte[] reply =
                        ("MULTI_ECHO:" + lastReceivedServerDatagram).getBytes(StandardCharsets.UTF_8);
                    session.sendDatagram(reply);
                  }
                })
            .build();

    server.start();
    log.info(
        "MultiSocketTest: Server started on port {} with {} active socket(s)",
        server.getPort(),
        server.getChannels().size());
  }

  /**
   * Shuts down channels and loops after test.
   *
   * @throws Exception if teardown fails
   */
  @AfterEach
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
  public void testMultiSocketDatagramExchange() throws Exception {
    assertTrue(server.getChannels().size() >= 1, "Server should have at least 1 bound socket");
    assertTrue(server.isStarted(), "Server should be started");

    clientGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols("h3")
            .build();

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(10, TimeUnit.SECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Bootstrap cb = new Bootstrap();
    clientUdpChannel =
        cb.group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    Http3Settings clientSettings = new Http3Settings((id, val) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L);

    QuicChannelBootstrap qcb =
        QuicChannel.newBootstrap(clientUdpChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<ByteBuf>() {
                              @Override
                              protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                long quarterStreamId =
                                    WebTransportUtils.readVariableLengthInt(msg);
                                if (quarterStreamId != -1 && msg.isReadable()) {
                                  byte[] payload = new byte[msg.readableBytes()];
                                  msg.readBytes(payload);
                                  lastReceivedClientDatagram =
                                      new String(payload, StandardCharsets.UTF_8);
                                  log.info(
                                      "MultiSocketTest: Client received datagram: {}",
                                      lastReceivedClientDatagram);
                                  clientDatagramReceivedLatch.countDown();
                                }
                              }
                            });
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                null,
                                null,
                                new UnknownStreamHandlerFactory(),
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", server.getPort()));

    clientQuicChannel = qcb.connect().get(5, TimeUnit.SECONDS);
    assertTrue(clientQuicChannel.isActive(), "Client QUIC connection should be active");

    CountDownLatch connectReady = new CountDownLatch(1);
    QuicStreamChannel[] connectHolder = new QuicStreamChannel[1];

    final QuicStreamChannel connectStream =
        Http3.newRequestStream(
                clientQuicChannel,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new SimpleChannelInboundHandler<Object>() {
                              @Override
                              protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                if (msg instanceof Http3HeadersFrame
                                    && "200"
                                        .equals(
                                            ((Http3HeadersFrame) msg)
                                                .headers()
                                                .status()
                                                .toString())) {
                                  connectHolder[0] = (QuicStreamChannel) ctx.channel();
                                  connectReady.countDown();
                                }
                              }
                            });
                  }
                })
            .sync()
            .getNow();

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("127.0.0.1:" + server.getPort());
    headers.path("/");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();

    assertTrue(connectReady.await(5, TimeUnit.SECONDS), "CONNECT handshake failed");

    long sessionId = connectHolder[0].streamId();

    ByteBuf datagramBuf = Unpooled.buffer();
    WebTransportUtils.writeVarInt(datagramBuf, sessionId / 4);
    String payload = "MULTI_SOCKET_HELLO";
    datagramBuf.writeBytes(payload.getBytes(StandardCharsets.UTF_8));

    clientQuicChannel.writeAndFlush(datagramBuf).sync();

    assertTrue(serverDatagramReceivedLatch.await(5, TimeUnit.SECONDS), "Server should receive datagram through multi-socket dispatcher");
    assertEquals("MULTI_SOCKET_HELLO", lastReceivedServerDatagram);

    assertTrue(clientDatagramReceivedLatch.await(5, TimeUnit.SECONDS), "Client should receive echoed datagram from server");
    assertEquals("MULTI_ECHO:MULTI_SOCKET_HELLO", lastReceivedClientDatagram);
  }
}
