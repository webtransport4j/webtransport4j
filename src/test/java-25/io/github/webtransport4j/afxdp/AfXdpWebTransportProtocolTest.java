package io.github.webtransport4j.afxdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.NettyWebTransportSession;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportUtils;
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
import java.io.File;
import java.lang.foreign.MemorySegment;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * True End-to-End Protocol Verification: QUIC TLS 1.3 Handshake + HTTP/3 WebTransport CONNECT
 * + Live Datagram Echo operating over {@link AfXdpDatagramChannel}.
 */
public class AfXdpWebTransportProtocolTest {

  private static final int SERVER_PORT = 54433;

  private WebTransportServer server;
  private DatagramSocket bridgeSocket;
  private AtomicBoolean bridgeRunning;
  private EventLoopGroup clientGroup;

  /** Sets up SSL and system properties. */
  @BeforeEach
  public void setUp() {
    configureSsl();
    System.setProperty("webtransport4j.server.port", String.valueOf(SERVER_PORT));
    System.setProperty("webtransport4j.server.transport", "afxdp");
    System.setProperty("webtransport4j.allowed.origins", "*");
  }

  /** Tears down and cleans up resources. */
  @AfterEach
  public void tearDown() {
    if (bridgeRunning != null) {
      bridgeRunning.set(false);
    }
    if (bridgeSocket != null && !bridgeSocket.isClosed()) {
      bridgeSocket.close();
    }
    if (clientGroup != null) {
      clientGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
    }
    if (server != null) {
      server.stop();
    }
    System.clearProperty("webtransport4j.server.port");
    System.clearProperty("webtransport4j.server.transport");
    System.clearProperty("webtransport4j.allowed.origins");
  }

  @Test
  public void testTrueEndToEndWebTransportOverAfXdp() throws Exception {
    final CountDownLatch sessionReadyLatch = new CountDownLatch(1);
    final CountDownLatch datagramReceivedLatch = new CountDownLatch(1);
    final AtomicReference<String> receivedDatagram = new AtomicReference<>();

    server = new WebTransportServer();
    server.registerHandler(
        "/echo",
        new WebTransportHandler() {
          @Override
          public void onSessionReady(WebTransportSession session) {
            sessionReadyLatch.countDown();
            if (session instanceof NettyWebTransportSession) {
              ((NettyWebTransportSession) session)
                  .onRawDatagram(
                      data -> {
                        byte[] bytes = new byte[data.readableBytes()];
                        data.readBytes(bytes);
                        receivedDatagram.set(new String(bytes, StandardCharsets.UTF_8));
                        datagramReceivedLatch.countDown();
                      });
            }
          }
        });

    server.start();

    AfXdpDatagramChannel afxdpChannel = (AfXdpDatagramChannel) server.getChannels().get(0);
    AfXdpUmem umem = afxdpChannel.getUmem();

    // Start UDP loopback bridge
    bridgeSocket = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
    final int bridgePort = bridgeSocket.getLocalPort();
    bridgeRunning = new AtomicBoolean(true);

    // TX loop: UMEM TX ring -> UDP bridge socket -> Client
    Thread txThread =
        new Thread(
            () -> {
              while (bridgeRunning.get()) {
                AfXdpUmem.TxDescriptor desc = umem.pollTx();
                if (desc != null) {
                  try {
                    MemorySegment seg =
                        umem.getFrameSlice(desc.getFrameAddress(), desc.getFrameLength());
                    AfXdpFrameParser.ParsedDatagram parsed =
                        AfXdpFrameParser.parse(seg, desc.getFrameLength());
                    if (parsed != null) {
                      byte[] payload = new byte[parsed.getPayload().readableBytes()];
                      parsed.getPayload().readBytes(payload);
                      java.net.DatagramPacket p =
                          new java.net.DatagramPacket(
                              payload, payload.length, parsed.getRecipient());
                      bridgeSocket.send(p);
                    }
                  } catch (Exception e) {
                    if (bridgeRunning.get()) {
                      e.printStackTrace();
                    }
                  } finally {
                    umem.completeTx(desc.getFrameAddress());
                  }
                } else {
                  Thread.onSpinWait();
                }
              }
            });
    txThread.setDaemon(true);
    txThread.start();

    // RX loop: Client -> UDP bridge socket -> UMEM -> AfXdpDatagramChannel.feedRxFrame()
    Thread rxThread =
        new Thread(
            () -> {
              byte[] rxBuf = new byte[4096];
              java.net.DatagramPacket packet = new java.net.DatagramPacket(rxBuf, rxBuf.length);
              while (bridgeRunning.get()) {
                try {
                  bridgeSocket.receive(packet);
                  long frameOffset = umem.leaseFrame();
                  if (frameOffset >= 0) {
                    MemorySegment seg = umem.getFrameSlice(frameOffset, 4096);
                    ByteBuf payloadBuf =
                        Unpooled.wrappedBuffer(packet.getData(), packet.getOffset(), packet.getLength());
                    InetSocketAddress sender = (InetSocketAddress) packet.getSocketAddress();
                    InetSocketAddress recipient = new InetSocketAddress("127.0.0.1", SERVER_PORT);
                    byte[] clientMac = new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};
                    byte[] serverMac = new byte[] {0x02, 0x00, 0x00, 0x00, 0x00, 0x01};

                    int wireLen =
                        AfXdpFrameParser.writeFrame(seg, clientMac, serverMac, sender, recipient, payloadBuf);
                    afxdpChannel.feedRxFrame(frameOffset, wireLen);
                  }
                } catch (Exception e) {
                  if (!bridgeRunning.get()) {
                    break;
                  }
                }
              }
            });
    rxThread.setDaemon(true);
    rxThread.start();

    // WebTransport Client connecting over QUIC to bridgePort
    clientGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    QuicSslContext sslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    ChannelHandler codec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(10000, TimeUnit.MILLISECONDS)
            .initialMaxData(Integer.MAX_VALUE)
            .initialMaxStreamDataBidirectionalLocal(Integer.MAX_VALUE)
            .initialMaxStreamDataBidirectionalRemote(Integer.MAX_VALUE)
            .initialMaxStreamsUnidirectional(Integer.MAX_VALUE)
            .initialMaxStreamDataUnidirectional(Integer.MAX_VALUE)
            .initialMaxStreamsBidirectional(Integer.MAX_VALUE)
            .datagram(Integer.MAX_VALUE, Integer.MAX_VALUE)
            .build();

    Bootstrap bs = new Bootstrap();
    Channel clientChannel =
        bs.group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(codec)
            .bind(0)
            .sync()
            .channel();

    Http3Settings settings = new Http3Settings((id, value) -> true);
    settings.enableConnectProtocol(true);
    settings.enableH3Datagram(true);

    CountDownLatch connectReady = new CountDownLatch(1);
    QuicStreamChannel[] connectHolder = new QuicStreamChannel[1];

    QuicChannelBootstrap qcb =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                null,
                                null,
                                null,
                                new DefaultHttp3SettingsFrame(settings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", bridgePort));

    QuicChannel quicChannel = qcb.connect().get(5, TimeUnit.SECONDS);

    final QuicStreamChannel connectStream =
        Http3.newRequestStream(
                quicChannel,
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
    headers.authority("127.0.0.1:" + SERVER_PORT);
    headers.path("/echo");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();

    assertTrue(connectReady.await(5, TimeUnit.SECONDS), "Client CONNECT response timed out");
    assertTrue(sessionReadyLatch.await(5, TimeUnit.SECONDS), "Server onSessionReady timed out");

    long sessionId = connectHolder[0].streamId();

    // Stream a WebTransport datagram
    String expectedMsg = "Hello-AfXdp-True-End-To-End-Datagram!";
    ByteBuf datagramBuf = Unpooled.buffer();
    WebTransportUtils.writeVarInt(datagramBuf, sessionId / 4);
    datagramBuf.writeBytes(expectedMsg.getBytes(StandardCharsets.UTF_8));

    quicChannel.writeAndFlush(datagramBuf).sync();

    assertTrue(datagramReceivedLatch.await(5, TimeUnit.SECONDS), "Datagram receive timed out");
    assertEquals(expectedMsg, receivedDatagram.get(), "Received datagram payload must match");

    quicChannel.close().sync();
  }

  private static void configureSsl() {
    if (new File("localhost-key.pem").exists() && new File("localhost.pem").exists()) {
      System.setProperty("webtransport4j.ssl.key.path", "localhost-key.pem");
      System.setProperty("webtransport4j.ssl.cert.path", "localhost.pem");
    } else if (new File("src/test/resources/test-key.pem").exists()
        && new File("src/test/resources/test-cert.pem").exists()) {
      System.setProperty("webtransport4j.ssl.key.path", "src/test/resources/test-key.pem");
      System.setProperty("webtransport4j.ssl.cert.path", "src/test/resources/test-cert.pem");
    } else {
      System.setProperty("webtransport4j.dev_mode", "true");
    }
  }
}
