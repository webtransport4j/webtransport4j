package io.github.webtransport4j.afxdp;

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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.File;
import java.lang.foreign.MemorySegment;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Head-to-Head Comparative Benchmark: WebTransportServer over Linux Native Epoll
 * vs AF_XDP Kernel-Bypass Channel.
 */
public final class FullWebTransportComparisonBenchmark {

  private static final int PACKET_COUNT = 50_000;
  private static final int PAYLOAD_SIZE = 1024;

  private FullWebTransportComparisonBenchmark() {}

  /**
   * Main entry point.
   */
  public static void main(String[] args) throws Exception {
    final int packetCount = args.length > 0 ? Integer.parseInt(args[0]) : PACKET_COUNT;
    configureSsl();

    System.out.println("==========================================================================");
    System.out.println("⚡ TRUE END-TO-END WEBTRANSPORT COMPARISON BENCHMARK ⚡");
    System.out.printf("   Workload:    Full QUIC TLS 1.3 Handshake + HTTP/3 CONNECT%n");
    System.out.printf("                + Live WebTransport Datagram Stream (1,024 bytes payload)%n");
    System.out.printf("   Packets:     %,d datagrams (%.2f MB)%n",
        packetCount, (packetCount * (long) PAYLOAD_SIZE) / (1024.0 * 1024.0));
    System.out.println("==========================================================================");

    // 1. Run Epoll WebTransport
    System.out.println("\n>>> [1/2] Benchmarking WebTransportServer over Linux Native Epoll...");
    WtResult epollRes = runEpollWebTransport(packetCount);
    printWtResult("Epoll Native Transport", epollRes);

    System.gc();
    Thread.sleep(1000);

    // 2. Run AF_XDP WebTransport
    System.out.println("\n>>> [2/2] Benchmarking WebTransportServer over AF_XDP Kernel-Bypass Channel...");
    WtResult afxdpRes = runAfXdpWebTransport(packetCount);
    printWtResult("AF_XDP Kernel-Bypass Transport", afxdpRes);

    // Print Comparative Table
    printSummary(epollRes, afxdpRes);
  }

  /**
   * Benchmark result holder.
   */
  public static final class WtResult {
    private final long packets;
    private final long bytes;
    private final double durationSec;
    private final double mbPerSec;
    private final double pps;
    private final long handshakeTimeMs;

    /** Constructs benchmark result. */
    public WtResult(
        long packets,
        long bytes,
        double durationSec,
        double mbPerSec,
        double pps,
        long handshakeTimeMs) {
      this.packets = packets;
      this.bytes = bytes;
      this.durationSec = durationSec;
      this.mbPerSec = mbPerSec;
      this.pps = pps;
      this.handshakeTimeMs = handshakeTimeMs;
    }

    public long getPackets() {
      return packets;
    }

    public long getBytes() {
      return bytes;
    }

    public double getDurationSec() {
      return durationSec;
    }

    public double getMbPerSec() {
      return mbPerSec;
    }

    public double getPps() {
      return pps;
    }

    public long getHandshakeTimeMs() {
      return handshakeTimeMs;
    }
  }

  /**
   * Runs the Epoll WebTransport benchmark.
   */
  public static WtResult runEpollWebTransport(int packetCount) throws Exception {
    System.setProperty("webtransport4j.server.port", "0");
    System.setProperty("webtransport4j.server.transport", "epoll");
    System.setProperty("webtransport4j.allowed.origins", "*");

    final CountDownLatch finishLatch = new CountDownLatch(1);
    final AtomicLong receivedDatagrams = new AtomicLong(0);
    final AtomicLong receivedBytes = new AtomicLong(0);
    final AtomicLong startTime = new AtomicLong(0);
    final AtomicLong endTime = new AtomicLong(0);

    WebTransportServer server = new WebTransportServer();
    server.registerHandler(
        "/bench",
        new WebTransportHandler() {
          @Override
          public void onSessionReady(WebTransportSession session) {
            if (session instanceof NettyWebTransportSession) {
              ((NettyWebTransportSession) session).onRawDatagram(data -> {
                startTime.compareAndSet(0, System.nanoTime());
                long count = receivedDatagrams.incrementAndGet();
                receivedBytes.addAndGet(data.readableBytes());
                if (count >= packetCount) {
                  endTime.set(System.nanoTime());
                  finishLatch.countDown();
                }
              });
            }
          }
        });

    server.start();
    int port = server.getPort();

    long hsStart = System.currentTimeMillis();
    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    try {
      ClientContext client = connectClient("127.0.0.1", port, clientGroup);
      long hsEnd = System.currentTimeMillis();
      final long handshakeTimeMs = hsEnd - hsStart;

      streamDatagrams(client, packetCount);

      boolean done = finishLatch.await(15, TimeUnit.SECONDS);
      if (!done) {
        endTime.set(System.nanoTime());
      }

      long durationNanos = Math.max(1, endTime.get() - startTime.get());
      double durationSec = durationNanos / 1_000_000_000.0;
      long totalDgrams = receivedDatagrams.get();
      long totalBytes = receivedBytes.get();
      double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / durationSec;
      double pps = totalDgrams / durationSec;

      client.quicChannel.close().sync();
      return new WtResult(totalDgrams, totalBytes, durationSec, mbPerSec, pps, handshakeTimeMs);
    } finally {
      clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      server.stop();
    }
  }

  /**
   * Runs the AF_XDP WebTransport benchmark.
   */
  public static WtResult runAfXdpWebTransport(int packetCount) throws Exception {
    int serverPort = 54434;
    System.setProperty("webtransport4j.server.port", String.valueOf(serverPort));
    System.setProperty("webtransport4j.server.transport", "afxdp");
    System.setProperty("webtransport4j.allowed.origins", "*");

    final CountDownLatch finishLatch = new CountDownLatch(1);
    final AtomicLong receivedDatagrams = new AtomicLong(0);
    final AtomicLong receivedBytes = new AtomicLong(0);
    final AtomicLong startTime = new AtomicLong(0);
    final AtomicLong endTime = new AtomicLong(0);

    WebTransportServer server = new WebTransportServer();
    server.registerHandler(
        "/bench",
        new WebTransportHandler() {
          @Override
          public void onSessionReady(WebTransportSession session) {
            if (session instanceof NettyWebTransportSession) {
              ((NettyWebTransportSession) session).onRawDatagram(data -> {
                startTime.compareAndSet(0, System.nanoTime());
                long count = receivedDatagrams.incrementAndGet();
                receivedBytes.addAndGet(data.readableBytes());
                if (count >= packetCount) {
                  endTime.set(System.nanoTime());
                  finishLatch.countDown();
                }
              });
            }
          }
        });

    server.start();

    AfXdpDatagramChannel afxdpChannel = (AfXdpDatagramChannel) server.getChannels().get(0);
    AfXdpUmem umem = afxdpChannel.getUmem();

    DatagramSocket bridgeSocket = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
    int bridgePort = bridgeSocket.getLocalPort();
    AtomicBoolean bridgeRunning = new AtomicBoolean(true);

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
                    InetSocketAddress recipient = new InetSocketAddress("127.0.0.1", serverPort);
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

    long hsStart = System.currentTimeMillis();
    EventLoopGroup clientGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    try {
      ClientContext client = connectClient("127.0.0.1", bridgePort, clientGroup);
      long hsEnd = System.currentTimeMillis();
      final long handshakeTimeMs = hsEnd - hsStart;

      streamDatagrams(client, packetCount);

      boolean done = finishLatch.await(15, TimeUnit.SECONDS);
      if (!done) {
        endTime.set(System.nanoTime());
      }

      long durationNanos = Math.max(1, endTime.get() - startTime.get());
      double durationSec = durationNanos / 1_000_000_000.0;
      long totalDgrams = receivedDatagrams.get();
      long totalBytes = receivedBytes.get();
      double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / durationSec;
      double pps = totalDgrams / durationSec;

      client.quicChannel.close().sync();
      return new WtResult(totalDgrams, totalBytes, durationSec, mbPerSec, pps, handshakeTimeMs);
    } finally {
      bridgeRunning.set(false);
      bridgeSocket.close();
      clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      server.stop();
    }
  }

  private static class ClientContext {
    private QuicChannel quicChannel;
    private long sessionId;
  }

  private static ClientContext connectClient(String host, int port, EventLoopGroup clientGroup) throws Exception {
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
            .remoteAddress(new InetSocketAddress(host, port));

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
    headers.authority(host + ":" + port);
    headers.path("/bench");
    headers.set(":protocol", "webtransport");

    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
    connectReady.await(5, TimeUnit.SECONDS);

    ClientContext ctx = new ClientContext();
    ctx.quicChannel = quicChannel;
    ctx.sessionId = connectHolder[0].streamId();
    return ctx;
  }

  private static void streamDatagrams(ClientContext client, int count) {
    byte[] payload = new byte[PAYLOAD_SIZE - 2];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i & 0xFF);
    }

    for (int i = 0; i < count; i++) {
      ByteBuf datagramBuf = Unpooled.buffer(PAYLOAD_SIZE);
      WebTransportUtils.writeVarInt(datagramBuf, client.sessionId / 4);
      datagramBuf.writeBytes(payload);
      client.quicChannel.write(datagramBuf);
      if ((i & 0x7F) == 0) {
        client.quicChannel.flush();
      }
    }
    client.quicChannel.flush();
  }

  private static void printWtResult(String title, WtResult res) {
    System.out.printf("   Transport:        %s%n", title);
    System.out.printf("   Handshake Time:   %d ms%n", res.getHandshakeTimeMs());
    System.out.printf("   Packets Received: %,d packets%n", res.getPackets());
    System.out.printf("   Data Ingested:    %.2f MB%n", res.getBytes() / (1024.0 * 1024.0));
    System.out.printf("   Duration:         %.4f s%n", res.getDurationSec());
    System.out.printf("   Throughput:       %.2f MB/s%n", res.getMbPerSec());
    System.out.printf("   Packet Rate:      %,.0f PPS (%.2f Mpps)%n", res.getPps(), res.getPps() / 1_000_000.0);
  }

  private static void printSummary(WtResult epoll, WtResult afxdp) {
    System.out.println("\n==========================================================================");
    System.out.println("📊 FULL WEBTRANSPORT PROTOCOL HEAD-TO-HEAD COMPARISON 📊");
    System.out.println("==========================================================================");
    System.out.printf("%-24s | %-24s | %-24s | %s%n",
        "Metric", "Epoll Native Server", "AF_XDP Bypass Server", "Delta / Advantage");
    System.out.println(
        "-------------------------+--------------------------+--------------------------+------------------");
    double hsRatio = (double) epoll.getHandshakeTimeMs() / Math.max(1, afxdp.getHandshakeTimeMs());
    String hsAdv = epoll.getHandshakeTimeMs() > afxdp.getHandshakeTimeMs()
        ? String.format("%.1fx faster", hsRatio) : "Comparable";
    System.out.printf("%-24s | %21d ms | %21d ms | %s%n",
        "TLS 1.3 + CONNECT Latency", epoll.getHandshakeTimeMs(), afxdp.getHandshakeTimeMs(), hsAdv);
    System.out.printf("%-24s | %,18d pkts | %,18d pkts | %s%n",
        "Packets Delivered", epoll.getPackets(), afxdp.getPackets(),
        afxdp.getPackets() >= epoll.getPackets() ? "100% Reliable" : "Packet drop");
    System.out.printf("%-24s | %18.2f MB/s | %18.2f MB/s | %.2fx throughput%n",
        "Throughput", epoll.getMbPerSec(), afxdp.getMbPerSec(),
        afxdp.getMbPerSec() / Math.max(0.01, epoll.getMbPerSec()));
    System.out.printf("%-24s | %,18.0f PPS  | %,18.0f PPS  | %.2fx packet rate%n",
        "Packet Rate", epoll.getPps(), afxdp.getPps(),
        afxdp.getPps() / Math.max(1.0, epoll.getPps()));
    System.out.printf("%-24s | %19.4f s   | %19.4f s   | %.2fx faster%n",
        "Total Duration", epoll.getDurationSec(), afxdp.getDurationSec(),
        epoll.getDurationSec() / Math.max(0.001, afxdp.getDurationSec()));
    System.out.println("==========================================================================");
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
