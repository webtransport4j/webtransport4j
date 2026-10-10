package io.github.webtransport4j.example.benchmark;

import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
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
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.NonNull;

/**
 * Standalone client process running in its own separate JVM to benchmark the server across all
 * three WebTransport communication primitives: Datagrams, Bidirectional Streams, and
 * Unidirectional Streams concurrently.
 */
public class ZeroGcClientMain {

  private static final String HOST = "127.0.0.1";

  /**
   * Main entry point for the external benchmark client process.
   *
   * @param args args[0] = port, args[1] = operations (optional)
   * @throws Exception if connection or benchmark fails
   */
  public static void main(@NonNull String[] args) throws Exception {
    int port = 54321;
    int datagrams = 50000;
    int bidiStreams = 10;
    int msgsPerBidiStream = 2500;
    int uniStreams = 10;
    int msgsPerUniStream = 2500;

    if (args.length >= 1) {
      try {
        port = Integer.parseInt(args[0]);
      } catch (NumberFormatException ignored) {
        // Fallback to default
      }
    }
    if (args.length >= 2) {
      try {
        int ops = Integer.parseInt(args[1]);
        datagrams = ops;
        msgsPerBidiStream = Math.max(1, ops / bidiStreams);
        msgsPerUniStream = Math.max(1, ops / uniStreams);
      } catch (NumberFormatException ignored) {
        // Fallback to default
      }
    }

    int totalExpected = datagrams + (bidiStreams * msgsPerBidiStream) + (uniStreams * msgsPerUniStream);
    System.out.printf(
        ">>> Client: Connecting to %s:%d to flood %,d total messages (%,d in each primitive)...%n",
        HOST, port, totalExpected, datagrams);
    System.out.printf(
        "    ├── Datagrams:       %,d packets%n"
            + "    ├── Bi-Streams:      %d streams x %,d msgs (%,d total)%n"
            + "    └── Uni-Streams:     %d streams x %,d msgs (%,d total)%n",
        datagrams,
        bidiStreams,
        msgsPerBidiStream,
        bidiStreams * msgsPerBidiStream,
        uniStreams,
        msgsPerUniStream,
        uniStreams * msgsPerUniStream);

    EventLoopGroup clientGroup = new NioEventLoopGroup(4);
    try {
      QuicSslContext sslContext =
          QuicSslContextBuilder.forClient()
              .trustManager(InsecureTrustManagerFactory.INSTANCE)
              .applicationProtocols(Http3.supportedApplicationProtocols())
              .build();

      ChannelHandler codec =
          Http3.newQuicClientCodecBuilder()
              .sslContext(sslContext)
              .maxIdleTimeout(60000, TimeUnit.MILLISECONDS)
              .initialMaxData(10737418240L)
              .initialMaxStreamDataBidirectionalLocal(1073741824L)
              .initialMaxStreamDataBidirectionalRemote(1073741824L)
              .initialMaxStreamsBidirectional(10000)
              .initialMaxStreamsUnidirectional(10000)
              .datagram(65535, 65535)
              .build();

      Bootstrap bs = new Bootstrap();
      Channel udpChannel =
          bs.group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(codec)
              .bind(0)
              .sync()
              .channel();

      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);
      settings.put(0x2c7cf000L, 1L);
      settings.put(0x2b61L, 1073741824L);
      settings.put(0x2b62L, 10000L);
      settings.put(0x2b63L, 10000L);

      final Semaphore dgramAckSemaphore = new Semaphore(0);
      final AtomicLong dgramAcks = new AtomicLong(0);

      QuicChannel quicChannel =
          QuicChannel.newBootstrap(udpChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(
                                    ChannelHandlerContext ctx, ByteBuf msg) {
                                  long quarterStreamId =
                                      WebTransportUtils.readVariableLengthInt(msg);
                                  if (quarterStreamId != -1 && msg.isReadable()) {
                                    dgramAcks.incrementAndGet();
                                    dgramAckSemaphore.release();
                                  }
                                }
                              });

                      ch.pipeline()
                          .addLast(
                              new Http3ClientConnectionHandler(
                                  null,
                                  null,
                                  new UnknownStreamHandlerFactory(),
                                  new DefaultHttp3SettingsFrame(settings),
                                  false,
                                  (id, value) -> true));
                    }
                  })
              .remoteAddress(new InetSocketAddress(HOST, port))
              .connect()
              .get();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      long[] sessionIdHolder = new long[1];
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
                                protected void channelRead0(
                                    ChannelHandlerContext ctx, Object msg) {
                                  if (msg instanceof Http3HeadersFrame) {
                                    Http3HeadersFrame resp = (Http3HeadersFrame) msg;
                                    if ("200".equals(resp.headers().status().toString())) {
                                      sessionIdHolder[0] =
                                          ((QuicStreamChannel) ctx.channel()).streamId();
                                      handshakeLatch.countDown();
                                    }
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
      headers.path("/");
      headers.authority(HOST + ":" + port);
      headers.set(":protocol", "webtransport");
      connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
      if (!handshakeLatch.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Handshake timed out");
      }
      final long sessionId = sessionIdHolder[0];

      final byte[] dgramPayload = "PING_ZERO_GC_DGRAM_123456789".getBytes(StandardCharsets.UTF_8);
      final ByteBuf templateDgram = Unpooled.directBuffer(36);
      WebTransportUtils.writeVarInt(templateDgram, sessionId / 4);
      templateDgram.writeBytes(dgramPayload);

      final byte[] streamPayloadBytes =
          "STREAM_ZERO_GC_PAYLOAD_32_BYTES!".getBytes(StandardCharsets.UTF_8);
      final ByteBuf templateStreamPayload = Unpooled.directBuffer(streamPayloadBytes.length);
      templateStreamPayload.writeBytes(streamPayloadBytes);

      final AtomicLong bidiAcks = new AtomicLong(0);
      final AtomicLong bidiAckBytes = new AtomicLong(0);
      final AtomicLong uniSent = new AtomicLong(0);

      final List<QuicStreamChannel> bidiStreamList = new ArrayList<>();
      for (int i = 0; i < bidiStreams; i++) {
        QuicStreamChannel bidi =
            quicChannel
                .createStream(
                    QuicStreamType.BIDIRECTIONAL,
                    new ChannelInitializer<QuicStreamChannel>() {
                      @Override
                      protected void initChannel(QuicStreamChannel ch) {
                        ch.pipeline()
                            .addLast(
                                new SimpleChannelInboundHandler<ByteBuf>() {
                                  @Override
                                  protected void channelRead0(
                                      ChannelHandlerContext ctx, ByteBuf msg) {
                                    long total = bidiAckBytes.addAndGet(msg.readableBytes());
                                    bidiAcks.set(total / 32);
                                  }
                                });
                      }
                    })
                .sync()
                .getNow();

        ByteBuf hdr = Unpooled.buffer(16);
        WebTransportUtils.writeVarInt(hdr, WebTransportUtils.BI_STREAM_TYPE);
        WebTransportUtils.writeVarInt(hdr, sessionId);
        bidi.writeAndFlush(hdr).sync();
        bidiStreamList.add(bidi);
      }

      final List<QuicStreamChannel> uniStreamList = new ArrayList<>();
      for (int i = 0; i < uniStreams; i++) {
        QuicStreamChannel uni =
            quicChannel
                .createStream(
                    QuicStreamType.UNIDIRECTIONAL,
                    new ChannelInitializer<QuicStreamChannel>() {
                      @Override
                      protected void initChannel(QuicStreamChannel ch) {}
                    })
                .sync()
                .getNow();

        ByteBuf hdr = Unpooled.buffer(16);
        WebTransportUtils.writeVarInt(hdr, WebTransportUtils.UNI_STREAM_TYPE);
        WebTransportUtils.writeVarInt(hdr, sessionId);
        uni.writeAndFlush(hdr).sync();
        uniStreamList.add(uni);
      }

      System.out.println(
          ">>> Client: All streams established. Launching concurrent flood across all 3 primitives...");
      final long startTime = System.nanoTime();

      final int finalDgrams = datagrams;
      final int finalMsgsPerBidi = msgsPerBidiStream;
      final int finalMsgsPerUni = msgsPerUniStream;

      Thread dgramThread =
          new Thread(
              () -> {
                final int maxInFlight = 250;
                int waitCount = 0;
                for (int i = 0; i < finalDgrams; i++) {
                  while ((i - dgramAcks.get()) > maxInFlight && quicChannel.isOpen()) {
                    quicChannel.flush();
                    LockSupport.parkNanos(20_000);
                    if (++waitCount > 50_000) {
                      waitCount = 0;
                      break;
                    }
                  }
                  waitCount = 0;
                  while (!quicChannel.isWritable() && quicChannel.isOpen()) {
                    quicChannel.flush();
                    Thread.yield();
                  }
                  quicChannel.write(
                      templateDgram.retainedDuplicate(), quicChannel.voidPromise());
                  if ((i & 0x7F) == 0) {
                    quicChannel.flush();
                  }
                }
                quicChannel.flush();
              },
              "dgram-flooder");

      Thread bidiThread =
          new Thread(
              () -> {
                for (int m = 0; m < finalMsgsPerBidi; m++) {
                  for (QuicStreamChannel bidi : bidiStreamList) {
                    while (!bidi.isWritable() && bidi.isOpen()) {
                      bidi.flush();
                      Thread.yield();
                    }
                    bidi.write(
                        templateStreamPayload.retainedDuplicate(), bidi.voidPromise());
                  }
                  if ((m & 0x7F) == 0) {
                    for (QuicStreamChannel bidi : bidiStreamList) {
                      bidi.flush();
                    }
                    Thread.yield();
                  }
                }
                for (QuicStreamChannel bidi : bidiStreamList) {
                  bidi.flush();
                }
              },
              "bidi-flooder");

      Thread uniThread =
          new Thread(
              () -> {
                for (int m = 0; m < finalMsgsPerUni; m++) {
                  for (QuicStreamChannel uni : uniStreamList) {
                    while (!uni.isWritable() && uni.isOpen()) {
                      uni.flush();
                      Thread.yield();
                    }
                    uni.write(
                        templateStreamPayload.retainedDuplicate(), uni.voidPromise());
                    uniSent.incrementAndGet();
                  }
                  if ((m & 0x7F) == 0) {
                    for (QuicStreamChannel uni : uniStreamList) {
                      uni.flush();
                    }
                    Thread.yield();
                  }
                }
                for (QuicStreamChannel uni : uniStreamList) {
                  uni.flush();
                }
              },
              "uni-flooder");

      dgramThread.start();
      bidiThread.start();
      uniThread.start();

      dgramThread.join();
      bidiThread.join();
      uniThread.join();

      // Allow adequate drain window for datagrams and reliable streams to complete echoes
      long dgramDeadline = System.currentTimeMillis() + 15000;
      while (dgramAcks.get() < finalDgrams && System.currentTimeMillis() < dgramDeadline) {
        Thread.sleep(10);
      }

      long waitDeadline = System.currentTimeMillis() + 60000;
      long targetBidiAcks = (long) bidiStreams * finalMsgsPerBidi;
      while (bidiAcks.get() < targetBidiAcks && System.currentTimeMillis() < waitDeadline) {
        Thread.sleep(20);
      }
      Thread.sleep(1000);

      long durationNs = System.nanoTime() - startTime;
      double durationSec = durationNs / 1_000_000_000.0;
      long totalSent = finalDgrams + (bidiStreams * finalMsgsPerBidi) + uniSent.get();
      double opsSec = totalSent / durationSec;

      System.out.printf(
          ">>> Client: Flood completed in %.2f seconds (%,.1f ops/s)%n", durationSec, opsSec);
      System.out.printf(
          "    ├── Datagrams ACKs:  %,d%n"
              + "    ├── Bi-Stream ACKs:  %,d%n"
              + "    └── Uni-Stream Sent: %,d%n",
          dgramAcks.get(),
          bidiAcks.get(),
          uniSent.get());

      templateDgram.release();
      templateStreamPayload.release();

      for (QuicStreamChannel bidi : bidiStreamList) {
        bidi.close().sync();
      }
      for (QuicStreamChannel uni : uniStreamList) {
        uni.close().sync();
      }

      connectStream.close().sync();
      quicChannel.close().sync();
      udpChannel.close().sync();
      System.out.println(">>> Client: Session closed. Exiting.");
    } finally {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
  }
}
