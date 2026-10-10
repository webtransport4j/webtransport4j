package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
import io.netty.util.ReferenceCountUtil;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bounded, loopback-only validation driver; not a remote load generator.
 */
public final class ProductionValidationMain {
  private static final AtomicLong bytes = new AtomicLong();
  private static final AtomicLong datagrams = new AtomicLong();
  private static final AtomicLong streamsOpened = new AtomicLong();
  private static final AtomicLong streamsClosed = new AtomicLong();
  private static final AtomicLong peakSessionUni = new AtomicLong();
  private static final AtomicLong errors = new AtomicLong();
  private static final AtomicLong transportUniLive = new AtomicLong();
  private static final AtomicLong transportUniPeak = new AtomicLong();
  private static final AtomicLong transportUniCreated = new AtomicLong();
  private static final AtomicLong transportUniClosed = new AtomicLong();
  private static final Set<QuicChannel> trackedConnections = ConcurrentHashMap.newKeySet();
  private static final byte[] PAYLOAD = new byte[256];

  static {
    Arrays.fill(PAYLOAD, (byte) 0x5a);
  }

  /**
   * Main entry point for the production validation driver.
   *
   * @param args command-line arguments
   * @throws Exception if an error occurs
   */
  public static void main(String[] args) throws Exception {
    if ("server".equals(args[0])) {
      serve(Boolean.parseBoolean(args[1]), Integer.parseInt(args[2]), args[3]);
      return;
    }
    try (Peer peer = new Peer(Integer.parseInt(args[1]))) {
      if ("soak".equals(args[0])) {
        peer.soak(Integer.parseInt(args[2]));
      } else if ("attack".equals(args[0])) {
        peer.attack(Integer.parseInt(args[2]), Integer.parseInt(args[3]));
      } else {
        throw new IllegalArgumentException("Expected server, soak or attack");
      }
    }
  }

  private static final class ValidationMetrics implements WebTransportMetricsListener {
    @Override
    public void onStreamOpened(long sid, long id, boolean bidi) {
      streamsOpened.incrementAndGet();
    }

    @Override
    public void onStreamClosed(long sid, long id) {
      streamsClosed.incrementAndGet();
    }
  }

  private static final class ValidationHandler implements WebTransportHandler {
    @Override
    public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
      stream.onData(
          buffer -> {
            if (!buffer.equalsBytes(PAYLOAD)) {
              for (int i = 0; i < buffer.readableBytes(); i++) {
                if (buffer.getByte(i) != (byte) 0x5a) {
                  errors.incrementAndGet();
                }
              }
            }
            bytes.addAndGet(buffer.readableBytes());
            if (stream.isBidirectional()) {
              stream.write(buffer);
            }
          });
    }

    @Override
    public void onDatagramReceived(WebTransportSession session, WebTransportBuffer buffer) {
      if (!buffer.equalsBytes(PAYLOAD)) {
        errors.incrementAndGet();
      }
      bytes.addAndGet(buffer.readableBytes());
      datagrams.incrementAndGet();
      session.sendDatagram(buffer);
    }
  }

  private static void serve(boolean reduced, int seconds, String csv) throws Exception {
    System.setProperty("webtransport4j.dev_mode", "true");
    WebTransportMetricsListener metrics = new ValidationMetrics();
    WebTransportHandler handler = new ValidationHandler();

    WebTransportServer server = new WebTransportServerBuilder()
        .host("127.0.0.1")
        .port(0)
        .enableZeroGc(reduced)
        .metricsListener(metrics)
        .defaultHandler(handler)
        .messageDispatcherSupplier(() -> new TrackingDispatcher(reduced))
        .build();

    server.start();
    System.out.println("VALIDATION_PORT=" + server.getPort());
    com.sun.management.ThreadMXBean allocation =
        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    allocation.setThreadAllocatedMemoryEnabled(true);
    Map<Long, Long> previous = new HashMap<>();
    long allocated = 0;
    long start = System.nanoTime();
    try (PrintWriter out = new PrintWriter(new FileWriter(csv))) {
      out.println("seconds,heap_used,nonheap_used,direct_used,allocated_sampled_cumulative,"
          + "gc_count,gc_ms,sessions,active_children,opened,closed,bytes,datagrams,"
          + "peak_session_uni,errors,transport_uni_live,transport_uni_peak,"
          + "transport_uni_created,transport_uni_closed,netty_direct,epoch_ms,retained_quic_uni");
      for (int i = 0; i <= seconds; i++) {
        for (long id : allocation.getAllThreadIds()) {
          long value = allocation.getThreadAllocatedBytes(id);
          if (value < 0) {
            continue;
          }
          Long old = previous.put(id, value);
          if (old != null && value >= old) {
            allocated += value - old;
          }
        }
        long active = 0;
        for (WebTransportSession apiSession : server.getActiveSessions()) {
          NettyWebTransportSession session = (NettyWebTransportSession) apiSession;
          active += session.getAllActiveWebTransportStreams().size();
          peakSessionUni.accumulateAndGet(session.getActiveClientInitiatedUni().size(), Math::max);
        }
        long direct = 0;
        long gc = 0;
        long gcMs = 0;
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
          if ("direct".equals(pool.getName())) {
            direct = pool.getMemoryUsed();
          }
        }
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
          gc += Math.max(0, bean.getCollectionCount());
          gcMs += Math.max(0, bean.getCollectionTime());
        }
        long retainedQuicUni = 0;
        for (QuicChannel quic : trackedConnections) {
          if (!quic.isOpen()) {
            continue;
          }
          retainedQuicUni += quic.eventLoop().submit(() -> {
            java.lang.reflect.Field field = quic.getClass().getDeclaredField("streams");
            field.setAccessible(true);
            io.netty.util.collection.LongObjectMap<?> map =
                (io.netty.util.collection.LongObjectMap<?>) field.get(quic);
            long count = 0;
            for (Object value : map.values()) {
              if (((QuicStreamChannel) value).type() == QuicStreamType.UNIDIRECTIONAL) {
                count++;
              }
            }
            return count;
          }).get(2, TimeUnit.SECONDS);
        }
        out.printf(Locale.ROOT,
            "%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
            (System.nanoTime() - start) / 1e9,
            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(),
            ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed(), direct,
            allocated, gc, gcMs, server.getActiveSessions().size(), active,
            streamsOpened.get(), streamsClosed.get(), bytes.get(), datagrams.get(),
            peakSessionUni.get(), errors.get(), transportUniLive.get(), transportUniPeak.get(),
            transportUniCreated.get(), transportUniClosed.get(),
            PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory(), System.currentTimeMillis(),
            retainedQuicUni);
        out.flush();
        if (i < seconds) {
          Thread.sleep(1000);
        }
      }
    } finally {
      server.stop();
    }
    if (errors.get() != 0) {
      throw new AssertionError("Server payload errors: " + errors.get());
    }
  }

  private static final class TrackingDispatcher extends DefaultMessageDispatcher {
    final boolean reduced;

    TrackingDispatcher(boolean reduced) {
      this.reduced = reduced;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
      QuicChannel quic = ctx.channel() instanceof QuicChannel
          ? (QuicChannel) ctx.channel()
          : (QuicChannel) ctx.channel().parent();
      if (trackedConnections.add(quic)) {
        quic.closeFuture().addListener(future -> trackedConnections.remove(quic));
      }
      if (ctx.channel() instanceof QuicStreamChannel
          && ((QuicStreamChannel) ctx.channel()).type() == QuicStreamType.UNIDIRECTIONAL) {
        transportUniCreated.incrementAndGet();
        transportUniPeak.accumulateAndGet(transportUniLive.incrementAndGet(), Math::max);
        ctx.channel().closeFuture().addListener(future -> {
          transportUniLive.decrementAndGet();
          transportUniClosed.incrementAndGet();
        });
      }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (reduced) {
        ZeroGcMessageDispatcher.INSTANCE.channelRead(ctx, msg);
      } else {
        super.channelRead(ctx, msg);
      }
    }
  }

  private static final ChannelHandler DISCARD = new ChannelInboundHandlerAdapter() {
    @Override
    public boolean isSharable() {
      return true;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ReferenceCountUtil.release(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ctx.close();
    }
  };

  private static final class Connection implements AutoCloseable {
    final QuicChannel quic;
    final QuicStreamChannel connect;
    final AtomicLong dgramAcks = new AtomicLong();
    final AtomicLong bidiBytes = new AtomicLong();
    final AtomicLong corrupt = new AtomicLong();

    Connection(QuicChannel quic, QuicStreamChannel connect) {
      this.quic = quic;
      this.connect = connect;
    }

    @Override
    public void close() {
      quic.close().awaitUninterruptibly(5000);
    }
  }

  private static final class Peer implements AutoCloseable {
    final EventLoopGroup group = new NioEventLoopGroup(2);
    final Channel udp;
    final int port;
    final List<Long> healthMicros = Collections.synchronizedList(new ArrayList<>());
    final AtomicLong healthFailures = new AtomicLong();

    Peer(int port) throws Exception {
      this.port = port;
      QuicSslContext ssl = QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols("h3")
          .build();
      udp = new Bootstrap().group(group).channel(NioDatagramChannel.class)
          .handler(Http3.newQuicClientCodecBuilder().sslContext(ssl)
              .maxIdleTimeout(30, TimeUnit.SECONDS)
              .initialMaxData(1L << 30)
              .initialMaxStreamDataBidirectionalLocal(1L << 24)
              .initialMaxStreamDataBidirectionalRemote(1L << 24)
              .initialMaxStreamDataUnidirectional(1L << 24)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(10000)
              .datagram(1024, 1024)
              .build())
          .bind("127.0.0.1", 0)
          .sync()
          .channel();
    }

    Connection connect(String path) throws Exception {
      Http3Settings settings = new Http3Settings((id, value) -> true);
      settings.enableConnectProtocol(true);
      settings.enableH3Datagram(true);
      settings.put(0x2c7cf000L, 1L);
      settings.put(0x2b61L, 1L << 30);
      settings.put(0x2b62L, 1000L);
      settings.put(0x2b63L, 1000L);
      AtomicReference<Connection> holder = new AtomicReference<>();
      QuicChannel quic = QuicChannel.newBootstrap(udp)
          .handler(new ChannelInitializer<QuicChannel>() {
            @Override
            protected void initChannel(QuicChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  if (WebTransportUtils.readVariableLengthInt(msg) < 0) {
                    return;
                  }
                  Connection connection = holder.get();
                  if (connection != null) {
                    if (msg.readableBytes() != PAYLOAD.length) {
                      connection.corrupt.incrementAndGet();
                    }
                    while (msg.isReadable()) {
                      if (msg.readByte() != (byte) 0x5a) {
                        connection.corrupt.incrementAndGet();
                      }
                    }
                    connection.dgramAcks.incrementAndGet();
                  }
                }
              });
              ch.pipeline().addLast(new Http3ClientConnectionHandler(
                  null,
                  null,
                  type -> DISCARD,
                  new DefaultHttp3SettingsFrame(settings),
                  false,
                  (id, value) -> true));
            }
          })
          .remoteAddress(new InetSocketAddress("127.0.0.1", port))
          .connect()
          .get(5, TimeUnit.SECONDS);

      CountDownLatch ready = new CountDownLatch(1);
      AtomicInteger status = new AtomicInteger();
      try {
        QuicStreamChannel connect = Http3.newRequestStream(quic,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                    if (msg instanceof Http3HeadersFrame) {
                      Http3Headers headers = ((Http3HeadersFrame) msg).headers();
                      status.set(Integer.parseInt(headers.status().toString()));
                      ready.countDown();
                    }
                  }

                  @Override
                  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                    ctx.close();
                  }
                });
              }
            }).get(5, TimeUnit.SECONDS);

        Http3Headers headers = new DefaultHttp3Headers()
            .method("CONNECT")
            .scheme("https")
            .authority("127.0.0.1:" + port)
            .path(path)
            .set(":protocol", "webtransport");
        connect.writeAndFlush(new DefaultHttp3HeadersFrame(headers)).sync();
        if (!ready.await(5, TimeUnit.SECONDS) || status.get() != 200) {
          throw new AssertionError("CONNECT failed: " + status.get());
        }
        Connection result = new Connection(quic, connect);
        holder.set(result);
        return result;
      } catch (Exception | AssertionError failure) {
        quic.close();
        throw failure;
      }
    }

    QuicStreamChannel child(Connection connection, boolean bidi, boolean payload) throws Exception {
      ChannelHandler handler = !bidi ? DISCARD : new SimpleChannelInboundHandler<ByteBuf>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
          connection.bidiBytes.addAndGet(msg.readableBytes());
          while (msg.isReadable()) {
            if (msg.readByte() != (byte) 0x5a) {
              connection.corrupt.incrementAndGet();
            }
          }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
          ctx.close();
        }
      };
      ByteBuf header = Unpooled.buffer();
      WebTransportUtils.writeVarInt(header, bidi ? 0x41 : 0x54);
      WebTransportUtils.writeVarInt(header, connection.connect.streamId());
      if (payload) {
        header.writeBytes(PAYLOAD);
      }
      QuicStreamChannel stream = connection.quic.createStream(
          bidi ? QuicStreamType.BIDIRECTIONAL : QuicStreamType.UNIDIRECTIONAL, handler)
          .get(5, TimeUnit.SECONDS);
      stream.writeAndFlush(header).sync();
      return stream;
    }

    Thread health(AtomicBoolean running) {
      Thread thread = new Thread(() -> {
        while (running.get()) {
          long start = System.nanoTime();
          try (Connection ignored = connect("/health")) {
            healthMicros.add((System.nanoTime() - start) / 1000);
          } catch (Throwable failure) {
            healthFailures.incrementAndGet();
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException stop) {
            return;
          }
        }
      }, "independent-health-probe");
      thread.start();
      return thread;
    }

    void healthReport() {
      List<Long> sorted = new ArrayList<>(healthMicros);
      Collections.sort(sorted);
      long p99 = sorted.isEmpty()
          ? -1
          : sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * .99)));
      System.out.println("HEALTH successes=" + sorted.size()
          + " failures=" + healthFailures.get()
          + " p99_us=" + p99);
      if (healthFailures.get() != 0 || sorted.isEmpty()) {
        throw new AssertionError("Health failed");
      }
    }

    void soak(int seconds) throws Exception {
      AtomicBoolean running = new AtomicBoolean(true);
      Thread probe = health(running);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
      long cycles = 0;
      long sentDatagrams = 0;
      long sentBidiBytes = 0;
      long sentUniBytes = 0;
      try {
        while (System.nanoTime() < deadline) {
          try (Connection connection = connect("/soak")) {
            QuicStreamChannel bidi = child(connection, true, true);
            QuicStreamChannel uni = child(connection, false, true);
            final int messages = 200;
            for (int i = 0; i < messages; i++) {
              ByteBuf packet = Unpooled.buffer();
              WebTransportUtils.writeVarInt(packet, connection.connect.streamId() / 4);
              packet.writeBytes(PAYLOAD);
              connection.quic.writeAndFlush(packet).sync();
              bidi.writeAndFlush(Unpooled.wrappedBuffer(PAYLOAD)).sync();
              uni.writeAndFlush(Unpooled.wrappedBuffer(PAYLOAD)).sync();
              if ((i & 15) == 0) {
                Thread.sleep(1);
              }
            }
            long drain = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ((connection.dgramAcks.get() < messages
                || connection.bidiBytes.get() < (messages + 1L) * PAYLOAD.length)
                && System.nanoTime() < drain) {
              Thread.sleep(1);
            }
            if (connection.bidiBytes.get() != (messages + 1L) * PAYLOAD.length
                || connection.corrupt.get() != 0) {
              throw new AssertionError("Payload mismatch");
            }
            sentDatagrams += connection.dgramAcks.get();
            sentBidiBytes += connection.bidiBytes.get();
            sentUniBytes += (messages + 1L) * PAYLOAD.length;
            uni.shutdownOutput().sync();
            bidi.shutdownOutput().sync();
            Thread.sleep(5);
            cycles++;
          }
          if (cycles % 100 == 0) {
            System.out.println("SOAK cycles=" + cycles);
          }
        }
      } finally {
        running.set(false);
        probe.join(6000);
      }
      System.out.println("SOAK_COMPLETE seconds=" + seconds + " connections=" + cycles
          + " datagram_echoes=" + sentDatagrams + " bidi_echo_bytes=" + sentBidiBytes
          + " uni_sent_bytes=" + sentUniBytes);
      healthReport();
    }

    void attack(int children, int repeats) throws Exception {
      AtomicBoolean running = new AtomicBoolean(true);
      Thread probe = health(running);
      List<Connection> held = new ArrayList<>();
      try {
        for (int repeat = 0; repeat < repeats; repeat++) {
          Connection connection = connect("/attack");
          held.add(connection);
          List<QuicStreamChannel> streams = new ArrayList<>();
          int failedWrites = 0;
          int attempted = 0;
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
          for (int i = 0; i < children; i++) {
            if (!connection.quic.isOpen()) {
              break;
            }
            if (System.nanoTime() > deadline) {
              throw new AssertionError("Attack send deadline exceeded at " + i);
            }
            attempted++;
            try {
              streams.add(child(connection, false, false));
            } catch (Exception failure) {
              failedWrites++;
            }
          }
          if (connection.connect.isOpen()) {
            connection.connect.shutdownOutput().await(1000);
          }
          Thread.sleep(1000);
          long live = streams.stream().filter(Channel::isOpen).count();
          System.out.println("ATTACK repeat=" + repeat + " requested=" + children
              + " attempted=" + attempted + " created=" + streams.size()
              + " failed=" + failedWrites + " live=" + live
              + " connect_open=" + connection.connect.isOpen()
              + " quic_open=" + connection.quic.isOpen());
          if (children > 1000 && connection.connect.isOpen()) {
            throw new AssertionError("Over-quota Session survived");
          }
          if (children > 1000 && (connection.quic.isOpen() || attempted <= 1000)) {
            throw new AssertionError("Abusive QUIC connection survived the rejection budget");
          }
          connection.quic.writeAndFlush(Unpooled.buffer().writeByte(0)).await(1000);
        }
        Thread.sleep(5000);
      } finally {
        for (Connection connection : held) {
          connection.close();
        }
        running.set(false);
        probe.join(6000);
      }
      healthReport();
    }

    @Override
    public void close() {
      udp.close().awaitUninterruptibly(5000);
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).awaitUninterruptibly(5000);
    }
  }
}
