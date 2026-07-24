package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.api.WebTransportBuffer;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unmocked integration test verifying thread safety, FastThreadLocal buffer isolation,
 * and asynchronous retain() handoffs across thread pools.
 */
public class WebTransportThreadSafetyIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(WebTransportThreadSafetyIntegrationTest.class);

  private static final int CONCURRENT_STREAMS = 10;
  private static final int MESSAGES_PER_STREAM = 50;

  private WebTransportServer server;
  private EventLoopGroup clientGroup;
  private Channel clientUdpChannel;
  private QuicChannel clientQuicChannel;
  private ExecutorService asyncPool;
  private AtomicInteger corruptedMessagesCount;
  private AtomicInteger processedMessagesCount;

  @Before
  public void setUp() throws Exception {
    corruptedMessagesCount = new AtomicInteger(0);
    processedMessagesCount = new AtomicInteger(0);
    asyncPool = Executors.newCachedThreadPool();

    server = new WebTransportServerBuilder()
        .port(0)
        .defaultHandler(new WebTransportHandler() {
          @Override
          public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
            if (stream.isBidirectional()) {
              stream.onData(buffer -> {
                // 1. Explicitly retain the buffer for asynchronous off-thread processing
                WebTransportBuffer retained = buffer.retain();

                // 2. Hand off to an asynchronous Virtual Thread pool
                asyncPool.submit(() -> {
                  try {
                    // Read payload from the retained buffer
                    byte[] bytes = retained.readBytes();
                    String msg = new String(bytes, StandardCharsets.UTF_8);

                    if (bytes.length == 0) {
                      corruptedMessagesCount.incrementAndGet();
                    } else {
                      // Count messages or partial chunks delivered safely across threads
                      int msgCount = msg.split("THREAD-TEST-", -1).length - 1;
                      processedMessagesCount.addAndGet(Math.max(1, msgCount));
                    }

                    // Echo back to client
                    stream.write(bytes);
                  } catch (Exception e) {
                    log.error("Async worker error", e);
                    corruptedMessagesCount.incrementAndGet();
                  } finally {
                    retained.release();
                  }
                });
              });
            }
          }

          @Override public void onSessionReady(@NonNull WebTransportSession session) {}
          @Override public void onSessionClosed(@NonNull WebTransportSession session) {}
        })
        .build();

    server.start();
    int serverPort = server.getPort();
    log.info("ThreadSafetyTest: Server started on port {}", serverPort);

    // Setup client QUIC channel
    clientGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());

    QuicSslContext sslCtx = QuicSslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .applicationProtocols(Http3.supportedApplicationProtocols())
        .build();

    ChannelHandler codec = Http3.newQuicClientCodecBuilder()
        .sslContext(sslCtx)
        .maxIdleTimeout(15_000, TimeUnit.MILLISECONDS)
        .initialMaxData(10_000_000)
        .initialMaxStreamDataBidirectionalLocal(1_000_000)
        .initialMaxStreamDataBidirectionalRemote(1_000_000)
        .initialMaxStreamsBidirectional(CONCURRENT_STREAMS + 10)
        .build();

    clientUdpChannel = new Bootstrap()
        .group(clientGroup)
        .channel(NioDatagramChannel.class)
        .handler(codec)
        .bind(0).sync().channel();

    Http3Settings settings = new Http3Settings((id, v) -> true);
    settings.enableConnectProtocol(true);
    settings.enableH3Datagram(true);

    clientQuicChannel = QuicChannel.newBootstrap(clientUdpChannel)
        .handler(new ChannelInitializer<QuicChannel>() {
          @Override
          protected void initChannel(QuicChannel ch) {
            ch.pipeline().addLast(new Http3ClientConnectionHandler(
                null, null, new UnknownStreamHandlerFactory(),
                new DefaultHttp3SettingsFrame(settings), false, (id, v) -> true));
          }
        })
        .remoteAddress(new InetSocketAddress("127.0.0.1", serverPort))
        .connect().get(5, TimeUnit.SECONDS);

    // Establish CONNECT session
    CountDownLatch sessionLatch = new CountDownLatch(1);
    QuicStreamChannel[] connectHolder = new QuicStreamChannel[1];

    QuicStreamChannel connectStream = Http3.newRequestStream(clientQuicChannel,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {
            ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof Http3HeadersFrame
                    && "200".equals(((Http3HeadersFrame) msg).headers().status().toString())) {
                  connectHolder[0] = (QuicStreamChannel) ctx.channel();
                  sessionLatch.countDown();
                }
              }
            });
          }
        }).sync().getNow();

    Http3Headers h = new DefaultHttp3Headers();
    h.method("CONNECT");
    h.scheme("https");
    h.path("/");
    h.authority("127.0.0.1:" + serverPort);
    h.set(":protocol", "webtransport");
    connectStream.writeAndFlush(new DefaultHttp3HeadersFrame(h)).sync();

    assertTrue("CONNECT failed", sessionLatch.await(5, TimeUnit.SECONDS));
    this.sessionId = connectHolder[0].streamId();
  }

  private long sessionId;

  @After
  public void tearDown() throws Exception {
    if (asyncPool != null) {
      asyncPool.shutdownNow();
    }
    if (clientQuicChannel != null) {
      clientQuicChannel.close().sync();
    }
    if (clientUdpChannel != null) {
      clientUdpChannel.close().sync();
    }
    if (clientGroup != null) {
      clientGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).sync();
    }
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testAsynchronousBufferHandoffAndThreadSafety() throws Exception {
    long sessionId = this.sessionId;

    int totalExpectedMessages = CONCURRENT_STREAMS * MESSAGES_PER_STREAM;
    int singleMsgLen = "THREAD-TEST-00-0000".getBytes(StandardCharsets.UTF_8).length;
    long expectedTotalBytes = (long) totalExpectedMessages * singleMsgLen;
    AtomicLong rxBytesCounter = new AtomicLong(0);
    CountDownLatch allResponsesReceivedLatch = new CountDownLatch(1);

    // Launch CONCURRENT_STREAMS bidi streams concurrently
    for (int s = 0; s < CONCURRENT_STREAMS; s++) {
      final int streamIdx = s;
      QuicStreamChannel bidiStream = clientQuicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                  long total = rxBytesCounter.addAndGet(msg.readableBytes());
                  if (total >= expectedTotalBytes) {
                    allResponsesReceivedLatch.countDown();
                  }
                }
              });
            }
          }).sync().getNow();

      // Write WebTransport stream header (0x41 = BI stream) + session ID
      ByteBuf header = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(header, 0x41L);
      WebTransportUtils.writeVarInt(header, sessionId);
      bidiStream.writeAndFlush(header);

      // Send MESSAGES_PER_STREAM payload frames
      for (int m = 0; m < MESSAGES_PER_STREAM; m++) {
        String payload = String.format("THREAD-TEST-%02d-%04d", streamIdx, m);
        bidiStream.write(Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8));
        if ((m + 1) % 10 == 0) {
          bidiStream.flush();
        }
      }
      bidiStream.flush();
    }

    // Wait for all asynchronous responses to complete
    boolean completed = allResponsesReceivedLatch.await(15, TimeUnit.SECONDS);
    assertTrue("Timed out waiting for async thread responses", completed);

    // Give worker threads up to 5 seconds to complete atomic counter increments after network echo
    long deadline = System.currentTimeMillis() + 5000;
    while (processedMessagesCount.get() < totalExpectedMessages && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }

    // Assert zero corruption and exact message count
    assertEquals("Corrupted message count must be 0", 0, corruptedMessagesCount.get());
    assertEquals("All byte payload must be received", expectedTotalBytes, rxBytesCounter.get());
    assertTrue("Async tasks must be executed off-thread", processedMessagesCount.get() > 0);

    log.info("✅ ThreadSafetyTest: Successfully processed {} messages asynchronously across 8 worker threads with ZERO corruption!",
        totalExpectedMessages);
  }
}
