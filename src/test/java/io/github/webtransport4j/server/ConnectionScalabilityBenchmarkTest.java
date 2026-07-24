package io.github.webtransport4j.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side connection benchmark for an external {@link BenchmarkServerRunner}.
 *
 * <p>Start {@code BenchmarkServerRunner} in a separate JVM, then run this test with
 * {@code -Dtarget.port=<port>}. The final configured tier remains open for
 * {@code -Dbenchmark.hold.seconds} (30 seconds by default), so the runner can report memory while
 * every connection is live. Keeping the server separate means the benchmark reports the runner's
 * server-side behavior rather than contention between client and server in one JVM.
 */
public class ConnectionScalabilityBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(ConnectionScalabilityBenchmarkTest.class);

  private static final int STREAMS_PER_CONNECTION = 4;
  private static final int MESSAGES_PER_STREAM = 50;
  private static final int PIPELINE_DEPTH = 16;
  private static final String MESSAGE_ONE = "BENCH-PING-1234567890";
  private static final String MESSAGE_TWO = "BENCH-PONG-0987654321";
  private static final byte[] MESSAGE_ONE_BYTES = MESSAGE_ONE.getBytes(StandardCharsets.UTF_8);
  private static final byte[] MESSAGE_TWO_BYTES = MESSAGE_TWO.getBytes(StandardCharsets.UTF_8);
  private static final int MESSAGE_LENGTH = MESSAGE_ONE_BYTES.length;
  private static final int[] DEFAULT_CONNECTION_TIERS = {1, 10, 100, 1_000, 10_000, 20_000};
  private static final int MAX_CLIENT_THREADS = 64;
  private static final int MAX_CLIENT_UDP_CHANNELS = 64;
  private static final long CONNECTION_TIMEOUT_SECONDS = 30;
  private static final long IDLE_TIMEOUT_SECONDS = positiveProperty(
      "benchmark.idle.timeout.seconds", 600);
  private static final long HOLD_SECONDS = nonNegativeProperty("benchmark.hold.seconds", 30);

  static {
    if (MESSAGE_LENGTH != MESSAGE_TWO_BYTES.length) {
      throw new ExceptionInInitializerError("Benchmark messages must have the same length");
    }
  }

  private String host;
  private int port;

  @Before
  public void setUp() {
    String configuredPort = System.getProperty("target.port");
    Assume.assumeTrue(
        "Start BenchmarkServerRunner and set -Dtarget.port=<port> to run this benchmark.",
        configuredPort != null && !configuredPort.trim().isEmpty());

    try {
      port = Integer.parseInt(configuredPort);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("target.port must be a valid TCP/UDP port: " + configuredPort, e);
    }
    if (port < 1 || port > 65_535) {
      throw new IllegalArgumentException("target.port must be between 1 and 65535: " + port);
    }

    host = System.getProperty("target.host", "127.0.0.1").trim();
    if (host.isEmpty()) {
      throw new IllegalArgumentException("target.host must not be empty");
    }
    logger.info("Benchmarking BenchmarkServerRunner at {}:{}", host, port);
  }

  @Test
  public void benchmarkConnectionScalability() throws Exception {
    System.out.printf("Benchmarking BenchmarkServerRunner at %s:%d%n", host, port);
    System.out.printf(
        "%8s %12s %12s %17s %17s %14s%n",
        "Connections",
        "Time (ms)",
        "Messages",
        "PING (sent/recv)",
        "PONG (sent/recv)",
        "Messages/sec");

    int[] tiers = connectionTiers();
    for (int i = 0; i < tiers.length; i++) {
      runTier(tiers[i], i == tiers.length - 1 ? HOLD_SECONDS : 0);
    }
  }

  private void runTier(int connections, long holdSeconds) throws Exception {
    int channelCount = Math.min(connections, MAX_CLIENT_UDP_CHANNELS);
    NioEventLoopGroup eventLoopGroup =
        new NioEventLoopGroup(Math.min(channelCount, MAX_CLIENT_THREADS));
    ExecutorService workers =
        Executors.newFixedThreadPool(Math.min(channelCount, MAX_CLIENT_THREADS));
    List<Throwable> failures = Collections.synchronizedList(new ArrayList<Throwable>());
    AtomicInteger successfulConnections = new AtomicInteger();
    AtomicLong receivedMessages = new AtomicLong();
    MessageCounts messageCounts = new MessageCounts();
    CountDownLatch complete = new CountDownLatch(connections);
    List<ClientDatagramChannel> udpChannels = new ArrayList<ClientDatagramChannel>();
    List<OpenConnection> openConnections =
        Collections.synchronizedList(new ArrayList<OpenConnection>());
    boolean finished = false;
    long startNanos = System.nanoTime();
    long elapsedMillis = 0;

    try {
      for (int i = 0; i < channelCount; i++) {
        udpChannels.add(bindClientChannel(eventLoopGroup));
      }
      for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
        final ClientDatagramChannel channel = udpChannels.get(channelIndex);
        final int firstConnection = channelIndex;
        workers.execute(
            () -> {
              for (int connection = firstConnection;
                  connection < connections;
                  connection += channelCount) {
                try {
                  OpenConnection openConnection = openConnection(channel.channel, messageCounts);
                  openConnections.add(openConnection);
                  receivedMessages.addAndGet(openConnection.echoedMessages);
                  successfulConnections.incrementAndGet();
                } catch (Throwable failure) {
                  failures.add(failure);
                } finally {
                  complete.countDown();
                }
              }
            });
      }
      finished = complete.await(tierTimeoutSeconds(connections), TimeUnit.SECONDS);
      elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      assertCompleteTier(
          connections,
          successfulConnections.get(),
          receivedMessages.get(),
          messageCounts,
          finished,
          failures);
      assertOpenConnections(openConnections, connections);

      if (holdSeconds > 0) {
        logger.info(
            "All {} connections are open. Holding them for {} seconds for server-side measurement.",
            connections,
            holdSeconds);
        TimeUnit.SECONDS.sleep(holdSeconds);
        assertOpenConnections(openConnections, connections);
      }
    } finally {
      closeOpenConnections(openConnections);
      workers.shutdownNow();
      awaitWorkerShutdown(workers);
      for (ClientDatagramChannel channel : udpChannels) {
        channel.close();
      }
      eventLoopGroup.shutdownGracefully(0, 3, TimeUnit.SECONDS).syncUninterruptibly();
    }

    System.out.printf(
        "%8d %12d %12d %8d/%-8d %8d/%-8d %14.0f%n",
        connections,
        elapsedMillis,
        receivedMessages.get(),
        messageCounts.sentMessageOne(),
        messageCounts.receivedMessageOne(),
        messageCounts.sentMessageTwo(),
        messageCounts.receivedMessageTwo(),
        receivedMessages.get() * 1_000.0 / Math.max(elapsedMillis, 1L));
  }

  private ClientDatagramChannel bindClientChannel(NioEventLoopGroup eventLoopGroup)
      throws InterruptedException {
    QuicSslContext sslContext = createClientSslContext();
    try {
      Channel channel = new Bootstrap()
          .group(eventLoopGroup)
          .channel(NioDatagramChannel.class)
          .handler(newClientCodec(sslContext))
          .bind(0)
          .sync()
          .channel();
      return new ClientDatagramChannel(channel, sslContext);
    } catch (InterruptedException | RuntimeException | Error failure) {
      throw failure;
    }
  }

  private ChannelHandler newClientCodec(QuicSslContext sslContext) {
    return Http3.newQuicClientCodecBuilder()
        .sslContext(sslContext)
        .maxIdleTimeout(IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .initialMaxData(100_000_000)
        .initialMaxStreamDataBidirectionalLocal(10_000_000)
        .initialMaxStreamDataBidirectionalRemote(10_000_000)
        .initialMaxStreamsBidirectional(STREAMS_PER_CONNECTION + 4)
        .initialMaxStreamsUnidirectional(STREAMS_PER_CONNECTION + 4)
        .initialMaxStreamDataUnidirectional(10_000_000)
        .build();
  }

  private OpenConnection openConnection(Channel udpChannel, MessageCounts messageCounts)
      throws Exception {
    QuicChannel quicChannel = null;
    QuicStreamChannel connectStream = null;
    List<QuicStreamChannel> dataStreams = new ArrayList<QuicStreamChannel>(STREAMS_PER_CONNECTION);
    AtomicBoolean closing = new AtomicBoolean();
    AtomicReference<Throwable> lifecycleFailure = new AtomicReference<Throwable>();
    OpenConnection openConnection = null;

    try {
      quicChannel = connect(udpChannel);
      CompletableFuture<Long> sessionId = new CompletableFuture<Long>();
      connectStream = openConnectStream(quicChannel, sessionId, lifecycleFailure, closing);
      sendConnectRequest(connectStream);

      long id = sessionId.get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      long echoedMessages =
          sendAndVerifyEchoes(
              quicChannel, id, dataStreams, messageCounts, lifecycleFailure, closing);
      openConnection = new OpenConnection(quicChannel, echoedMessages, lifecycleFailure, closing);
      return openConnection;
    } finally {
      if (openConnection == null) {
        closing.set(true);
        for (QuicStreamChannel stream : dataStreams) {
          close(stream);
        }
        close(connectStream);
        close(quicChannel);
      }
    }
  }

  private QuicChannel connect(Channel udpChannel) throws Exception {
    Http3Settings settings = new Http3Settings((id, value) -> true);
    settings.enableConnectProtocol(true);
    settings.enableH3Datagram(true);

    return QuicChannel.newBootstrap(udpChannel)
        .handler(
            new ChannelInitializer<QuicChannel>() {
              @Override
              protected void initChannel(QuicChannel channel) {
                channel.pipeline().addLast(
                    new Http3ClientConnectionHandler(
                        null,
                        null,
                        new UnknownStreamHandlerFactory(),
                        new DefaultHttp3SettingsFrame(settings),
                        false,
                        (id, value) -> true));
              }
            })
        .remoteAddress(new InetSocketAddress(host, port))
        .connect()
        .get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private QuicStreamChannel openConnectStream(
      QuicChannel quicChannel,
      CompletableFuture<Long> sessionId,
      AtomicReference<Throwable> lifecycleFailure,
      AtomicBoolean closing) throws InterruptedException {
    return Http3.newRequestStream(
            quicChannel,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel channel) {
                channel.pipeline().addLast(
                    new ConnectResponseHandler(sessionId, lifecycleFailure, closing));
              }
            })
        .sync()
        .getNow();
  }

  private void sendConnectRequest(QuicStreamChannel connectStream) throws InterruptedException {
    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.path("/bench");
    headers.authority(host + ':' + port);
    headers.set(":protocol", "webtransport");
    connectStream.writeAndFlush(new io.netty.handler.codec.http3.DefaultHttp3HeadersFrame(headers)).sync();
  }

  private long sendAndVerifyEchoes(
      QuicChannel quicChannel,
      long sessionId,
      List<QuicStreamChannel> dataStreams,
      MessageCounts messageCounts,
      AtomicReference<Throwable> lifecycleFailure,
      AtomicBoolean closing) throws Exception {
    long bytesPerStream = (long) MESSAGES_PER_STREAM * MESSAGE_LENGTH;
    CompletableFuture<Long> echoedMessages = new CompletableFuture<Long>();
    AtomicInteger remainingStreams = new AtomicInteger(STREAMS_PER_CONNECTION);

    for (int streamIndex = 0; streamIndex < STREAMS_PER_CONNECTION; streamIndex++) {
      boolean[] sentMessageOne = new boolean[MESSAGES_PER_STREAM];
      for (int message = 0; message < MESSAGES_PER_STREAM; message++) {
        sentMessageOne[message] = ThreadLocalRandom.current().nextBoolean();
      }
      QuicStreamChannel stream =
          quicChannel
              .createStream(
                  QuicStreamType.BIDIRECTIONAL,
                  new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel channel) {
                      channel.pipeline().addLast(
                          new EchoResponseHandler(
                              bytesPerStream,
                              remainingStreams,
                              echoedMessages,
                              sentMessageOne,
                              messageCounts,
                              lifecycleFailure,
                              closing));
                    }
                  })
              .sync()
              .getNow();
      dataStreams.add(stream);

      ByteBuf streamHeader = Unpooled.buffer(16);
      WebTransportUtils.writeVarInt(streamHeader, 0x41L);
      WebTransportUtils.writeVarInt(streamHeader, sessionId);
      stream.writeAndFlush(streamHeader).sync();

      for (int message = 0; message < MESSAGES_PER_STREAM; message++) {
        boolean isMessageOne = sentMessageOne[message];
        messageCounts.recordSent(isMessageOne);
        stream.write(Unpooled.copiedBuffer(isMessageOne ? MESSAGE_ONE_BYTES : MESSAGE_TWO_BYTES));
        if ((message + 1) % PIPELINE_DEPTH == 0) {
          stream.flush();
        }
      }
      stream.flush();
    }

    return echoedMessages.get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private static void assertCompleteTier(
      int connections,
      int successfulConnections,
      long receivedMessages,
      MessageCounts messageCounts,
      boolean finished,
      List<Throwable> failures) {
    Assert.assertTrue(
        "Timed out after " + tierTimeoutSeconds(connections) + " seconds; "
            + successfulConnections + "/" + connections + " connections completed",
        finished);

    if (!failures.isEmpty()) {
      AssertionError failure = new AssertionError(
          failures.size() + " of " + connections + " connections failed");
      failure.initCause(failures.get(0));
      for (int i = 1; i < failures.size(); i++) {
        failure.addSuppressed(failures.get(i));
      }
      throw failure;
    }

    Assert.assertEquals("Every connection must complete", connections, successfulConnections);
    Assert.assertEquals(
        "Every echoed message must be received",
        (long) connections * STREAMS_PER_CONNECTION * MESSAGES_PER_STREAM,
        receivedMessages);
    Assert.assertEquals(
        "Every PING message sent must be received",
        messageCounts.sentMessageOne(),
        messageCounts.receivedMessageOne());
    Assert.assertEquals(
        "Every PONG message sent must be received",
        messageCounts.sentMessageTwo(),
        messageCounts.receivedMessageTwo());
  }

  private static void assertOpenConnections(
      List<OpenConnection> openConnections, int expectedConnections) {
    List<OpenConnection> connections = new ArrayList<OpenConnection>(openConnections);
    Assert.assertEquals("Every successful connection must remain open", expectedConnections, connections.size());
    for (OpenConnection connection : connections) {
      connection.assertHealthy();
    }
  }

  private static void closeOpenConnections(List<OpenConnection> openConnections) {
    for (OpenConnection connection : new ArrayList<OpenConnection>(openConnections)) {
      connection.close();
    }
  }

  private static int[] connectionTiers() {
    String configuredTiers = System.getProperty("benchmark.connections");
    if (configuredTiers == null || configuredTiers.trim().isEmpty()) {
      return DEFAULT_CONNECTION_TIERS.clone();
    }

    String[] values = configuredTiers.split(",");
    int[] tiers = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      try {
        tiers[i] = Integer.parseInt(values[i].trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("benchmark.connections must be comma-separated integers", e);
      }
      if (tiers[i] < 1) {
        throw new IllegalArgumentException("benchmark.connections values must be positive: " + tiers[i]);
      }
    }
    return tiers;
  }

  private static long tierTimeoutSeconds(int connections) {
    return Math.min(300L, Math.max(60L, 30L + connections / 25));
  }

  private static int positiveProperty(String property, int defaultValue) {
    int value = Integer.getInteger(property, defaultValue);
    if (value < 1) {
      throw new IllegalArgumentException(property + " must be positive: " + value);
    }
    return value;
  }

  private static int nonNegativeProperty(String property, int defaultValue) {
    int value = Integer.getInteger(property, defaultValue);
    if (value < 0) {
      throw new IllegalArgumentException(property + " must not be negative: " + value);
    }
    return value;
  }

  private static QuicSslContext createClientSslContext() {
    try {
      return QuicSslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static void awaitWorkerShutdown(ExecutorService workers) {
    try {
      if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.warn("Benchmark worker threads did not stop within 5 seconds");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void close(Channel channel) {
    if (channel != null && channel.isOpen()) {
      channel.close().syncUninterruptibly();
    }
  }

  private static final class ClientDatagramChannel {
    private final Channel channel;
    private final QuicSslContext sslContext;

    private ClientDatagramChannel(Channel channel, QuicSslContext sslContext) {
      this.channel = channel;
      this.sslContext = sslContext;
    }

    private void close() {
      ConnectionScalabilityBenchmarkTest.close(channel);
    }
  }

  private static final class MessageCounts {
    private final AtomicLong sentMessageOne = new AtomicLong();
    private final AtomicLong sentMessageTwo = new AtomicLong();
    private final AtomicLong receivedMessageOne = new AtomicLong();
    private final AtomicLong receivedMessageTwo = new AtomicLong();

    private void recordSent(boolean isMessageOne) {
      (isMessageOne ? sentMessageOne : sentMessageTwo).incrementAndGet();
    }

    private void recordReceived(boolean isMessageOne) {
      (isMessageOne ? receivedMessageOne : receivedMessageTwo).incrementAndGet();
    }

    private long sentMessageOne() {
      return sentMessageOne.get();
    }

    private long sentMessageTwo() {
      return sentMessageTwo.get();
    }

    private long receivedMessageOne() {
      return receivedMessageOne.get();
    }

    private long receivedMessageTwo() {
      return receivedMessageTwo.get();
    }
  }

  private static final class OpenConnection {
    private final QuicChannel channel;
    private final long echoedMessages;
    private final AtomicReference<Throwable> lifecycleFailure;
    private final AtomicBoolean closing;

    private OpenConnection(
        QuicChannel channel,
        long echoedMessages,
        AtomicReference<Throwable> lifecycleFailure,
        AtomicBoolean closing) {
      this.channel = channel;
      this.echoedMessages = echoedMessages;
      this.lifecycleFailure = lifecycleFailure;
      this.closing = closing;
      channel.closeFuture().addListener(
          ignored -> recordFailure(lifecycleFailure, closing,
              new IllegalStateException("QUIC connection closed before benchmark release")));
    }

    private void assertHealthy() {
      Throwable failure = lifecycleFailure.get();
      if (failure != null) {
        AssertionError assertion = new AssertionError("An open connection failed during the hold period");
        assertion.initCause(failure);
        throw assertion;
      }
      Assert.assertTrue("A connection closed during the hold period", channel.isActive());
    }

    private void close() {
      closing.set(true);
      channel.close();
    }
  }

  private static void recordFailure(
      AtomicReference<Throwable> lifecycleFailure, AtomicBoolean closing, Throwable failure) {
    if (!closing.get()) {
      lifecycleFailure.compareAndSet(null, failure);
    }
  }

  private static final class ConnectResponseHandler extends SimpleChannelInboundHandler<Object> {
    private final CompletableFuture<Long> sessionId;
    private final AtomicReference<Throwable> lifecycleFailure;
    private final AtomicBoolean closing;

    private ConnectResponseHandler(
        CompletableFuture<Long> sessionId,
        AtomicReference<Throwable> lifecycleFailure,
        AtomicBoolean closing) {
      this.sessionId = sessionId;
      this.lifecycleFailure = lifecycleFailure;
      this.closing = closing;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, Object message) {
      if (!(message instanceof Http3HeadersFrame)) {
        return;
      }

      Http3HeadersFrame response = (Http3HeadersFrame) message;
      String status = response.headers().status().toString();
      if ("200".equals(status)) {
        sessionId.complete(((QuicStreamChannel) context.channel()).streamId());
      } else {
        IllegalStateException failure = new IllegalStateException("CONNECT returned HTTP " + status);
        recordFailure(lifecycleFailure, closing, failure);
        sessionId.completeExceptionally(failure);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      recordFailure(lifecycleFailure, closing, cause);
      sessionId.completeExceptionally(cause);
      context.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
      IllegalStateException failure = new IllegalStateException("CONNECT stream closed before success");
      recordFailure(lifecycleFailure, closing, failure);
      sessionId.completeExceptionally(failure);
    }
  }

  private static final class EchoResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final long expectedBytes;
    private final AtomicInteger remainingStreams;
    private final CompletableFuture<Long> echoedMessages;
    private final boolean[] expectedMessageOne;
    private final MessageCounts messageCounts;
    private final AtomicReference<Throwable> lifecycleFailure;
    private final AtomicBoolean closing;
    private int receivedMessageIndex;
    private int bytesInMessage;
    private boolean complete;

    private EchoResponseHandler(
        long expectedBytes,
        AtomicInteger remainingStreams,
        CompletableFuture<Long> echoedMessages,
        boolean[] expectedMessageOne,
        MessageCounts messageCounts,
        AtomicReference<Throwable> lifecycleFailure,
        AtomicBoolean closing) {
      this.expectedBytes = expectedBytes;
      this.remainingStreams = remainingStreams;
      this.echoedMessages = echoedMessages;
      this.expectedMessageOne = expectedMessageOne;
      this.messageCounts = messageCounts;
      this.lifecycleFailure = lifecycleFailure;
      this.closing = closing;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, ByteBuf message) {
      try {
        verifyMessages(message);
        if (receivedMessageIndex == expectedMessageOne.length && !complete) {
          complete = true;
          if (remainingStreams.decrementAndGet() == 0) {
            echoedMessages.complete((long) STREAMS_PER_CONNECTION * MESSAGES_PER_STREAM);
          }
        }
      } catch (Throwable failure) {
        recordFailure(lifecycleFailure, closing, failure);
        echoedMessages.completeExceptionally(failure);
        context.close();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      recordFailure(lifecycleFailure, closing, cause);
      echoedMessages.completeExceptionally(cause);
      context.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
      if (!complete) {
        IllegalStateException failure =
            new IllegalStateException("Echo stream closed before all messages were received");
        recordFailure(lifecycleFailure, closing, failure);
        echoedMessages.completeExceptionally(failure);
      }
    }

    private void verifyMessages(ByteBuf message) {
      int readerIndex = message.readerIndex();
      int readableBytes = message.readableBytes();
      int consumedBytes = 0;

      while (consumedBytes < readableBytes) {
        if (receivedMessageIndex >= expectedMessageOne.length) {
          throw new IllegalStateException("Received more echoed bytes than were sent");
        }

        boolean isMessageOne = expectedMessageOne[receivedMessageIndex];
        byte[] expectedPayload = isMessageOne ? MESSAGE_ONE_BYTES : MESSAGE_TWO_BYTES;
        int bytesToVerify = Math.min(MESSAGE_LENGTH - bytesInMessage, readableBytes - consumedBytes);
        for (int i = 0; i < bytesToVerify; i++) {
          byte actual = message.getByte(readerIndex + consumedBytes + i);
          byte expected = expectedPayload[bytesInMessage + i];
          if (actual != expected) {
            throw new IllegalStateException(
                "Echo payload mismatch in message " + receivedMessageIndex + " at byte "
                    + (bytesInMessage + i) + ": expected " + expected + ", received " + actual);
          }
        }

        consumedBytes += bytesToVerify;
        bytesInMessage += bytesToVerify;
        if (bytesInMessage == MESSAGE_LENGTH) {
          messageCounts.recordReceived(isMessageOne);
          receivedMessageIndex++;
          bytesInMessage = 0;
        }
      }

      if ((long) receivedMessageIndex * MESSAGE_LENGTH + bytesInMessage > expectedBytes) {
        throw new IllegalStateException("Received more echoed bytes than were sent");
      }
    }
  }

}
