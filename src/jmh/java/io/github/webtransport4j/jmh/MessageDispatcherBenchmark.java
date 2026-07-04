package io.github.webtransport4j.jmh;

import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.DefaultMessageDispatcher;
import io.github.webtransport4j.server.StreamMailbox;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportFrame;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamChannelConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.*;

/** JMH benchmark for MessageDispatcher performance across execution modes. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MessageDispatcherBenchmark {

  @Param({"NETTY_EVENT_LOOP", "VIRTUAL_THREADS", "FIXED_THREAD_POOL"})
  public String executionMode;

  private DefaultMessageDispatcher dispatcher;
  private ChannelHandlerContext mockCtx;
  private QuicStreamChannel mockStream;
  private WebTransportFrame streamFrame;
  private ExecutorService executor;

  /** Sets up test fixtures. */
  @Setup
  public void setup() {
    dispatcher = new DefaultMessageDispatcher();
    mockCtx = Mockito.mock(ChannelHandlerContext.class);
    mockStream = Mockito.mock(QuicStreamChannel.class);
    QuicChannel mockParent = Mockito.mock(QuicChannel.class);

    Mockito.when(mockCtx.channel()).thenReturn(mockStream);
    Mockito.when(mockStream.parent()).thenReturn(mockParent);
    Mockito.when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    io.netty.channel.EventLoop mockEventLoop = Mockito.mock(io.netty.channel.EventLoop.class);
    Mockito.when(mockStream.eventLoop()).thenReturn(mockEventLoop);
    Mockito.doAnswer(
            invocation -> {
              Runnable r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(mockEventLoop)
        .execute(Mockito.any(Runnable.class));

    QuicStreamChannelConfig mockConfig = Mockito.mock(QuicStreamChannelConfig.class);
    Mockito.when(mockStream.config()).thenReturn(mockConfig);
    Mockito.when(mockConfig.isAutoRead()).thenReturn(true);

    // Setup Executor based on param
    if ("VIRTUAL_THREADS".equalsIgnoreCase(executionMode)) {
      try {
        java.lang.reflect.Method method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
        executor = (ExecutorService) method.invoke(null);
      } catch (Exception e) {
        executor = Executors.newCachedThreadPool();
      }
    } else if ("FIXED_THREAD_POOL".equalsIgnoreCase(executionMode)) {
      executor = Executors.newFixedThreadPool(8);
    } else {
      executor = null; // NETTY_EVENT_LOOP
    }

    // Mock WT_SESSION_MGR (default mock returns null for get(1L), bypassing cleanly)
    WebTransportSessionManager mockSessionMgr = Mockito.mock(WebTransportSessionManager.class);

    // Setup high-performance attributes
    BenchmarkAttribute<ExecutorService> execAttr = new BenchmarkAttribute<>();
    execAttr.set(executor);
    Mockito.when(mockParent.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR)).thenReturn(execAttr);

    BenchmarkAttribute<WebTransportSessionManager> sessionMgrAttr = new BenchmarkAttribute<>();
    sessionMgrAttr.set(mockSessionMgr);
    Mockito.when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(sessionMgrAttr);

    BenchmarkAttribute<StreamMailbox> mailboxAttr = new BenchmarkAttribute<>();
    Mockito.when(mockStream.attr(WebTransportAttributeKeys.STREAM_MAILBOX_KEY)).thenReturn(mailboxAttr);

    ByteBuf buf = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4, 5});
    try {
      Class<?> clazz = Class.forName("io.github.webtransport4j.server.WebTransportStreamFrame");
      java.lang.reflect.Constructor<?> constructor = clazz.getDeclaredConstructor(long.class, long.class, boolean.class, ByteBuf.class);
      constructor.setAccessible(true);
      streamFrame = (WebTransportFrame) constructor.newInstance(1L, 1L, true, buf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @TearDown
  public void tearDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Benchmark
  public void testDispatchStreamFrame() throws Exception {
    streamFrame.retain();
    dispatcher.channelRead(mockCtx, streamFrame);
  }

  private static class BenchmarkAttribute<T> implements io.netty.util.Attribute<T> {
    private T value;

    @Override
    public io.netty.util.AttributeKey<T> key() {
      return null;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public void set(T value) {
      this.value = value;
    }

    @Override
    public T getAndSet(T value) {
      T old = this.value;
      this.value = value;
      return old;
    }

    @Override
    public T setIfAbsent(T value) {
      if (this.value == null) {
        this.value = value;
        return null;
      }
      return this.value;
    }

    @Override
    public T getAndRemove() {
      T old = this.value;
      this.value = null;
      return old;
    }

    @Override
    public boolean compareAndSet(T expect, T update) {
      if (this.value == expect) {
        this.value = update;
        return true;
      }
      return false;
    }

    @Override
    public void remove() {
      this.value = null;
    }
  }
}
