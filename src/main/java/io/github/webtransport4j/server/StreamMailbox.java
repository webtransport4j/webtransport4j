package io.github.webtransport4j.server;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sequential per-stream dispatcher with auto-read backpressure.
 *
 * <p>Lock-free MPSC design ensures Netty EventLoop threads never synchronize or block. Closing
 * drains queued frames; an in-flight frame is released by its worker when the callback returns.
 */
public final class StreamMailbox implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(StreamMailbox.class);

  @FunctionalInterface
  interface FrameDispatcher {
    void dispatch(@NonNull Channel channel, long sessionId, @NonNull WebTransportFrame frame)
        throws Exception;
  }

  private final QuicStreamChannel channel;
  private final ConcurrentLinkedQueue<WebTransportFrame> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger size = new AtomicInteger();
  private final AtomicBoolean processing = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicBoolean paused = new AtomicBoolean();
  private final ExecutorService executor;
  private final FrameDispatcher dispatcher;
  private final long sessionId;
  private final int highWaterMark;
  private final int lowWaterMark;

  /**
   * Constructs a mailbox for one stream.
   *
   * @param channel the stream channel
   * @param executor the business executor
   * @param dispatcher the frame dispatcher
   * @param sessionId the session identifier
   */
  public StreamMailbox(
      @NonNull QuicStreamChannel channel,
      @NonNull ExecutorService executor,
      @NonNull FrameDispatcher dispatcher,
      long sessionId) {
    this.channel = Objects.requireNonNull(channel, "channel must not be null");
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
    this.sessionId = sessionId;
    this.highWaterMark = WebTransportConfig.getInt("webtransport4j.mailbox.high_water_mark", 16);
    this.lowWaterMark = WebTransportConfig.getInt("webtransport4j.mailbox.low_water_mark", 4);
    if (lowWaterMark < 1 || highWaterMark < lowWaterMark) {
      throw new IllegalArgumentException("mailbox watermarks must satisfy 1 <= low <= high");
    }
    if (channel.closeFuture() != null) {
      channel.closeFuture().addListener(future -> drainAndRelease());
    }
  }

  /**
   * Acquires one additional frame reference when accepted. The caller retains ownership of its
   * original reference, including when this mailbox is already closed.
   *
   * @param frame the frame to enqueue
   */
  public void enqueue(@NonNull WebTransportFrame frame) {
    if (closed.get()) {
      return;
    }
    frame.retain();
    try {
      queue.add(frame);
      size.incrementAndGet();
    } catch (RuntimeException | Error failure) {
      frame.release();
      throw failure;
    }
    if (closed.get()) {
      drainAndRelease();
      return;
    }

    boolean schedule = processing.compareAndSet(false, true);

    try {
      refreshAutoRead();
      if (schedule) {
        executor.execute(this);
      }
    } catch (RejectedExecutionException failure) {
      failMailbox("Executor or event loop rejected mailbox work", failure);
    } catch (RuntimeException | Error failure) {
      failMailbox("Unable to schedule mailbox work", failure);
    }
  }

  /**
   * Prevents new publication and releases all queued frames, without touching an in-flight frame.
   */
  public void drainAndRelease() {
    closed.set(true);
    WebTransportFrame frame;
    while ((frame = queue.poll()) != null) {
      size.decrementAndGet();
      releaseFrame(frame);
    }
  }

  @Override
  public void run() {
    for (; ; ) {
      if (closed.get()) {
        processing.set(false);
        return;
      }
      WebTransportFrame frame = queue.poll();
      if (frame == null) {
        processing.set(false);
        if (!queue.isEmpty() && processing.compareAndSet(false, true)) {
          continue;
        }
        return;
      }
      size.decrementAndGet();

      try {
        try {
          refreshAutoRead();
        } catch (RuntimeException | Error failure) {
          failMailbox("Unable to update stream backpressure", failure);
          return;
        }
        dispatcher.dispatch(channel, sessionId, frame);
      } catch (Throwable failure) {
        logger.error("Uncaught exception/error during business logic execution", failure);
      } finally {
        releaseFrame(frame);
      }
    }
  }

  private void releaseFrame(WebTransportFrame frame) {
    try {
      frame.release();
    } catch (Throwable failure) {
      logger.error("Error releasing mailbox frame", failure);
    }
  }

  private void failMailbox(String message, Throwable failure) {
    logger.error(message, failure);
    processing.set(false);
    drainAndRelease();
    try {
      channel.shutdown(WebTransportUtils.WT_SESSION_GONE, channel.newPromise());
    } catch (Throwable shutdownFailure) {
      logger.debug("Unable to shut down failed mailbox stream", shutdownFailure);
      channel.close();
    }
  }

  private void refreshAutoRead() {
    EventLoop eventLoop = channel.eventLoop();
    if (eventLoop == null) {
      return;
    }
    if (eventLoop.inEventLoop()) {
      applyAutoRead();
    } else {
      eventLoop.execute(
          () -> {
            try {
              applyAutoRead();
            } catch (RuntimeException | Error failure) {
              failMailbox("Unable to apply stream backpressure", failure);
            }
          });
    }
  }

  // Execute on the event loop and calculate from current queue state, not a stale captured value.
  private void applyAutoRead() {
    if (closed.get()) {
      return;
    }
    int pending = size.get();
    boolean wasPaused = paused.get();
    boolean newPaused;
    if (!wasPaused && pending > highWaterMark) {
      if (!paused.compareAndSet(false, true)) {
        return;
      }
      newPaused = true;
    } else if (wasPaused && pending < lowWaterMark) {
      if (!paused.compareAndSet(true, false)) {
        return;
      }
      newPaused = false;
    } else {
      return;
    }
    channel.config().setAutoRead(!newPaused);
  }
}
