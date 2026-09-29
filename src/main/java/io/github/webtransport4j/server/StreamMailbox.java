package io.github.webtransport4j.server;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sequential per-stream dispatcher with auto-read backpressure.
 *
 * <p>Queue publication, closure and worker ownership share one lock. Callbacks, executor submission
 * and frame release happen outside that lock. Closing drains queued frames; an in-flight frame is
 * released by its worker when the callback returns.
 */
public final class StreamMailbox implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(StreamMailbox.class);

  @FunctionalInterface
  interface FrameDispatcher {
    void dispatch(@NonNull Channel channel, long sessionId, @NonNull WebTransportFrame frame)
            throws Exception;
  }

  private final Object lock = new Object();
  private final QuicStreamChannel channel;
  private final Queue<WebTransportFrame> queue = new ArrayDeque<>();
  private final ExecutorService executor;
  private final FrameDispatcher dispatcher;
  private final long sessionId;
  private final int highWaterMark;
  private final int lowWaterMark;

  // Guarded by lock. processing means a worker is scheduled or owns the drain loop.
  private boolean processing;
  private boolean closed;
  private boolean paused;

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
    boolean schedule;
    synchronized (lock) {
      if (closed) {
        return;
      }
      frame.retain();
      try {
        queue.add(frame);
      } catch (RuntimeException | Error failure) {
        frame.release();
        throw failure;
      }
      schedule = !processing;
      if (schedule) {
        processing = true;
      }
    }

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

  /** Prevents new publication and releases all queued frames, without touching an in-flight frame. */
  public void drainAndRelease() {
    synchronized (lock) {
      closed = true;
    }
    for (;;) {
      WebTransportFrame frame;
      synchronized (lock) {
        frame = queue.poll();
      }
      if (frame == null) {
        return;
      }
      releaseFrame(frame);
    }
  }

  @Override
  public void run() {
    for (;;) {
      WebTransportFrame frame;
      synchronized (lock) {
        if (closed) {
          processing = false;
          return;
        }
        frame = queue.poll();
        if (frame == null) {
          // Relinquish ownership atomically with observing an empty queue. No later finally block
          // may clear this flag after a new worker acquires it.
          processing = false;
          return;
        }
      }

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
    boolean autoRead;
    synchronized (lock) {
      if (closed) {
        return;
      }
      int pending = queue.size();
      if (!paused && pending > highWaterMark) {
        paused = true;
      } else if (paused && pending < lowWaterMark) {
        paused = false;
      } else {
        return;
      }
      autoRead = !paused;
    }
    channel.config().setAutoRead(autoRead);
  }
}
