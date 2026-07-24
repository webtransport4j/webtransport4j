package io.github.webtransport4j.server;

import io.netty.channel.Channel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.NonNull;

/**
 * Mailbox implementation for WebTransport streams to guarantee sequential processing
 * and apply auto-read backpressure when executors (like ThreadPool or Virtual Threads) are used.
 */
public final class StreamMailbox implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(StreamMailbox.class);

  @FunctionalInterface
  interface FrameDispatcher {
    void dispatch(@NonNull Channel channel, long sessionId, @NonNull WebTransportFrame frame) throws Exception;
  }

  private final QuicStreamChannel channel;
  private final Queue<WebTransportFrame> queue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean processing = new AtomicBoolean(false);
  private final ExecutorService executor;
  private final FrameDispatcher dispatcher;
  private final long sessionId;
  private final int highWaterMark;
  private final int lowWaterMark;

  private volatile boolean paused = false;

  public StreamMailbox(@NonNull QuicStreamChannel channel, @NonNull ExecutorService executor,
                       @NonNull FrameDispatcher dispatcher,
                       long sessionId) {
    this.channel = Objects.requireNonNull(channel, "channel must not be null");
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
    this.sessionId = sessionId;
    this.highWaterMark = WebTransportConfig.getInt("webtransport4j.mailbox.high_water_mark", 16);
    this.lowWaterMark = WebTransportConfig.getInt("webtransport4j.mailbox.low_water_mark", 4);
  }

  private void setAutoRead(boolean value) {
    if (channel.eventLoop().inEventLoop()) {
      channel.config().setAutoRead(value);
    } else {
      channel.eventLoop().execute(() -> channel.config().setAutoRead(value));
    }
  }

  public void enqueue(@NonNull WebTransportFrame frame) {
    frame.retain();
    queue.add(frame);

    if (queue.size() > highWaterMark && !paused) {
      paused = true;
      setAutoRead(false);
    }

    if (processing.compareAndSet(false, true)) {
      try {
        executor.execute(this);
      } catch (RejectedExecutionException e) {
        processing.set(false);
        WebTransportFrame f;
        while ((f = queue.poll()) != null) {
          f.release();
        }
        channel.shutdown(WebTransportUtils.WT_SESSION_GONE, channel.newPromise());
      }
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        WebTransportFrame frame = queue.poll();
        if (frame == null) {
          processing.set(false);
          if (queue.isEmpty() || !processing.compareAndSet(false, true)) {
            break;
          }
          continue;
        }

        if (paused && queue.size() < lowWaterMark) {
          paused = false;
          setAutoRead(true);
        }

        try {
          dispatcher.dispatch(channel, sessionId, frame);
        } catch (Throwable t) {
          logger.error("Uncaught exception/error during business logic execution", t);
        } finally {
          frame.release();
        }
      }
    } catch (Throwable t) {
      processing.set(false);
      logger.error("Error in StreamMailbox run loop", t);
    }
  }
}
