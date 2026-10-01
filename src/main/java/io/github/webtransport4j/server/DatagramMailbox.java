package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.netty.channel.Channel;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sequential per-connection datagram dispatcher with batch draining and overload drop protection.
 *
 * <p>Unreliable datagrams are enqueued up to a bounded capacity. When overloaded or rejected,
 * datagrams are discarded with metric recording rather than aborting the underlying QUIC connection.
 */
public final class DatagramMailbox implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DatagramMailbox.class);

  /** Dispatcher callback for invoking handler methods. */
  @FunctionalInterface
  public interface FrameDispatcher {
    void dispatch(@NonNull Channel channel, long sessionId, @NonNull WebTransportFrame frame)
        throws Exception;
  }

  private final Object lock = new Object();
  private final Channel channel;
  private final Queue<WebTransportFrame> queue = new ArrayDeque<>();
  private final ExecutorService executor;
  private final FrameDispatcher dispatcher;
  private final int maxCapacity;
  private final int maxBatchSize;

  // Guarded by lock. processing means a worker is scheduled or actively draining.
  private boolean processing;
  private boolean closed;

  /**
   * Constructs a datagram mailbox for a connection channel.
   *
   * @param channel the connection channel
   * @param executor the business executor
   * @param dispatcher the frame dispatcher
   */
  public DatagramMailbox(
      @NonNull Channel channel,
      @NonNull ExecutorService executor,
      @NonNull FrameDispatcher dispatcher) {
    this.channel = Objects.requireNonNull(channel, "channel must not be null");
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
    this.maxCapacity =
        WebTransportConfig.getInt("webtransport4j.datagram.mailbox.capacity", 1024);
    this.maxBatchSize =
        WebTransportConfig.getInt("webtransport4j.datagram.mailbox.batch_size", 64);
    if (maxCapacity < 1 || maxBatchSize < 1) {
      throw new IllegalArgumentException("capacity and batch size must be positive");
    }
    if (channel.closeFuture() != null) {
      channel.closeFuture().addListener(future -> drainAndRelease("channel_closed"));
    }
  }

  /**
   * Enqueues a datagram frame for sequential batch processing.
   *
   * <p>If the queue is full or the mailbox is closed, the frame is discarded without retaining it,
   * firing discard metrics. The caller retains ownership of its original reference.
   *
   * @param frame the datagram frame to enqueue
   */
  public void enqueue(@NonNull WebTransportFrame frame) {
    boolean schedule;
    synchronized (lock) {
      if (closed) {
        discardFrame(frame, "mailbox_closed");
        return;
      }
      if (queue.size() >= maxCapacity) {
        discardFrame(frame, "mailbox_full");
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

    if (schedule) {
      try {
        executor.execute(this);
      } catch (RejectedExecutionException failure) {
        failMailbox("Datagram executor rejected work", failure);
      } catch (RuntimeException | Error failure) {
        failMailbox("Unable to schedule datagram work", failure);
      }
    }
  }

  /** Prevents new publication and releases all queued frames. */
  public void drainAndRelease() {
    drainAndRelease("mailbox_drained");
  }

  /**
   * Prevents new publication, records metrics, and releases all queued frames.
   *
   * @param reason the reason for draining
   */
  public void drainAndRelease(@NonNull String reason) {
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
      WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
      if (metrics != null) {
        metrics.onDatagramDiscarded(frame.sessionId(), reason);
      }
      releaseFrame(frame);
    }
  }

  @Override
  public void run() {
    for (;;) {
      int processedCount = 0;
      while (processedCount < maxBatchSize) {
        WebTransportFrame frame;
        synchronized (lock) {
          if (closed) {
            processing = false;
            return;
          }
          frame = queue.poll();
          if (frame == null) {
            processing = false;
            return;
          }
        }

        try {
          dispatcher.dispatch(channel, frame.sessionId(), frame);
        } catch (Throwable failure) {
          logger.error("Uncaught exception during datagram execution", failure);
        } finally {
          releaseFrame(frame);
        }
        processedCount++;
      }

      // Check if more frames remain after completing batch
      boolean hasMore;
      synchronized (lock) {
        if (closed) {
          processing = false;
          return;
        }
        hasMore = !queue.isEmpty();
        if (!hasMore) {
          processing = false;
          return;
        }
      }

      // Reschedule onto executor to yield fairly to other tasks
      try {
        executor.execute(this);
        return;
      } catch (RejectedExecutionException failure) {
        failMailbox("Datagram executor rejected continuation", failure);
        return;
      } catch (RuntimeException | Error failure) {
        failMailbox("Unable to reschedule datagram worker", failure);
        return;
      }
    }
  }

  private void discardFrame(@NonNull WebTransportFrame frame, @NonNull String reason) {
    WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
    if (metrics != null) {
      metrics.onDatagramDiscarded(frame.sessionId(), reason);
    }
  }

  private void releaseFrame(WebTransportFrame frame) {
    try {
      frame.release();
    } catch (Throwable failure) {
      logger.error("Error releasing datagram mailbox frame", failure);
    }
  }

  private void failMailbox(String message, Throwable failure) {
    logger.warn("{}: {}", message, failure.getMessage());
    synchronized (lock) {
      processing = false;
    }
    drainAndRelease("executor_rejected");
  }
}
