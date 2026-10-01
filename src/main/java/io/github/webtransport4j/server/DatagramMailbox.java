package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.netty.channel.Channel;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
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
  private final Queue<WebTransportFrame> highPriorityQueue = new ArrayDeque<>();
  private final Queue<WebTransportFrame> normalQueue = new ArrayDeque<>();
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
   * Enqueues a normal-priority datagram frame for sequential batch processing.
   *
   * @param frame the datagram frame to enqueue
   */
  public void enqueue(@NonNull WebTransportFrame frame) {
    enqueue(frame, false);
  }

  /**
   * Enqueues a datagram frame with specified QoS priority for sequential batch processing.
   *
   * <p>When highPriority is true, this frame is queued ahead of normal datagrams. If the mailbox
   * is full, an older normal-priority frame is evicted to make room for this high-priority frame.
   *
   * @param frame the datagram frame to enqueue
   * @param highPriority true if this frame has priority QoS over normal datagrams
   */
  public void enqueue(@NonNull WebTransportFrame frame, boolean highPriority) {
    boolean schedule;
    synchronized (lock) {
      if (closed) {
        discardFrame(frame, "mailbox_closed");
        return;
      }
      final int totalQueued = highPriorityQueue.size() + normalQueue.size();
      if (totalQueued >= maxCapacity) {
        if (highPriority && !normalQueue.isEmpty()) {
          final WebTransportFrame evicted = normalQueue.poll();
          if (evicted != null) {
            discardFrame(evicted, "evicted_for_high_priority");
            releaseFrame(evicted);
          }
        } else {
          discardFrame(frame, "mailbox_full");
          return;
        }
      }
      frame.retain();
      try {
        if (highPriority) {
          highPriorityQueue.add(frame);
        } else {
          normalQueue.add(frame);
        }
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

  /**
   * Returns the total count of currently queued datagram frames.
   *
   * @return total queued frames
   */
  public int size() {
    synchronized (lock) {
      return highPriorityQueue.size() + normalQueue.size();
    }
  }

  /**
   * Returns the count of queued high-priority datagram frames.
   *
   * @return queued high priority frames
   */
  public int getHighPrioritySize() {
    synchronized (lock) {
      return highPriorityQueue.size();
    }
  }

  private @Nullable WebTransportFrame pollNextFrame() {
    final WebTransportFrame highPri = highPriorityQueue.poll();
    if (highPri != null) {
      return highPri;
    }
    return normalQueue.poll();
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
      final WebTransportFrame frame;
      synchronized (lock) {
        frame = pollNextFrame();
      }
      if (frame == null) {
        return;
      }
      final WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
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
          frame = pollNextFrame();
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
        hasMore = !highPriorityQueue.isEmpty() || !normalQueue.isEmpty();
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
