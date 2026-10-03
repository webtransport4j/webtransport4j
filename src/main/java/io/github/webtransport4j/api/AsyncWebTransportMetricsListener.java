package io.github.webtransport4j.api;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded asynchronous adapter for metric exporters.
 *
 * <p>Callbacks originate on QUIC event loops. This adapter never blocks those loops: events are
 * dropped when the bounded queue is full and exporter exceptions are contained in the worker.
 */
public final class AsyncWebTransportMetricsListener
    implements WebTransportMetricsListener, AutoCloseable {

  private static final Logger logger =
      LoggerFactory.getLogger(AsyncWebTransportMetricsListener.class);
  private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

  private final WebTransportMetricsListener delegate;
  private final ThreadPoolExecutor executor;
  private final AtomicLong droppedEvents = new AtomicLong();

  /** Creates an adapter with the default bounded queue capacity. */
  public AsyncWebTransportMetricsListener(@NonNull WebTransportMetricsListener delegate) {
    this(delegate, DEFAULT_QUEUE_CAPACITY);
  }

  /** Creates an adapter with a caller-supplied bounded queue capacity. */
  public AsyncWebTransportMetricsListener(
      @NonNull WebTransportMetricsListener delegate, int queueCapacity) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be positive");
    }
    this.executor =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            new MetricsThreadFactory(),
            (task, rejectedExecutor) -> droppedEvents.incrementAndGet());
  }

  /** Returns the number of events discarded because the exporter queue was full. */
  public long droppedEvents() {
    return droppedEvents.get();
  }

  private void dispatch(@NonNull Runnable callback) {
    if (!executor.isShutdown()) {
      executor.execute(
          () -> {
            try {
              callback.run();
            } catch (RuntimeException e) {
              logger.warn("Metrics exporter callback failed", e);
            }
          });
    }
  }

  @Override
  public void onSessionOpened(long sessionId, @NonNull String path) {
    dispatch(() -> delegate.onSessionOpened(sessionId, path));
  }

  @Override
  public void onSessionClosed(long sessionId, int closeCode) {
    dispatch(() -> delegate.onSessionClosed(sessionId, closeCode));
  }

  @Override
  public void onStreamOpened(long sessionId, long streamId, boolean bidirectional) {
    dispatch(() -> delegate.onStreamOpened(sessionId, streamId, bidirectional));
  }

  @Override
  public void onStreamClosed(long sessionId, long streamId) {
    dispatch(() -> delegate.onStreamClosed(sessionId, streamId));
  }

  @Override
  public void onDatagramSent(long sessionId, int bytes) {
    dispatch(() -> delegate.onDatagramSent(sessionId, bytes));
  }

  @Override
  public void onDatagramReceived(long sessionId, int bytes) {
    dispatch(() -> delegate.onDatagramReceived(sessionId, bytes));
  }

  @Override
  public void onDatagramDiscarded(long sessionId, @NonNull String reason) {
    dispatch(() -> delegate.onDatagramDiscarded(sessionId, reason));
  }

  @Override
  public void onConnectionMigration(
      long sessionId, @NonNull String oldAddress, @NonNull String newAddress) {
    dispatch(() -> delegate.onConnectionMigration(sessionId, oldAddress, newAddress));
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static final class MetricsThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(@NonNull Runnable runnable) {
      Thread thread = new Thread(runnable, "wt-metrics-exporter");
      thread.setDaemon(true);
      return thread;
    }
  }
}
