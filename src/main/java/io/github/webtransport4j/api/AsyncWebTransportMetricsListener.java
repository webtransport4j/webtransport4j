package io.github.webtransport4j.api;

import io.github.webtransport4j.internal.EventLoopSafety;
import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import io.github.webtransport4j.internal.handles.LongHandle;
import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
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

  private static final IntHandle<AsyncWebTransportMetricsListener> PENDING_HANDLE =
      Handles.newIntHandle(
          AsyncWebTransportMetricsListener.class,
          "pending",
          MethodHandles.lookup(),
          () -> AtomicIntegerFieldUpdater.newUpdater(AsyncWebTransportMetricsListener.class, "pending"));

  private static final IntHandle<AsyncWebTransportMetricsListener> CLOSED_HANDLE =
      Handles.newIntHandle(
          AsyncWebTransportMetricsListener.class,
          "closed",
          MethodHandles.lookup(),
          () -> AtomicIntegerFieldUpdater.newUpdater(AsyncWebTransportMetricsListener.class, "closed"));

  private static final LongHandle<AsyncWebTransportMetricsListener> DROPPED_EVENTS_HANDLE =
      Handles.newLongHandle(
          AsyncWebTransportMetricsListener.class,
          "droppedEvents",
          MethodHandles.lookup(),
          () ->
              AtomicLongFieldUpdater.newUpdater(
                  AsyncWebTransportMetricsListener.class, "droppedEvents"));

  private final WebTransportMetricsListener delegate;
  private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
  private volatile int pending;
  private volatile int closed;
  private final int capacity;
  private final Thread worker;
  private volatile long droppedEvents;

  /** Creates an adapter with the default bounded queue capacity. */
  public AsyncWebTransportMetricsListener(@NonNull WebTransportMetricsListener delegate) {
    this(delegate, DEFAULT_QUEUE_CAPACITY);
  }

  /** Creates an adapter with a caller-supplied bounded queue capacity. */
  public AsyncWebTransportMetricsListener(
      @NonNull WebTransportMetricsListener delegate, int queueCapacity) {
    EventLoopSafety.requireBlockingAllowed();
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be positive");
    }
    capacity = queueCapacity;
    worker = new Thread(this::runWorker, "wt-metrics-exporter");
    worker.setDaemon(true);
    worker.start();
  }

  /** Returns the number of events discarded because the exporter queue was full. */
  public long droppedEvents() {
    return droppedEvents;
  }

  private void dispatch(@NonNull Runnable callback) {
    if (closed != 0) {
      return;
    }
    if (PENDING_HANDLE.incrementAndGet(this) > capacity) {
      PENDING_HANDLE.decrementAndGet(this);
      DROPPED_EVENTS_HANDLE.incrementAndGet(this);
      return;
    }
    queue.offer(callback);
    if (closed != 0 && queue.remove(callback)) {
      PENDING_HANDLE.decrementAndGet(this);
      DROPPED_EVENTS_HANDLE.incrementAndGet(this);
    }
    LockSupport.unpark(worker);
  }

  private void runWorker() {
    EventLoopSafety.requireBlockingAllowed();
    try {
      for (; ; ) {
        Runnable callback = queue.poll();
        if (callback != null) {
          PENDING_HANDLE.decrementAndGet(this);
          try {
            callback.run();
          } catch (RuntimeException failure) {
            logger.warn("Metrics exporter callback failed", failure);
          }
        } else if (closed != 0) {
          return;
        } else {
          LockSupport.park(this);
        }
      }
    } finally {
      closed = 1;
      while (queue.poll() != null) {
        PENDING_HANDLE.decrementAndGet(this);
        DROPPED_EVENTS_HANDLE.incrementAndGet(this);
      }
    }
  }

  @Override
  public void onSessionOpened(long sessionId, @NonNull String path) {
    dispatch(() -> delegate.onSessionOpened(sessionId, path));
  }

  @Override
  public void onSessionOpened(long sessionId, long uniqueSessionId, @NonNull String path) {
    dispatch(() -> delegate.onSessionOpened(sessionId, uniqueSessionId, path));
  }

  @Override
  public void onSessionClosed(long sessionId, int closeCode) {
    dispatch(() -> delegate.onSessionClosed(sessionId, closeCode));
  }

  @Override
  public void onSessionClosed(long sessionId, long uniqueSessionId, int closeCode) {
    dispatch(() -> delegate.onSessionClosed(sessionId, uniqueSessionId, closeCode));
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
    EventLoopSafety.requireBlockingAllowed();
    closed = 1;
    LockSupport.unpark(worker);
    if (Thread.currentThread() == worker) {
      return;
    }
    try {
      worker.join(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
    if (worker.isAlive()) {
      worker.interrupt();
      while (queue.poll() != null) {
        PENDING_HANDLE.decrementAndGet(this);
      }
    }
  }
}
