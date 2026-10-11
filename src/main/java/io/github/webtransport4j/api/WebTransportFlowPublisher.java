package io.github.webtransport4j.api;

import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import io.github.webtransport4j.internal.handles.LongHandle;
import io.github.webtransport4j.internal.handles.RefHandle;
import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A standard-compliant Reactive Streams Publisher implementation. Used internally to dispatch event
 * flows (streams, datagrams) without Project Reactor compile dependencies.
 */
public class WebTransportFlowPublisher<T> implements Publisher<T> {

  @SuppressWarnings("rawtypes")
  private static final IntHandle<WebTransportFlowPublisher> DRAIN_WIP_HANDLE =
      Handles.newIntHandle(
          WebTransportFlowPublisher.class,
          "drainWip",
          MethodHandles.lookup(),
          () -> AtomicIntegerFieldUpdater.newUpdater(WebTransportFlowPublisher.class, "drainWip"));

  @SuppressWarnings("rawtypes")
  private static final LongHandle<WebTransportFlowPublisher> DEMAND_HANDLE =
      Handles.newLongHandle(
          WebTransportFlowPublisher.class,
          "demand",
          MethodHandles.lookup(),
          () -> AtomicLongFieldUpdater.newUpdater(WebTransportFlowPublisher.class, "demand"));

  @SuppressWarnings("rawtypes")
  private static final IntHandle<WebTransportFlowPublisher> CANCELLED_HANDLE =
      Handles.newIntHandle(
          WebTransportFlowPublisher.class,
          "cancelled",
          MethodHandles.lookup(),
          () -> AtomicIntegerFieldUpdater.newUpdater(WebTransportFlowPublisher.class, "cancelled"));

  @SuppressWarnings("rawtypes")
  private static final RefHandle<WebTransportFlowPublisher, Throwable> COMPLETED_HANDLE =
      Handles.newRefHandle(
          WebTransportFlowPublisher.class,
          Throwable.class,
          "completed",
          MethodHandles.lookup(),
          () ->
              AtomicReferenceFieldUpdater.newUpdater(
                  WebTransportFlowPublisher.class, Throwable.class, "completed"));

  @SuppressWarnings("rawtypes")
  private static final IntHandle<WebTransportFlowPublisher> TERMINATED_HANDLE =
      Handles.newIntHandle(
          WebTransportFlowPublisher.class,
          "terminated",
          MethodHandles.lookup(),
          () -> AtomicIntegerFieldUpdater.newUpdater(WebTransportFlowPublisher.class, "terminated"));

  private final Queue<T> queue = new ConcurrentLinkedQueue<>();
  private volatile int drainWip;
  private volatile long demand;
  private volatile int cancelled;
  private static final Throwable COMPLETE = new Throwable();
  private volatile Throwable completed;
  private volatile int terminated;
  private volatile Subscriber<? super T> subscriber;

  @Override
  public void subscribe(Subscriber<? super T> s) {
    Objects.requireNonNull(s, "Subscriber must not be null");
    if (this.subscriber != null) {
      s.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {}
          });
      s.onError(new IllegalStateException("Subscriber already exists"));
      return;
    }
    this.subscriber = s;
    s.onSubscribe(
        new Subscription() {
          @Override
          public void request(long n) {
            if (n <= 0) {
              if (TERMINATED_HANDLE.compareAndSet(WebTransportFlowPublisher.this, 0, 1)) {
                cancelled = 1;
                drainAndCloseQueue();
                s.onError(new IllegalArgumentException("Demand must be positive"));
              }
              return;
            }
            DEMAND_HANDLE.updateAndGet(
                WebTransportFlowPublisher.this,
                current -> {
                  if (current == Long.MAX_VALUE) {
                    return Long.MAX_VALUE;
                  }
                  long updated = current + n;
                  return updated < 0 ? Long.MAX_VALUE : updated;
                });
            drain();
          }

          @Override
          public void cancel() {
            cancelled = 1;
            drain();
          }
        });
  }

  private void drainAndCloseQueue() {
    T item;
    while ((item = queue.poll()) != null) {
      if (item instanceof AutoCloseable) {
        try {
          ((AutoCloseable) item).close();
        } catch (Exception expected) {
          // ignored
        }
      }
    }
  }

  /**
   * Emits the next item to downstream subscribers or buffers if demand is exhausted.
   *
   * @param item the item to emit
   */
  public void emitNext(T item) {
    if (cancelled != 0 || (completed != null)) {
      if (item instanceof AutoCloseable) {
        try {
          ((AutoCloseable) item).close();
        } catch (Exception expected) {
          // ignored
        }
      }
      return;
    }
    queue.offer(item);
    drain();
  }

  /** Signals completion to downstream subscriber after pending items are drained. */
  public void emitComplete() {
    if (COMPLETED_HANDLE.compareAndSet(this, null, COMPLETE)) {
      drain();
    }
  }

  /** Signals an error condition to downstream subscriber. */
  public void emitError(Throwable t) {
    if (COMPLETED_HANDLE.compareAndSet(this, null, Objects.requireNonNull(t, "error"))) {
      drain();
    }
  }

  private void drain() {
    if (DRAIN_WIP_HANDLE.getAndIncrement(this) != 0) {
      return;
    }
    int missed = 1;
    do {
      if (cancelled != 0
          || terminated != 0
          || (subscriber == null && (completed != null))) {
        drainAndCloseQueue();
      } else {
        Subscriber<? super T> sub = this.subscriber;
        if (sub != null) {
          while (demand > 0 && cancelled == 0 && terminated == 0) {
            T item = queue.poll();
            if (item == null) {
              break;
            }
            DEMAND_HANDLE.updateAndGet(
                this, current -> current == Long.MAX_VALUE ? current : current - 1);
            try {
              sub.onNext(item);
            } catch (Throwable t) {
              if (item instanceof AutoCloseable) {
                try {
                  ((AutoCloseable) item).close();
                } catch (Exception expected) {
                  // ignored
                }
              }
              cancelled = 1;
              drainAndCloseQueue();
              if (TERMINATED_HANDLE.compareAndSet(this, 0, 1)) {
                sub.onError(t);
              }
              break;
            }
          }
          Throwable signal = completed;
          if (signal != null && cancelled == 0) {
            if (signal != COMPLETE) {
              drainAndCloseQueue();
              if (TERMINATED_HANDLE.compareAndSet(this, 0, 1)) {
                sub.onError(signal);
              }
            } else if (queue.isEmpty() && TERMINATED_HANDLE.compareAndSet(this, 0, 1)) {
              sub.onComplete();
            }
          }
        }
      }
      // Terminal paths also release drain ownership so a late offer can be disposed.
      missed = DRAIN_WIP_HANDLE.addAndGet(this, -missed);
    } while (missed != 0);
  }
}
