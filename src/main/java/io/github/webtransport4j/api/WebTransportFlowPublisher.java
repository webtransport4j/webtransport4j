package io.github.webtransport4j.api;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A standard-compliant Reactive Streams Publisher implementation.
 * Used internally to dispatch event flows (streams, datagrams) without Project
 * Reactor compile dependencies.
 */
public class WebTransportFlowPublisher<T> implements Publisher<T> {
  private final Queue<T> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger drainWip = new AtomicInteger();
  private final AtomicLong demand = new AtomicLong(0L);
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final AtomicBoolean terminated = new AtomicBoolean(false);
  private volatile Subscriber<? super T> subscriber;
  private Throwable error;

  @Override
  public void subscribe(Subscriber<? super T> s) {
    Objects.requireNonNull(s, "Subscriber must not be null");
    if (this.subscriber != null) {
      s.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
      });
      s.onError(new IllegalStateException("Subscriber already exists"));
      return;
    }
    this.subscriber = s;
    s.onSubscribe(new Subscription() {
      @Override
      public void request(long n) {
        if (n <= 0) {
          if (terminated.compareAndSet(false, true)) {
            cancelled.set(true);
            drainAndCloseQueue();
            s.onError(new IllegalArgumentException("Demand must be positive"));
          }
          return;
        }
        demand.updateAndGet(current -> {
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
        cancelled.set(true);
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
    if (cancelled.get() || completed.get()) {
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

  /**
   * Signals completion to downstream subscriber after pending items are drained.
   */
  public void emitComplete() {
    if (completed.compareAndSet(false, true)) {
      drain();
    }
  }

  /**
   * Signals an error condition to downstream subscriber.
   *
   * @param t the error to emit
   */
  public void emitError(Throwable t) {
    this.error = t;
    if (completed.compareAndSet(false, true)) {
      drain();
    }
  }

  private void drain() {
    if (drainWip.getAndIncrement() != 0) {
      return;
    }
    int missed = 1;
    do {
      if (cancelled.get() || (subscriber == null && completed.get())) {
        drainAndCloseQueue();
        return;
      }
      Subscriber<? super T> sub = this.subscriber;
      if (sub != null) {
        while (demand.get() > 0 && !cancelled.get()) {
          T item = queue.poll();
          if (item == null) {
            break;
          }
          demand.updateAndGet(current -> current == Long.MAX_VALUE ? current : current - 1);
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
            cancelled.set(true);
            drainAndCloseQueue();
            if (terminated.compareAndSet(false, true)) {
              sub.onError(t);
            }
            return;
          }
        }
        if (completed.get() && !cancelled.get()) {
          if (error != null) {
            drainAndCloseQueue();
            if (terminated.compareAndSet(false, true)) {
              sub.onError(error);
            }
            return;
          } else if (queue.isEmpty()) {
            if (terminated.compareAndSet(false, true)) {
              sub.onComplete();
            }
            return;
          }
        }
      }
      missed = drainWip.addAndGet(-missed);
    } while (missed != 0);
  }
}
