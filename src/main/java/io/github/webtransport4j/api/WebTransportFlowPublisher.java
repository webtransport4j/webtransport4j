package io.github.webtransport4j.api;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A standard-compliant Reactive Streams Publisher implementation.
 * Used internally to dispatch event flows (streams, datagrams) without Project
 * Reactor compile dependencies.
 */
public class WebTransportFlowPublisher<T> implements Publisher<T> {
  private final Queue<T> queue = new ConcurrentLinkedQueue<>();
  private Subscriber<? super T> subscriber;
  private final AtomicLong demand = new AtomicLong(0L);
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final AtomicBoolean completed = new AtomicBoolean(false);
  private Throwable error;

  @Override
  public void subscribe(Subscriber<? super T> s) {
    this.subscriber = s;
    s.onSubscribe(new Subscription() {
      @Override
      public void request(long n) {
        if (n <= 0) {
          s.onError(new IllegalArgumentException("Demand must be positive"));
          return;
        }
        demand.addAndGet(n);
        drain();
      }

      @Override
      public void cancel() {
        cancelled.set(true);
        drainAndCloseQueue();
      }
    });
  }

  private void drainAndCloseQueue() {
    T item;
    while ((item = queue.poll()) != null) {
      if (item instanceof AutoCloseable) {
        try {
          ((AutoCloseable) item).close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  public void emitNext(T item) {
    if (cancelled.get() || completed.get()) {
      if (item instanceof AutoCloseable) {
        try {
          ((AutoCloseable) item).close();
        } catch (Exception ignored) {
        }
      }
      return;
    }
    queue.offer(item);
      
    drain();
  }

  public void emitComplete() {
    if (completed.compareAndSet(false, true)) {
      drain();
    }
  }

  public void emitError(Throwable t) {
    this.error = t;
    if (completed.compareAndSet(false, true)) {
      drain();
    }
  }

  private void drain() {
    if (subscriber == null) return;
    while (demand.get() > 0
      && !queue.isEmpty() && !cancelled.get()) {
      T item = queue.poll();
      if (item != null) {
        demand.decrementAndGet();
        try {
          subscriber.onNext(item);
        } catch (Throwable t) {
          if (item instanceof AutoCloseable) {
            try {
              ((AutoCloseable) item).close();
            } catch (Exception ignored) {
            }
          }
          cancelled.set(true);
          drainAndCloseQueue();
          subscriber.onError(t);
          return;
        }
      }
    }
    if (completed.get() && !cancelled.get()) {
      if (error != null) {
        drainAndCloseQueue();
        subscriber.onError(error);
      } else if (queue.isEmpty()) {
        subscriber.onComplete();
      }
    }
  }
}
