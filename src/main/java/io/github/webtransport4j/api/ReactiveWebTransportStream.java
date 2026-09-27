package io.github.webtransport4j.api;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Standard Reactive Streams wrapper for WebTransportStream.
 * Implements both Publisher (for reading from the stream)
 * and Subscriber (for writing to the stream).
 * Compatible natively with Spring WebFlux, Project Reactor, RxJava, etc.
 */
public class ReactiveWebTransportStream implements Publisher<WebTransportBuffer>, Subscriber<WebTransportBuffer> {
  private final WebTransportStream stream;

  public ReactiveWebTransportStream(@NonNull WebTransportStream stream) {
    this.stream = stream;
  }

  public @NonNull WebTransportStream stream() {
    return stream;
  }

  public @NonNull CompletableFuture<Void> setPriority(@NonNull StreamPriority priority) {
    return stream.setPriority(priority);
  }

  public @NonNull CompletableFuture<Void> setPriority(int urgency, boolean incremental) {
    return stream.setPriority(urgency, incremental);
  }

  public @NonNull StreamPriority getPriority() {
    return stream.getPriority();
  }

  /**
   * Returns true if the underlying stream is writable.
   *
   * @return true if writes can be accepted without exceeding backpressure thresholds
   */
  public boolean isWritable() {
    return stream.isWritable();
  }

  /**
   * Returns a future that completes when the stream becomes writable again.
   *
   * @return future that completes when writable
   */
  public @NonNull CompletableFuture<Void> waitForWritable() {
    return stream.waitForWritable();
  }

  /**
   * Registers a listener to be notified when the stream's writability state changes.
   *
   * @param listener consumer receiving true when writable, false when congested
   */
  public void onWritabilityChanged(@NonNull Consumer<Boolean> listener) {
    stream.onWritabilityChanged(listener);
  }

  // --- Publisher Implementation ---
  @Override
  public void subscribe(Subscriber<? super WebTransportBuffer> subscriber) {
    SubscriptionImpl subscription = new SubscriptionImpl(subscriber);
    subscriber.onSubscribe(subscription);
  }

  private class SubscriptionImpl implements Subscription {
    private final Subscriber<? super WebTransportBuffer> subscriber;
    private final AtomicLong demand = new AtomicLong(0L);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final Queue<WebTransportBuffer> pendingQueue = new ConcurrentLinkedQueue<>();

    SubscriptionImpl(Subscriber<? super WebTransportBuffer> subscriber) {
      this.subscriber = subscriber;

      // Wire callbacks from the WebTransportStream
      stream.onData(buf -> {
        if (cancelled.get()) {
          return;
        }
        if (demand.get() > 0) {
          drainQueue();
          if (demand.get() > 0) {
            demand.decrementAndGet();
            try {
              subscriber.onNext(buf);
            } catch (Throwable t) {
              subscriber.onError(t);
              cancel();
            }
          } else {
            pendingQueue.offer(buf);
          }
        } else {
          pendingQueue.offer(buf);
        }
      });

      stream.onClose(() -> {
        if (!cancelled.get()) {
          drainQueue();
          subscriber.onComplete();
        }
      });

      stream.onError(t -> {
        if (!cancelled.get()) {
          subscriber.onError(t);
        }
      });
    }

    private void drainQueue() {
      while (demand.get() > 0 && !pendingQueue.isEmpty()) {
        WebTransportBuffer buf = pendingQueue.poll();
        if (buf != null) {
          demand.decrementAndGet();
          try {
            subscriber.onNext(buf);
          } catch (Throwable t) {
            subscriber.onError(t);
            cancel();
            break;
          }
        }
      }
    }

    @Override
    public void request(long n) {
      if (n <= 0) {
        subscriber.onError(new IllegalArgumentException("Demand must be positive"));
        return;
      }
      demand.addAndGet(n);
      drainQueue();
    }

    @Override
    public void cancel() {
      cancelled.set(true);
      stream.close();
      pendingQueue.clear();
    }
  }

  // --- Subscriber Implementation ---
  private Subscription subscription;

  @Override
  public void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1); // request the first item
  }

  @Override
  public void onNext(WebTransportBuffer item) {
    stream.write(item).whenComplete((res, ex) -> {
      if (ex == null) {
        subscription.request(1); // request next item
      } else {
        onError(ex);
      }
    });
  }

  @Override
  public void onError(Throwable throwable) {
    if (stream.getErrorHandler() != null) {
      stream.getErrorHandler().accept(throwable);
    }
    stream.close();
  }

  @Override
  public void onComplete() {
    stream.close();
  }
}
