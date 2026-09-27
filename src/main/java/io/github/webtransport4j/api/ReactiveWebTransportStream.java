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
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final AtomicBoolean streamClosed = new AtomicBoolean(false);
    private final Queue<WebTransportBuffer> pendingQueue = new ConcurrentLinkedQueue<>();

    SubscriptionImpl(Subscriber<? super WebTransportBuffer> subscriber) {
      this.subscriber = subscriber;
      // Start with auto-read disabled to enforce reactive backpressure at the wire level
      stream.setAutoRead(false);

      // Wire callbacks from the WebTransportStream
      stream.onData(buf -> {
        if (cancelled.get() || terminated.get()) {
          return;
        }
        if (demand.get() > 0) {
          drainQueue();
          if (cancelled.get() || terminated.get()) {
            return;
          }
          if (demand.get() > 0) {
            if (demand.get() != Long.MAX_VALUE) {
              demand.decrementAndGet();
            }
            try {
              buf.retain();
              subscriber.onNext(buf);
            } catch (Throwable t) {
              try {
                buf.release();
              } catch (Throwable ignored) {
              }
              signalError(t);
              return;
            }
          } else {
            buf.retain();
            pendingQueue.offer(buf);
          }
        } else {
          buf.retain();
          pendingQueue.offer(buf);
        }

        checkAndTriggerRead();
      });

      stream.onClose(() -> {
        streamClosed.set(true);
        if (!cancelled.get()) {
          drainQueue();
        }
      });

      stream.onError(this::signalError);
    }

    private void drainQueue() {
      while (!terminated.get() && !cancelled.get() && demand.get() > 0 && !pendingQueue.isEmpty()) {
        WebTransportBuffer buf = pendingQueue.poll();
        if (buf != null) {
          if (demand.get() != Long.MAX_VALUE) {
            demand.decrementAndGet();
          }
          try {
            subscriber.onNext(buf);
          } catch (Throwable t) {
            try {
              buf.release();
            } catch (Throwable ignored) {
            }
            signalError(t);
            break;
          }
        }
      }
      if (streamClosed.get() && pendingQueue.isEmpty()) {
        signalComplete();
      }
    }

    private void signalComplete() {
      if (!cancelled.get() && terminated.compareAndSet(false, true)) {
        subscriber.onComplete();
      }
    }

    private void signalError(Throwable t) {
      if (!cancelled.get() && terminated.compareAndSet(false, true)) {
        try {
          subscriber.onError(t);
        } finally {
          cancel();
        }
      }
    }

    private void addDemand(long n) {
      for (;;) {
        long current = demand.get();
        if (current == Long.MAX_VALUE) {
          return;
        }
        long next = current + n;
        if (next < 0) {
          next = Long.MAX_VALUE;
        }
        if (demand.compareAndSet(current, next)) {
          return;
        }
      }
    }

    private void checkAndTriggerRead() {
      if (cancelled.get() || terminated.get() || streamClosed.get()) {
        return;
      }
      long currentDemand = demand.get();
      if (currentDemand > 0 && pendingQueue.isEmpty()) {
        if (currentDemand == Long.MAX_VALUE) {
          stream.setAutoRead(true);
        } else {
          stream.setAutoRead(false);
          stream.read();
        }
      } else if (currentDemand == 0) {
        stream.setAutoRead(false);
      }
    }

    @Override
    public void request(long n) {
      if (cancelled.get() || terminated.get()) {
        return;
      }
      if (n <= 0) {
        signalError(new IllegalArgumentException("Demand must be positive"));
        return;
      }
      addDemand(n);
      drainQueue();
      checkAndTriggerRead();
    }

    @Override
    public void cancel() {
      terminated.set(true);
      if (!cancelled.compareAndSet(false, true)) {
        return;
      }
      stream.setAutoRead(false);
      stream.close();
      WebTransportBuffer b;
      while ((b = pendingQueue.poll()) != null) {
        try {
          b.release();
        } catch (Throwable ignored) {
        }
      }
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
