package io.github.webtransport4j.api;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Reactive Streams wrapper for reading from and writing to a WebTransport stream.
 *
 * <p>Buffers delivered by this publisher belong to its subscriber and must be released. This
 * wrapper's write subscriber consumes each incoming buffer reference and releases it after the
 * write completes, fails, or throws synchronously. The underlying stream's write method borrows
 * the buffer rather than consuming that reference.
 */
public class ReactiveWebTransportStream
        implements Publisher<WebTransportBuffer>, Subscriber<WebTransportBuffer> {

  private final WebTransportStream stream;
  private Subscription subscription;

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

  /** Returns whether the underlying stream is writable. */
  public boolean isWritable() {
    return stream.isWritable();
  }

  /** Returns a future that completes when the stream becomes writable. */
  public @NonNull CompletableFuture<Void> waitForWritable() {
    return stream.waitForWritable();
  }

  /** Registers a listener for changes to the underlying stream's writability. */
  public void onWritabilityChanged(@NonNull Consumer<Boolean> listener) {
    stream.onWritabilityChanged(listener);
  }

  @Override
  public void subscribe(Subscriber<? super WebTransportBuffer> subscriber) {
    SubscriptionImpl incomingSubscription = new SubscriptionImpl(subscriber);
    subscriber.onSubscribe(incomingSubscription);
  }

  private class SubscriptionImpl implements Subscription {

    private final Subscriber<? super WebTransportBuffer> subscriber;
    private final AtomicInteger drainWip = new AtomicInteger();
    private final AtomicLong demand = new AtomicLong(0L);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final AtomicBoolean streamClosed = new AtomicBoolean(false);
    private final Queue<WebTransportBuffer> pendingQueue = new ConcurrentLinkedQueue<>();

    SubscriptionImpl(Subscriber<? super WebTransportBuffer> subscriber) {
      this.subscriber = subscriber;
      stream.setAutoRead(false);

      stream.onData(
              buffer -> {
                if (cancelled.get() || terminated.get()) {
                  return;
                }
                // The dispatcher owns its original reference. Acquire one for reactive delivery.
                buffer.retain();
                pendingQueue.offer(buffer);
                drainQueue();
                checkAndTriggerRead();
              });

      stream.onClose(
              () -> {
                streamClosed.set(true);
                if (!cancelled.get()) {
                  drainQueue();
                }
              });

      stream.onError(this::signalError);
    }

    private void drainQueue() {
      if (drainWip.getAndIncrement() != 0) {
        return;
      }
      int missed = 1;
      do {
        while (!terminated.get() && !cancelled.get() && demand.get() > 0) {
          WebTransportBuffer buffer = pendingQueue.poll();
          if (buffer == null) {
            break;
          }
          demand.updateAndGet(current -> current == Long.MAX_VALUE ? current : current - 1);
          try {
            subscriber.onNext(buffer);
          } catch (Throwable failure) {
            try {
              buffer.release();
            } catch (Throwable ignored) {
              // Continue terminating and releasing the remaining queued buffers.
            }
            signalError(failure);
            break;
          }
        }
        if (terminated.get() || cancelled.get()) {
          releasePendingBuffers();
        }
        if (streamClosed.get() && pendingQueue.isEmpty()) {
          signalComplete();
        }
        missed = drainWip.addAndGet(-missed);
      } while (missed != 0);
    }

    private void signalComplete() {
      if (!cancelled.get() && terminated.compareAndSet(false, true)) {
        subscriber.onComplete();
      }
    }

    private void signalError(Throwable failure) {
      if (!cancelled.get() && terminated.compareAndSet(false, true)) {
        try {
          subscriber.onError(failure);
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
      try {
        stream.setAutoRead(false);
      } finally {
        try {
          stream.close();
        } finally {
          releasePendingBuffers();
        }
      }
    }

    private void releasePendingBuffers() {
      WebTransportBuffer buffer;
      while ((buffer = pendingQueue.poll()) != null) {
        try {
          buffer.release();
        } catch (Throwable ignored) {
          // Continue releasing the other queued references.
        }
      }
    }
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }

  @Override
  public void onNext(WebTransportBuffer item) {
    final CompletableFuture<Void> writeFuture;

    try {
      writeFuture = stream.write(item);
    } catch (Throwable failure) {
      releaseAfterWrite(item, failure);
      return;
    }

    writeFuture.whenComplete(
            (result, failure) -> releaseAfterWrite(item, failure));
  }

  private void releaseAfterWrite(WebTransportBuffer item, Throwable failure) {
    try {
      if (failure != null) {
        onError(failure);
      } else {
        subscription.request(1);
      }
    } finally {
      item.close();
    }
  }


  @Override
  public void onError(Throwable throwable) {
    try {
      if (subscription != null) {
        subscription.cancel();
      }
    } finally {
      try {
        Consumer<Throwable> handler = stream.getErrorHandler();
        if (handler != null) {
          handler.accept(throwable);
        }
      } finally {
        stream.close();
      }
    }
  }

  @Override
  public void onComplete() {
    stream.close();
  }
}
