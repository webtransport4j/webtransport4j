package io.github.webtransport4j.api;

import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import io.github.webtransport4j.internal.handles.LongHandle;
import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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
 * write completes, fails, or throws synchronously. The underlying stream's write method borrows the
 * buffer rather than consuming that reference.
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
    SubscriptionImpl incomingSubscription = new SubscriptionImpl(stream, subscriber);
    subscriber.onSubscribe(incomingSubscription);
  }

  private static final class SubscriptionImpl implements Subscription {

    private static final IntHandle<SubscriptionImpl> DRAIN_WIP_HANDLE =
        Handles.newIntHandle(
            SubscriptionImpl.class,
            "drainWip",
            MethodHandles.lookup(),
            () -> AtomicIntegerFieldUpdater.newUpdater(SubscriptionImpl.class, "drainWip"));
    private static final LongHandle<SubscriptionImpl> DEMAND_HANDLE =
        Handles.newLongHandle(
            SubscriptionImpl.class,
            "demand",
            MethodHandles.lookup(),
            () -> AtomicLongFieldUpdater.newUpdater(SubscriptionImpl.class, "demand"));
    private static final IntHandle<SubscriptionImpl> CANCELLED_HANDLE =
        Handles.newIntHandle(
            SubscriptionImpl.class,
            "cancelled",
            MethodHandles.lookup(),
            () -> AtomicIntegerFieldUpdater.newUpdater(SubscriptionImpl.class, "cancelled"));
    private static final IntHandle<SubscriptionImpl> TERMINATED_HANDLE =
        Handles.newIntHandle(
            SubscriptionImpl.class,
            "terminated",
            MethodHandles.lookup(),
            () -> AtomicIntegerFieldUpdater.newUpdater(SubscriptionImpl.class, "terminated"));
    private static final IntHandle<SubscriptionImpl> STREAM_CLOSED_HANDLE =
        Handles.newIntHandle(
            SubscriptionImpl.class,
            "streamClosed",
            MethodHandles.lookup(),
            () -> AtomicIntegerFieldUpdater.newUpdater(SubscriptionImpl.class, "streamClosed"));

    private final WebTransportStream stream;
    private final Subscriber<? super WebTransportBuffer> subscriber;
    private volatile int drainWip;
    private volatile long demand;
    private volatile int cancelled;
    private volatile int terminated;
    private volatile int streamClosed;
    private final Queue<WebTransportBuffer> pendingQueue = new ConcurrentLinkedQueue<>();

    SubscriptionImpl(
        WebTransportStream stream, Subscriber<? super WebTransportBuffer> subscriber) {
      this.stream = stream;
      this.subscriber = subscriber;
      stream.setAutoRead(false);

      stream.onData(
          buffer -> {
            if (cancelled != 0 || terminated != 0) {
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
            streamClosed = 1;
            if (cancelled == 0) {
              drainQueue();
            }
          });

      stream.onError(this::signalError);
    }

    private void drainQueue() {
      if (DRAIN_WIP_HANDLE.getAndIncrement(this) != 0) {
        return;
      }
      int missed = 1;
      do {
        while (terminated == 0 && cancelled == 0 && demand > 0) {
          WebTransportBuffer buffer = pendingQueue.poll();
          if (buffer == null) {
            break;
          }
          DEMAND_HANDLE.updateAndGet(
              this, current -> current == Long.MAX_VALUE ? current : current - 1);
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
        if (terminated != 0 || cancelled != 0) {
          releasePendingBuffers();
        }
        if (streamClosed != 0 && pendingQueue.isEmpty()) {
          signalComplete();
        }
        missed = DRAIN_WIP_HANDLE.addAndGet(this, -missed);
      } while (missed != 0);
    }

    private void signalComplete() {
      if (cancelled == 0 && TERMINATED_HANDLE.compareAndSet(this, 0, 1)) {
        subscriber.onComplete();
      }
    }

    private void signalError(Throwable failure) {
      if (cancelled == 0 && TERMINATED_HANDLE.compareAndSet(this, 0, 1)) {
        try {
          subscriber.onError(failure);
        } finally {
          cancel();
        }
      }
    }

    private void addDemand(long n) {
      for (; ; ) {
        long current = demand;
        if (current == Long.MAX_VALUE) {
          return;
        }
        long next = current + n;
        if (next < 0) {
          next = Long.MAX_VALUE;
        }
        if (DEMAND_HANDLE.compareAndSet(this, current, next)) {
          return;
        }
      }
    }

    private void checkAndTriggerRead() {
      if (cancelled != 0 || terminated != 0 || streamClosed != 0) {
        return;
      }
      long currentDemand = demand;
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
      if (cancelled != 0 || terminated != 0) {
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
      terminated = 1;
      if (!CANCELLED_HANDLE.compareAndSet(this, 0, 1)) {
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

    writeFuture.whenComplete((result, failure) -> releaseAfterWrite(item, failure));
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
