package io.github.webtransport4j.api;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A spec-compliant empty Publisher that signals completion immediately. Eliminates custom lambda
 * warning warnings on Publisher implementations.
 */
public final class EmptyPublisher<T> implements Publisher<T> {
  private static final EmptyPublisher<Object> INSTANCE = new EmptyPublisher<>();

  @SuppressWarnings("unchecked")
  public static <T> EmptyPublisher<T> instance() {
    return (EmptyPublisher<T>) INSTANCE;
  }

  private EmptyPublisher() {}

  @Override
  public void subscribe(Subscriber<? super T> s) {
    Objects.requireNonNull(s, "Subscriber must not be null");
    s.onSubscribe(
        new Subscription() {
          private final AtomicBoolean done = new AtomicBoolean();

          @Override
          public void request(long n) {
            if (n <= 0) {
              s.onError(new IllegalArgumentException("Demand must be positive"));
              return;
            }
            if (done.compareAndSet(false, true)) {
              s.onComplete();
            }
          }

          @Override
          public void cancel() {
            done.set(true);
          }
        });
  }
}
