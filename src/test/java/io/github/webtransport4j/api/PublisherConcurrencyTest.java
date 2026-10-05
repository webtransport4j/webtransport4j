package io.github.webtransport4j.api;

import static io.github.webtransport4j.concurrency.ConcurrencySupport.await;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/** Deterministic demand/cancellation and terminal ownership races. */
public class PublisherConcurrencyTest {
  @Test
  public void losingErrorCannotReplaceBufferedCompletion() {
    WebTransportFlowPublisher<Integer> publisher = new WebTransportFlowPublisher<>();
    AtomicInteger completions = new AtomicInteger();
    AtomicReference<Subscription> subscription = new AtomicReference<>();
    publisher.subscribe(
        new Subscriber<Integer>() {
          public void onSubscribe(Subscription value) {
            subscription.set(value);
          }

          public void onNext(Integer item) {
            assertEquals(Integer.valueOf(1), item);
          }

          public void onComplete() {
            completions.incrementAndGet();
          }

          public void onError(Throwable error) {
            throw new AssertionError(error);
          }
        });
    publisher.emitNext(1);
    publisher.emitComplete();
    publisher.emitError(new IllegalStateException("losing terminal"));
    subscription.get().request(1);
    assertEquals(1, completions.get());
  }

  @Test(timeout = 15000)
  public void cancelWhileOnNextRunsDisposesQueuedItemsExactlyOnce() throws Exception {
    WebTransportFlowPublisher<Item> publisher = new WebTransportFlowPublisher<>();
    AtomicReference<Subscription> subscription = new AtomicReference<>();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger terminals = new AtomicInteger();
    publisher.subscribe(
        new Subscriber<Item>() {
          public void onSubscribe(Subscription s) {
            subscription.set(s);
            s.request(1);
          }

          public void onNext(Item item) {
            entered.countDown();
            await(release);
            item.close();
          }

          public void onError(Throwable error) {
            terminals.incrementAndGet();
          }

          public void onComplete() {
            terminals.incrementAndGet();
          }
        });
    Item first = new Item();
    Item queued = new Item();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> emitter = executor.submit(() -> publisher.emitNext(first));
      await(entered);
      publisher.emitNext(queued);
      subscription.get().cancel();
      publisher.emitComplete();
      release.countDown();
      emitter.get(5, TimeUnit.SECONDS);
      assertEquals(1, first.closes.get());
      assertEquals(1, queued.closes.get());
      assertEquals(0, terminals.get());
    } finally {
      release.countDown();
      executor.shutdownNow();
      publisher.emitComplete();
    }
  }

  @Test(timeout = 15000)
  public void concurrentErrorAndCompleteHaveOneTerminalOwner() throws Exception {
    WebTransportFlowPublisher<Integer> publisher = new WebTransportFlowPublisher<>();
    AtomicInteger terminals = new AtomicInteger();
    publisher.subscribe(
        new Subscriber<Integer>() {
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          public void onNext(Integer item) {}

          public void onError(Throwable error) {
            terminals.incrementAndGet();
          }

          public void onComplete() {
            terminals.incrementAndGet();
          }
        });
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> first =
          executor.submit(
              () -> {
                await(go);
                publisher.emitComplete();
              });
      final Future<?> second =
          executor.submit(
              () -> {
                await(go);
                publisher.emitError(new Exception("race"));
              });
      go.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
      assertEquals(1, terminals.get());
    } finally {
      go.countDown();
      executor.shutdownNow();
    }
  }

  static final class Item implements AutoCloseable {
    final AtomicInteger closes = new AtomicInteger();

    public void close() {
      closes.incrementAndGet();
    }
  }
}
