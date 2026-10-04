package io.github.webtransport4j.api;

import static io.github.webtransport4j.concurrency.ConcurrencySupport.await;
import static org.junit.Assert.assertEquals;

import io.github.webtransport4j.concurrency.ConcurrencySupport;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Tests bounded exporter admission and the shutdown/submission race. */
public class MetricsConcurrencyTest {
  @Test(timeout = 15000)
  public void saturationAndShutdownPreserveDocumentedDropAccounting() {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger callbacks = new AtomicInteger();
    AsyncWebTransportMetricsListener listener =
        new AsyncWebTransportMetricsListener(
            new io.github.webtransport4j.concurrency.MetricsAdapter() {
              public void onSessionOpened(long id, String path) {
                callbacks.incrementAndGet();
                if (id == 1) {
                  entered.countDown();
                  await(release);
                }
              }
            },
            1);
    try {
      listener.onSessionOpened(1, "/test");
      await(entered);
      listener.onSessionOpened(2, "/test");
      listener.onSessionOpened(3, "/test");
      assertEquals(1, listener.droppedEvents());
      release.countDown();
      listener.close();
      listener.onSessionOpened(4, "/test");
      assertEquals(2, callbacks.get());
      assertEquals(1, listener.droppedEvents());
    } finally {
      release.countDown();
      listener.close();
    }
  }

  @Test(timeout = 15000)
  public void shutdownDuringPublicationCountsDiscard() throws Exception {
    AsyncWebTransportMetricsListener listener =
        new AsyncWebTransportMetricsListener(
            new io.github.webtransport4j.concurrency.MetricsAdapter() {}, 1);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    java.util.concurrent.ConcurrentLinkedQueue<Runnable> controlled =
        new java.util.concurrent.ConcurrentLinkedQueue<Runnable>() {
          public boolean offer(Runnable task) {
            entered.countDown();
            await(release);
            return super.offer(task);
          }
        };
    ConcurrencySupport.replace(listener, "queue", controlled);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      final Future<?> submission = caller.submit(() -> listener.onSessionOpened(1, "/test"));
      await(entered);
      listener.close();
      release.countDown();
      submission.get(5, TimeUnit.SECONDS);
      assertEquals(1, listener.droppedEvents());
    } finally {
      release.countDown();
      caller.shutdownNow();
      listener.close();
    }
  }
}
