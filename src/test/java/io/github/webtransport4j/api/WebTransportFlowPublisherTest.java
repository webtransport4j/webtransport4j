package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Tests for {@link WebTransportFlowPublisher}.
 */
public class WebTransportFlowPublisherTest {

  @Test
  public void testNullSubscriberThrowsNpe() {
    WebTransportFlowPublisher<String> publisher = new WebTransportFlowPublisher<>();
    try {
      publisher.subscribe(null);
      fail("Should throw NullPointerException");
    } catch (NullPointerException expected) {
      // expected
    }
  }

  @Test
  public void testSequentialDeliveryAndDemandAccounting() {
    WebTransportFlowPublisher<Integer> publisher = new WebTransportFlowPublisher<>();
    List<Integer> received = new ArrayList<>();
    AtomicBoolean completed = new AtomicBoolean(false);
    AtomicReference<Subscription> subRef = new AtomicReference<>();

    publisher.emitNext(1);
    publisher.emitNext(2);

    publisher.subscribe(new Subscriber<Integer>() {
      @Override
      public void onSubscribe(Subscription s) {
        subRef.set(s);
      }

      @Override
      public void onNext(Integer item) {
        received.add(item);
      }

      @Override
      public void onError(Throwable t) {
      }

      @Override
      public void onComplete() {
        completed.set(true);
      }
    });

    // Zero demand -> nothing delivered yet
    assertEquals(0, received.size());

    // Request 1 item
    subRef.get().request(1);
    assertEquals(1, received.size());
    assertEquals(Integer.valueOf(1), received.get(0));

    // Emit 3rd item
    publisher.emitNext(3);
    assertEquals(1, received.size());

    // Request 2 items -> delivers 2 and 3
    subRef.get().request(2);
    assertEquals(3, received.size());
    assertEquals(Integer.valueOf(2), received.get(1));
    assertEquals(Integer.valueOf(3), received.get(2));
    assertFalse(completed.get());

    // Complete publisher
    publisher.emitComplete();
    assertTrue(completed.get());
  }

  @Test
  public void testConcurrentRequestsAndEmitsDeliveredInOrderWithoutOverlap() throws Exception {
    WebTransportFlowPublisher<Integer> publisher = new WebTransportFlowPublisher<>();
    int itemCount = 1000;
    List<Integer> received = Collections.synchronizedList(new ArrayList<>());
    AtomicBoolean concurrentSignalViolation = new AtomicBoolean(false);
    AtomicBoolean inSignal = new AtomicBoolean(false);
    CountDownLatch completedLatch = new CountDownLatch(1);
    AtomicReference<Subscription> subRef = new AtomicReference<>();

    publisher.subscribe(new Subscriber<Integer>() {
      @Override
      public void onSubscribe(Subscription s) {
        subRef.set(s);
      }

      @Override
      public void onNext(Integer item) {
        if (!inSignal.compareAndSet(false, true)) {
          concurrentSignalViolation.set(true);
        }
        received.add(item);
        inSignal.set(false);
      }

      @Override
      public void onError(Throwable t) {
      }

      @Override
      public void onComplete() {
        if (!inSignal.compareAndSet(false, true)) {
          concurrentSignalViolation.set(true);
        }
        completedLatch.countDown();
        inSignal.set(false);
      }
    });

    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      // Thread 1 emits items
      executor.submit(() -> {
        for (int i = 0; i < itemCount; i++) {
          publisher.emitNext(i);
        }
        publisher.emitComplete();
      });

      // Thread 2 & 3 request items concurrently
      for (int t = 0; t < 2; t++) {
        executor.submit(() -> {
          for (int i = 0; i < itemCount / 2; i++) {
            subRef.get().request(1);
          }
        });
      }

      assertTrue(completedLatch.await(5, TimeUnit.SECONDS));
      assertFalse("Subscriber signals must not be called concurrently",
          concurrentSignalViolation.get());
      assertEquals(itemCount, received.size());
      for (int i = 0; i < itemCount; i++) {
        assertEquals(Integer.valueOf(i), received.get(i));
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testDemandMustBePositive() {
    WebTransportFlowPublisher<String> publisher = new WebTransportFlowPublisher<>();
    AtomicReference<Throwable> errorRef = new AtomicReference<>();

    publisher.subscribe(new Subscriber<String>() {
      @Override
      public void onSubscribe(Subscription s) {
        s.request(0);
      }

      @Override
      public void onNext(String s) {
      }

      @Override
      public void onError(Throwable t) {
        errorRef.set(t);
      }

      @Override
      public void onComplete() {
      }
    });

    assertTrue(errorRef.get() instanceof IllegalArgumentException);
  }

  @Test
  public void testCloseableReleasedOnCancel() {
    WebTransportFlowPublisher<TestCloseable> publisher = new WebTransportFlowPublisher<>();
    TestCloseable item1 = new TestCloseable();
    TestCloseable item2 = new TestCloseable();

    publisher.emitNext(item1);
    publisher.emitNext(item2);

    publisher.subscribe(new Subscriber<TestCloseable>() {
      @Override
      public void onSubscribe(Subscription s) {
        s.cancel();
      }

      @Override
      public void onNext(TestCloseable item) {
      }

      @Override
      public void onError(Throwable t) {
      }

      @Override
      public void onComplete() {
      }
    });

    assertTrue(item1.closed);
    assertTrue(item2.closed);

    // Emitting after cancel immediately closes
    TestCloseable item3 = new TestCloseable();
    publisher.emitNext(item3);
    assertTrue(item3.closed);
  }

  @Test
  public void testCloseableReleasedOnCompletedWithoutSubscriber() {
    WebTransportFlowPublisher<TestCloseable> publisher = new WebTransportFlowPublisher<>();
    TestCloseable item1 = new TestCloseable();
    publisher.emitNext(item1);
    assertFalse(item1.closed);

    publisher.emitComplete();
    assertTrue(item1.closed);
  }

  @Test
  public void testOnlySingleSubscriberAllowed() {
    WebTransportFlowPublisher<String> publisher = new WebTransportFlowPublisher<>();
    publisher.subscribe(new NoopSubscriber<>());

    AtomicReference<Throwable> secondError = new AtomicReference<>();
    publisher.subscribe(new Subscriber<String>() {
      @Override
      public void onSubscribe(Subscription s) {
      }

      @Override
      public void onNext(String s) {
      }

      @Override
      public void onError(Throwable t) {
        secondError.set(t);
      }

      @Override
      public void onComplete() {
      }
    });

    assertTrue(secondError.get() instanceof IllegalStateException);
  }

  @Test
  public void testErrorEmission() {
    WebTransportFlowPublisher<String> publisher = new WebTransportFlowPublisher<>();
    AtomicReference<Throwable> errorRef = new AtomicReference<>();
    RuntimeException expectedError = new RuntimeException("test error");

    publisher.subscribe(new Subscriber<String>() {
      @Override
      public void onSubscribe(Subscription s) {
      }

      @Override
      public void onNext(String s) {
      }

      @Override
      public void onError(Throwable t) {
        errorRef.set(t);
      }

      @Override
      public void onComplete() {
      }
    });

    publisher.emitError(expectedError);
    assertEquals(expectedError, errorRef.get());
  }

  private static class TestCloseable implements AutoCloseable {
    boolean closed = false;

    @Override
    public void close() {
      closed = true;
    }
  }

  private static class NoopSubscriber<T> implements Subscriber<T> {
    @Override
    public void onSubscribe(Subscription s) {
    }

    @Override
    public void onNext(T t) {
    }

    @Override
    public void onError(Throwable t) {
    }

    @Override
    public void onComplete() {
    }
  }
}
