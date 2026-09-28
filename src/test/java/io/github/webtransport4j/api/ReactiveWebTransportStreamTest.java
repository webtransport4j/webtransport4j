package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/** Test cases for ReactiveWebTransportStream wrapper. */
public class ReactiveWebTransportStreamTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testPublisherEmitsData() {
    WebTransportStream mockStream = mock(WebTransportStream.class);
    ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(mockStream);

    List<WebTransportBuffer> received = new ArrayList<>();
    Subscriber<WebTransportBuffer> subscriber = new Subscriber<WebTransportBuffer>() {
      private Subscription subscription;

      @Override
      public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(2); // request 2 items
      }

      @Override
      public void onNext(WebTransportBuffer item) {
        received.add(item);
      }

      @Override
      public void onError(Throwable throwable) {}

      @Override
      public void onComplete() {}
    };

    reactiveStream.subscribe(subscriber);

    // Verify callback was registered on the mock stream
    ArgumentCaptor<Consumer<WebTransportBuffer>> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
    verify(mockStream).onData(consumerCaptor.capture());
    Consumer<WebTransportBuffer> registeredConsumer = consumerCaptor.getValue();

    // Simulate incoming data
    WebTransportBuffer mockBuffer1 = mock(WebTransportBuffer.class);
    WebTransportBuffer mockBuffer2 = mock(WebTransportBuffer.class);
    registeredConsumer.accept(mockBuffer1);
    registeredConsumer.accept(mockBuffer2);

    assertEquals(2, received.size());
    assertTrue(received.contains(mockBuffer1));
    assertTrue(received.contains(mockBuffer2));
  }

  @Test
  public void testSubscriberWritesData() {
    WebTransportStream mockStream = mock(WebTransportStream.class);
    CompletableFuture<Void> completedFuture =
        CompletableFuture.completedFuture(null);
    when(mockStream.write(any(WebTransportBuffer.class))).thenReturn(completedFuture);

    ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(mockStream);

    Subscription mockSubscription = mock(Subscription.class);
    reactiveStream.onSubscribe(mockSubscription);

    // The subscription should request first item
    verify(mockSubscription).request(1);

    WebTransportBuffer mockBuffer = mock(WebTransportBuffer.class);
    reactiveStream.onNext(mockBuffer);

    // Verify stream write was called
    verify(mockStream).write(mockBuffer);
    // Verify subscription requested next item after write completes successfully
    verify(mockSubscription, times(2)).request(1);
  }

  @Test
  public void testStreamDelegationAndPriority() {
    WebTransportStream mockStream = mock(WebTransportStream.class);
    when(mockStream.setPriority(any(StreamPriority.class))).thenReturn(CompletableFuture.completedFuture(null));
    when(mockStream.setPriority(anyInt(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
    when(mockStream.getPriority()).thenReturn(StreamPriority.of(1, true));

    ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(mockStream);
    assertSame(mockStream, reactiveStream.stream());

    reactiveStream.setPriority(StreamPriority.of(1, true));
    verify(mockStream).setPriority(StreamPriority.of(1, true));

    reactiveStream.setPriority(4, false);
    verify(mockStream).setPriority(4, false);

    assertEquals(StreamPriority.of(1, true), reactiveStream.getPriority());
    verify(mockStream).getPriority();
  }

  @Test
  public void testWireLevelBackpressureWithDemandAndPause() {
    WebTransportStream mockStream = mock(WebTransportStream.class);
    ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(mockStream);

    ArgumentCaptor<Consumer<WebTransportBuffer>> consumerCaptor =
        ArgumentCaptor.forClass(Consumer.class);

    List<WebTransportBuffer> received = new ArrayList<>();
    final Subscription[] subscriptionHolder = new Subscription[1];

    reactiveStream.subscribe(new Subscriber<WebTransportBuffer>() {
      @Override
      public void onSubscribe(Subscription s) {
        subscriptionHolder[0] = s;
      }

      @Override
      public void onNext(WebTransportBuffer item) {
        received.add(item);
      }

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onComplete() {}
    });

    // 1. Initial subscription must disable auto-read on the wire
    verify(mockStream, atLeastOnce()).setAutoRead(false);
    verify(mockStream, never()).read();

    verify(mockStream).onData(consumerCaptor.capture());
    Consumer<WebTransportBuffer> onData = consumerCaptor.getValue();

    // 2. Request 1 item -> triggers transport read
    subscriptionHolder[0].request(1);
    verify(mockStream, times(1)).read();

    // 3. Deliver item -> demand becomes 0, pauses reading at wire level
    WebTransportBuffer buf1 = mock(WebTransportBuffer.class);
    when(buf1.retain()).thenReturn(buf1);
    onData.accept(buf1);

    assertEquals(1, received.size());
    // Since demand reached 0, read() must NOT be called again
    verify(mockStream, times(1)).read();
    verify(mockStream, atLeastOnce()).setAutoRead(false);

    // 4. Request another item -> resumes transport read
    subscriptionHolder[0].request(1);
    verify(mockStream, times(2)).read();
  }

  @Test
  public void testUnboundedDemandEnablesAutoRead() {
    WebTransportStream mockStream = mock(WebTransportStream.class);
    ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(mockStream);

    reactiveStream.subscribe(new Subscriber<WebTransportBuffer>() {
      @Override
      public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(WebTransportBuffer item) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onComplete() {}
    });

    // Unbounded demand switches auto-read to true for maximum line rate
    verify(mockStream).setAutoRead(true);
  }

  @Test
  public void testCancelDisablesAutoReadAndReleasesPendingBuffers() {
    WebTransportStream mockStream = mock(WebTransportStream.class);
    ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(mockStream);

    ArgumentCaptor<Consumer<WebTransportBuffer>> consumerCaptor =
        ArgumentCaptor.forClass(Consumer.class);

    final Subscription[] subscriptionHolder = new Subscription[1];
    reactiveStream.subscribe(new Subscriber<WebTransportBuffer>() {
      @Override
      public void onSubscribe(Subscription s) {
        subscriptionHolder[0] = s;
      }

      @Override
      public void onNext(WebTransportBuffer item) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onComplete() {}
    });

    verify(mockStream).onData(consumerCaptor.capture());
    Consumer<WebTransportBuffer> onData = consumerCaptor.getValue();

    // Deliver data while demand is 0 -> queued in pendingQueue
    WebTransportBuffer queuedBuffer = mock(WebTransportBuffer.class);
    when(queuedBuffer.retain()).thenReturn(queuedBuffer);
    onData.accept(queuedBuffer);

    verify(queuedBuffer).retain();

    // Cancel subscription -> drains queue and releases buffer
    subscriptionHolder[0].cancel();
    verify(queuedBuffer).release();
    verify(mockStream).close();
    verify(mockStream, atLeastOnce()).setAutoRead(false);
  }
  @Test
  public void testCloseAndRepeatedRequestsSignalCompleteOnce() {
    TerminalFixture f = new TerminalFixture();
    f.close.onClose();
    f.close.onClose();
    f.subscription.request(1);
    f.subscription.request(0);
    f.error.accept(new IllegalStateException("late error"));
    WebTransportBuffer lateBuffer = mock(WebTransportBuffer.class);
    f.data.accept(lateBuffer);
    verify(f.subscriber, never()).onNext(any());
    verify(lateBuffer, never()).release(); // No reference was retained after termination.
    verify(f.subscriber, times(1)).onComplete();
    verify(f.subscriber, never()).onError(any());
  }

  @Test
  public void testCloseWaitsForQueuedDataThenCompletesOnce() {
    TerminalFixture f = new TerminalFixture();
    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    f.data.accept(buffer);
    f.close.onClose();
    verify(f.subscriber, never()).onComplete();
    f.subscription.request(1);
    f.subscription.request(1);
    f.close.onClose();
    verify(f.subscriber).onNext(buffer);
    verify(f.subscriber, times(1)).onComplete();
  }

  @Test
  public void testInvalidDemandReleasesQueueAndSignalsErrorOnce() {
    TerminalFixture f = new TerminalFixture();
    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    f.data.accept(buffer);
    f.subscription.request(0);
    f.subscription.request(-1);
    f.close.onClose();
    f.error.accept(new IllegalStateException("late error"));
    verify(f.subscriber, times(1)).onError(any(IllegalArgumentException.class));
    verify(f.subscriber, never()).onComplete();
    verify(buffer).release();
  }

  @Test
  public void testOnNextFailureSignalsErrorOnce() {
    TerminalFixture f = new TerminalFixture();
    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    org.mockito.Mockito.doThrow(new IllegalStateException("subscriber failed"))
        .when(f.subscriber).onNext(buffer);
    f.data.accept(buffer);
    f.close.onClose();
    f.subscription.request(1);
    f.subscription.request(1);
    f.close.onClose();
    verify(f.subscriber, times(1)).onError(any(IllegalStateException.class));
    verify(f.subscriber, never()).onComplete();
    verify(buffer).release();
  }

  @Test
  public void testConcurrentCloseAndErrorSignalOnlyOneTerminalEvent() throws Exception {
    for (int i = 0; i < 50; i++) {
      TerminalFixture f = new TerminalFixture();
      java.util.concurrent.CountDownLatch ready = new java.util.concurrent.CountDownLatch(2);
      java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
      CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
        ready.countDown();
        await(go);
        f.close.onClose();
      });
      CompletableFuture<Void> error = CompletableFuture.runAsync(() -> {
        ready.countDown();
        await(go);
        f.error.accept(new IllegalStateException("transport failed"));
      });
      try {
        assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
      } finally {
        go.countDown();
      }
      CompletableFuture.allOf(close, error).get(5, java.util.concurrent.TimeUnit.SECONDS);
      long terminals = org.mockito.Mockito.mockingDetails(f.subscriber).getInvocations().stream()
          .filter(invocation -> invocation.getMethod().getName().equals("onComplete")
              || invocation.getMethod().getName().equals("onError"))
          .count();
      assertEquals(1L, terminals);
    }
  }

  private static void await(java.util.concurrent.CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, java.util.concurrent.TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static class TerminalFixture {
    final Subscriber<WebTransportBuffer> subscriber;
    final Subscription subscription;
    final OnCloseListener close;
    final Consumer<Throwable> error;
    final Consumer<WebTransportBuffer> data;

    @SuppressWarnings("unchecked")
    TerminalFixture() {
      WebTransportStream stream = mock(WebTransportStream.class);
      subscriber = mock(Subscriber.class);
      new ReactiveWebTransportStream(stream).subscribe(subscriber);
      ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
      ArgumentCaptor<OnCloseListener> closeCaptor = ArgumentCaptor.forClass(OnCloseListener.class);
      ArgumentCaptor<Consumer<Throwable>> errorCaptor = ArgumentCaptor.forClass(Consumer.class);
      ArgumentCaptor<Consumer<WebTransportBuffer>> dataCaptor = ArgumentCaptor.forClass(Consumer.class);
      verify(subscriber).onSubscribe(subscriptionCaptor.capture());
      verify(stream).onClose(closeCaptor.capture());
      verify(stream).onError(errorCaptor.capture());
      verify(stream).onData(dataCaptor.capture());
      subscription = subscriptionCaptor.getValue();
      close = closeCaptor.getValue();
      error = errorCaptor.getValue();
      data = dataCaptor.getValue();
    }
  }

}
