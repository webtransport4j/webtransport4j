package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
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

  @SuppressWarnings("unchecked")
  @Test
  public void testSubscriberWritesData() {
    WebTransportStream mockStream = mock(WebTransportStream.class);
    Future<Void> mockFuture = mock(Future.class);
    when(mockStream.write(any(WebTransportBuffer.class))).thenReturn(mockFuture);

    // Mock successful write listener trigger
    doAnswer(invocation -> {
      io.netty.util.concurrent.GenericFutureListener listener = invocation.getArgument(0);
      listener.operationComplete(mockFuture);
      return mockFuture;
    }).when(mockFuture).addListener(any());
    when(mockFuture.isSuccess()).thenReturn(true);

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
}
