package io.github.webtransport4j.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for WebTransportTestHandler delay handling and admission control. */
public class WebTransportTestHandlerTest {

  private ScheduledExecutorService scheduler;

  @Before
  public void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @After
  public void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  public void testImmediateProcessingForNormalChunk() {
    WebTransportStream stream = mock(WebTransportStream.class);
    when(stream.isBidirectional()).thenReturn(true);
    when(stream.streamId()).thenReturn(1L);
    when(stream.write(any(byte[].class))).thenReturn(CompletableFuture.completedFuture(null));

    AtomicReference<Consumer<WebTransportBuffer>> dataConsumerRef = new AtomicReference<>();
    org.mockito.Mockito.doAnswer(
            invocation -> {
              dataConsumerRef.set(invocation.getArgument(0));
              return null;
            })
        .when(stream)
        .onData(any());

    WebTransportTestHandler handler = new WebTransportTestHandler(scheduler, 5);
    WebTransportSession session = mock(WebTransportSession.class);
    handler.onIncomingStream(session, stream);

    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    when(buffer.readBytes()).thenReturn("NormalPayload".getBytes(StandardCharsets.UTF_8));

    dataConsumerRef.get().accept(buffer);

    // Verify immediate write without consuming delay permits
    verify(stream).write(any(byte[].class));
    assertEquals(5, handler.getAvailableDelayPermits());
  }

  @Test
  public void testDelayedProcessingUsesSchedulerAndReleasesPermit() throws Exception {
    WebTransportStream stream = mock(WebTransportStream.class);
    when(stream.isBidirectional()).thenReturn(true);
    when(stream.streamId()).thenReturn(2L);

    CountDownLatch writeLatch = new CountDownLatch(1);
    when(stream.write(any(byte[].class)))
        .thenAnswer(
            invocation -> {
              writeLatch.countDown();
              return CompletableFuture.completedFuture(null);
            });

    AtomicReference<Consumer<WebTransportBuffer>> dataConsumerRef = new AtomicReference<>();
    org.mockito.Mockito.doAnswer(
            invocation -> {
              dataConsumerRef.set(invocation.getArgument(0));
              return null;
            })
        .when(stream)
        .onData(any());

    WebTransportTestHandler handler = new WebTransportTestHandler(scheduler, 2);
    WebTransportSession session = mock(WebTransportSession.class);
    handler.onIncomingStream(session, stream);

    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    when(buffer.readBytes()).thenReturn("SleepServer_12345".getBytes(StandardCharsets.UTF_8));

    dataConsumerRef.get().accept(buffer);

    // Permit should be consumed while task is scheduled
    assertEquals(1, handler.getAvailableDelayPermits());

    // Task is delayed by 3000ms; wait for it to complete
    assertTrue(writeLatch.await(5, TimeUnit.SECONDS));

    long deadline = System.currentTimeMillis() + 1000;
    while (handler.getAvailableDelayPermits() < 2 && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }

    // Permit should be released back
    assertEquals(2, handler.getAvailableDelayPermits());
  }

  @Test
  public void testAdmissionControlRejectsWhenCapacityExceeded() {
    WebTransportStream stream1 = mock(WebTransportStream.class);
    when(stream1.isBidirectional()).thenReturn(true);
    when(stream1.streamId()).thenReturn(10L);
    when(stream1.write(any(byte[].class))).thenReturn(CompletableFuture.completedFuture(null));

    WebTransportStream stream2 = mock(WebTransportStream.class);
    when(stream2.isBidirectional()).thenReturn(true);
    when(stream2.streamId()).thenReturn(20L);
    when(stream2.write(any(byte[].class))).thenReturn(CompletableFuture.completedFuture(null));

    AtomicReference<Consumer<WebTransportBuffer>> dataConsumer1 = new AtomicReference<>();
    org.mockito.Mockito.doAnswer(
            invocation -> {
              dataConsumer1.set(invocation.getArgument(0));
              return null;
            })
        .when(stream1)
        .onData(any());

    AtomicReference<Consumer<WebTransportBuffer>> dataConsumer2 = new AtomicReference<>();
    org.mockito.Mockito.doAnswer(
            invocation -> {
              dataConsumer2.set(invocation.getArgument(0));
              return null;
            })
        .when(stream2)
        .onData(any());

    WebTransportTestHandler handler = new WebTransportTestHandler(scheduler, 1);
    WebTransportSession session = mock(WebTransportSession.class);
    handler.onIncomingStream(session, stream1);
    handler.onIncomingStream(session, stream2);

    WebTransportBuffer buffer1 = mock(WebTransportBuffer.class);
    when(buffer1.readBytes()).thenReturn("SleepServer_task1".getBytes(StandardCharsets.UTF_8));

    WebTransportBuffer buffer2 = mock(WebTransportBuffer.class);
    when(buffer2.readBytes()).thenReturn("SleepServer_task2".getBytes(StandardCharsets.UTF_8));

    // 1st task should be admitted
    dataConsumer1.get().accept(buffer1);
    assertEquals(0, handler.getAvailableDelayPermits());

    // 2nd task should be rejected due to admission control capacity
    dataConsumer2.get().accept(buffer2);
    verify(stream2).close();
    verify(stream2, never()).write(any(byte[].class));
  }
}
