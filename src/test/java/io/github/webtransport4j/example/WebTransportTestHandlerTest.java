package io.github.webtransport4j.example;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.OnCloseListener;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** Unit tests for WebTransportTestHandler ACK streams, delay handling and admission control. */
public class WebTransportTestHandlerTest {

  private ScheduledExecutorService scheduler;

  @BeforeEach
  public void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
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

  @Test
  public void testInlineAckCreationFailureCanBeRetried() {
    WebTransportSession session = mock(WebTransportSession.class);
    WebTransportStream ackStream = ackStream();
    CompletableFuture<WebTransportStream> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("creation failed"));
    when(session.createUniStream())
        .thenReturn(failed, CompletableFuture.completedFuture(ackStream));
    Runnable receive = incomingUniData(new WebTransportTestHandler(scheduler, 5), session);

    receive.run();
    receive.run();

    verify(session, times(2)).createUniStream();
    verify(ackStream).write("ACK UNI: payload".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testPendingAckCreationWritesChunksInReceiveOrder() {
    WebTransportSession session = mock(WebTransportSession.class);
    final WebTransportStream ackStream = ackStream();
    CompletableFuture<WebTransportStream> creation = new CompletableFuture<>();
    when(session.createUniStream()).thenReturn(creation);
    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    when(buffer.readBytes())
        .thenReturn("A".getBytes(StandardCharsets.UTF_8), "B".getBytes(StandardCharsets.UTF_8));
    Runnable receive = incomingUniData(new WebTransportTestHandler(scheduler, 5), session, buffer);

    receive.run();
    receive.run();
    verify(session).createUniStream();
    verify(ackStream, never()).write(any(byte[].class));
    creation.complete(ackStream);

    InOrder order = inOrder(ackStream);
    order.verify(ackStream).write("ACK UNI: A".getBytes(StandardCharsets.UTF_8));
    order.verify(ackStream).write("ACK UNI: B".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testReentrantAckCreationSharesFutureAndRegistersHandlersBeforeWriting() {
    WebTransportSession session = mock(WebTransportSession.class);
    final WebTransportStream ackStream = ackStream();
    CompletableFuture<WebTransportStream> creation = new CompletableFuture<>();
    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    when(buffer.readBytes())
        .thenReturn("A".getBytes(StandardCharsets.UTF_8), "B".getBytes(StandardCharsets.UTF_8));
    Runnable receive = incomingUniData(new WebTransportTestHandler(scheduler, 5), session, buffer);
    when(session.createUniStream())
        .thenAnswer(
            invocation -> {
              receive.run();
              return creation;
            });

    receive.run();
    verify(session).createUniStream();
    verify(ackStream, never()).write(any(byte[].class));
    creation.complete(ackStream);

    InOrder order = inOrder(ackStream);
    order.verify(ackStream).onClose(any());
    order.verify(ackStream).onError(any());
    order.verify(ackStream).write("ACK UNI: A".getBytes(StandardCharsets.UTF_8));
    order.verify(ackStream).write("ACK UNI: B".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testStaleAckCallbacksPreserveReplacementStream() {
    WebTransportSession session = mock(WebTransportSession.class);
    WebTransportStream first = ackStream();
    WebTransportStream second = ackStream();
    WebTransportStream third = ackStream();
    AtomicReference<Consumer<Throwable>> firstError = new AtomicReference<>();
    doAnswer(
            invocation -> {
              firstError.set(invocation.getArgument(0));
              return null;
            })
        .when(first)
        .onError(any());
    when(session.createUniStream())
        .thenReturn(
            CompletableFuture.completedFuture(first),
            CompletableFuture.completedFuture(second),
            CompletableFuture.completedFuture(third));
    Runnable receive = incomingUniData(new WebTransportTestHandler(scheduler, 5), session);

    receive.run();
    ArgumentCaptor<OnCloseListener> firstClose = ArgumentCaptor.forClass(OnCloseListener.class);
    verify(first).onClose(firstClose.capture());
    firstError.get().accept(new IllegalStateException("stream error"));
    receive.run();
    firstClose.getValue().onClose();
    firstError.get().accept(new IllegalStateException("late stream error"));
    receive.run();
    ArgumentCaptor<OnCloseListener> secondClose = ArgumentCaptor.forClass(OnCloseListener.class);
    verify(second).onClose(secondClose.capture());
    secondClose.getValue().onClose();
    receive.run();

    verify(session, times(3)).createUniStream();
    verify(first).write(any(byte[].class));
    verify(second, times(2)).write(any(byte[].class));
    verify(third).write(any(byte[].class));
  }

  @Test
  public void testLateAckCreationFailurePreservesReplacementStream() {
    WebTransportSession session = mock(WebTransportSession.class);
    WebTransportStream replacement = ackStream();
    CompletableFuture<WebTransportStream> pending = new CompletableFuture<>();
    when(session.createUniStream())
        .thenReturn(pending, CompletableFuture.completedFuture(replacement));
    WebTransportTestHandler handler = new WebTransportTestHandler(scheduler, 5);
    Runnable receive = incomingUniData(handler, session);

    receive.run();
    handler.onSessionClosed(session);
    receive.run();
    pending.completeExceptionally(new IllegalStateException("late creation failure"));
    receive.run();

    verify(session, times(2)).createUniStream();
    verify(replacement, times(2)).write(any(byte[].class));
  }

  @Test
  public void testSynchronousAckCreationFailureCanBeRetried() {
    WebTransportSession session = mock(WebTransportSession.class);
    WebTransportStream ackStream = ackStream();
    when(session.createUniStream())
        .thenThrow(new IllegalStateException("creation threw"))
        .thenReturn(CompletableFuture.completedFuture(ackStream));
    Runnable receive = incomingUniData(new WebTransportTestHandler(scheduler, 5), session);

    receive.run();
    receive.run();

    verify(session, times(2)).createUniStream();
    verify(ackStream).write(any(byte[].class));
  }

  @Test
  public void testAckWriteFailuresLogSessionIdAndCause() {
    WebTransportSession session = mock(WebTransportSession.class);
    when(session.getSessionStreamId()).thenReturn(42L);
    WebTransportStream ackStream = ackStream();
    CompletableFuture<Void> write = new CompletableFuture<>();
    IllegalStateException asyncFailure = new IllegalStateException("async write failure");
    IllegalStateException syncFailure = new IllegalStateException("sync write failure");
    when(ackStream.write(any(byte[].class))).thenReturn(write).thenThrow(syncFailure);
    when(session.createUniStream()).thenReturn(CompletableFuture.completedFuture(ackStream));
    Runnable receive = incomingUniData(new WebTransportTestHandler(scheduler, 5), session);
    List<LogEvent> errors = new ArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("ackWriteErrors", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            if (event.getLevel() == Level.ERROR) {
              errors.add(event.toImmutable());
            }
          }
        };
    Logger logger = (Logger) LogManager.getLogger(WebTransportTestHandler.class);
    appender.start();
    logger.addAppender(appender);
    try {
      receive.run();
      assertEquals(0, errors.size());
      write.completeExceptionally(asyncFailure);
      receive.run();

      assertEquals(2, errors.size());
      String expected = "❌ Failed to write to cached ack uni stream for session 42";
      assertEquals(expected, errors.get(0).getMessage().getFormattedMessage());
      assertSame(asyncFailure, errors.get(0).getThrown());
      assertEquals(expected, errors.get(1).getMessage().getFormattedMessage());
      assertSame(syncFailure, errors.get(1).getThrown());
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  private WebTransportStream ackStream() {
    WebTransportStream stream = mock(WebTransportStream.class);
    when(stream.write(any(byte[].class))).thenReturn(CompletableFuture.completedFuture(null));
    return stream;
  }

  private Runnable incomingUniData(
      WebTransportTestHandler handler, WebTransportSession session) {
    WebTransportBuffer buffer = mock(WebTransportBuffer.class);
    when(buffer.readBytes()).thenReturn("payload".getBytes(StandardCharsets.UTF_8));
    return incomingUniData(handler, session, buffer);
  }

  private Runnable incomingUniData(
      WebTransportTestHandler handler, WebTransportSession session, WebTransportBuffer buffer) {
    WebTransportStream stream = mock(WebTransportStream.class);
    AtomicReference<Consumer<WebTransportBuffer>> consumer = new AtomicReference<>();
    doAnswer(
            invocation -> {
              consumer.set(invocation.getArgument(0));
              return null;
            })
        .when(stream)
        .onData(any());
    handler.onIncomingStream(session, stream);
    return () -> consumer.get().accept(buffer);
  }
}
