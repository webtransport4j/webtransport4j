package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.util.Attribute;
import io.netty.util.concurrent.GenericFutureListener;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link DatagramMailbox}. */
public class DatagramMailboxTest {

  private QuicChannel mockChannel;
  private DefaultChannelPromise closePromise;
  private WebTransportMetricsListener mockMetrics;

  /** Sets up mock channel and metrics before each test. */
  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    mockChannel = mock(QuicChannel.class);
    EventLoop mockLoop = mock(EventLoop.class);
    closePromise = new DefaultChannelPromise(mockChannel, mockLoop);
    when(mockChannel.closeFuture()).thenReturn(closePromise);

    mockMetrics = mock(WebTransportMetricsListener.class);
    Attribute<WebTransportMetricsListener> metricsAttr = mock(Attribute.class);
    when(metricsAttr.get()).thenReturn(mockMetrics);
    when(mockChannel.attr(WebTransportAttributeKeys.METRICS_LISTENER)).thenReturn(metricsAttr);
  }

  /** Tears down system properties and reloads configuration after each test. */
  @After
  public void tearDown() {
    System.clearProperty("webtransport4j.datagram.mailbox.capacity");
    System.clearProperty("webtransport4j.datagram.mailbox.batch_size");
    WebTransportConfig.reload();
  }

  @Test
  public void testSequentialBatchProcessing() throws Exception {
    List<Long> dispatchedSessions = Collections.synchronizedList(new ArrayList<>());
    List<String> payloads = Collections.synchronizedList(new ArrayList<>());

    ExecutorService directExecutor = mock(ExecutorService.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              Runnable r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(directExecutor)
        .execute(any(Runnable.class));

    DatagramMailbox.FrameDispatcher dispatcher =
        (ch, sessionId, frame) -> {
          dispatchedSessions.add(sessionId);
          payloads.add(frame.content().toString(StandardCharsets.UTF_8));
        };

    DatagramMailbox mailbox = new DatagramMailbox(mockChannel, directExecutor, dispatcher);

    List<WebTransportDatagramFrame> frames = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ByteBuf data = Unpooled.copiedBuffer(("payload-" + i).getBytes(StandardCharsets.UTF_8));
      WebTransportDatagramFrame frame = new WebTransportDatagramFrame(100L + i, data);
      frames.add(frame);
      mailbox.enqueue(frame);
    }

    assertEquals(5, dispatchedSessions.size());
    for (int i = 0; i < 5; i++) {
      assertEquals(Long.valueOf(100L + i), dispatchedSessions.get(i));
      assertEquals("payload-" + i, payloads.get(i));
      // Enqueued copy was retained (+1) and then released in worker (-1), leaving Netty caller ref (1)
      assertEquals(1, frames.get(i).refCnt());
      frames.get(i).release();
      assertEquals(0, frames.get(i).refCnt());
    }

    verify(mockChannel, never()).close();
  }

  @Test
  public void testOverloadDropDoesNotCloseChannel() {
    System.setProperty("webtransport4j.datagram.mailbox.capacity", "2");
    WebTransportConfig.reload();

    List<Runnable> submittedTasks = new ArrayList<>();
    ExecutorService queuingExecutor = mock(ExecutorService.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              submittedTasks.add(invocation.getArgument(0));
              return null;
            })
        .when(queuingExecutor)
        .execute(any(Runnable.class));

    DatagramMailbox.FrameDispatcher dispatcher = (ch, sessionId, frame) -> {};
    DatagramMailbox mailbox = new DatagramMailbox(mockChannel, queuingExecutor, dispatcher);

    List<WebTransportDatagramFrame> frames = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ByteBuf data = Unpooled.copiedBuffer(("pkt-" + i).getBytes(StandardCharsets.UTF_8));
      WebTransportDatagramFrame frame = new WebTransportDatagramFrame(200L, data);
      frames.add(frame);
      mailbox.enqueue(frame);
    }

    // Only 1 task scheduled for the batch
    assertEquals(1, submittedTasks.size());

    // First 2 frames accepted and retained (refCnt=2)
    assertEquals(2, frames.get(0).refCnt());
    assertEquals(2, frames.get(1).refCnt());

    // Remaining 3 frames dropped without retaining (refCnt=1)
    assertEquals(1, frames.get(2).refCnt());
    assertEquals(1, frames.get(3).refCnt());
    assertEquals(1, frames.get(4).refCnt());

    // Verify metrics recorded discards
    verify(mockMetrics, org.mockito.Mockito.times(3))
        .onDatagramDiscarded(200L, "mailbox_full");

    // CRUCIAL: Verify channel.close() was NEVER called!
    verify(mockChannel, never()).close();

    // Clean up
    submittedTasks.get(0).run();
    for (WebTransportDatagramFrame f : frames) {
      f.release();
      assertEquals(0, f.refCnt());
    }
  }

  @Test
  public void testExecutorRejectionDrainsAndDoesNotCloseChannel() {
    ExecutorService rejectingExecutor = mock(ExecutorService.class);
    org.mockito.Mockito.doThrow(new RejectedExecutionException("pool saturated"))
        .when(rejectingExecutor)
        .execute(any(Runnable.class));

    DatagramMailbox.FrameDispatcher dispatcher = (ch, sessionId, frame) -> {};
    DatagramMailbox mailbox = new DatagramMailbox(mockChannel, rejectingExecutor, dispatcher);

    ByteBuf data = Unpooled.copiedBuffer("reject-me".getBytes(StandardCharsets.UTF_8));
    WebTransportDatagramFrame frame = new WebTransportDatagramFrame(300L, data);

    mailbox.enqueue(frame);

    // Frame was drained and released in mailbox fail handler, returning to caller ref count
    assertEquals(1, frame.refCnt());
    frame.release();
    assertEquals(0, frame.refCnt());

    // Verify discard metric fired
    verify(mockMetrics).onDatagramDiscarded(300L, "executor_rejected");

    // CRUCIAL: Verify channel.close() was NEVER called!
    verify(mockChannel, never()).close();
  }

  @Test
  public void testChannelCloseDrainsFrames() {
    List<Runnable> pendingTasks = new ArrayList<>();
    ExecutorService queuedExecutor = mock(ExecutorService.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              pendingTasks.add(invocation.getArgument(0));
              return null;
            })
        .when(queuedExecutor)
        .execute(any(Runnable.class));

    DatagramMailbox.FrameDispatcher dispatcher = (ch, sessionId, frame) -> {};
    DatagramMailbox mailbox = new DatagramMailbox(mockChannel, queuedExecutor, dispatcher);

    ByteBuf data = Unpooled.copiedBuffer("close-drain".getBytes(StandardCharsets.UTF_8));
    WebTransportDatagramFrame frame = new WebTransportDatagramFrame(400L, data);
    mailbox.enqueue(frame);

    // Close channel
    mailbox.drainAndRelease();

    // Verify frame was drained
    assertEquals(1, frame.refCnt());
    frame.release();
    assertEquals(0, frame.refCnt());

    verify(mockMetrics).onDatagramDiscarded(400L, "mailbox_drained");
    verify(mockChannel, never()).close();
  }
}
