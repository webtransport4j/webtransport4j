package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.util.Attribute;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for priority QoS scheduling and eviction in {@link DatagramMailbox}.
 */
public class DatagramMailboxPriorityTest {

  private QuicChannel mockChannel;
  private DefaultChannelPromise closePromise;
  private WebTransportMetricsListener mockMetrics;

  /** Sets up mock channel and metrics before each test. */
  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    mockChannel = mock(QuicChannel.class);
    final EventLoop mockLoop = mock(EventLoop.class);
    closePromise = new DefaultChannelPromise(mockChannel, mockLoop);
    when(mockChannel.closeFuture()).thenReturn(closePromise);

    mockMetrics = mock(WebTransportMetricsListener.class);
    final Attribute<WebTransportMetricsListener> metricsAttr = mock(Attribute.class);
    when(metricsAttr.get()).thenReturn(mockMetrics);
    when(mockChannel.attr(WebTransportAttributeKeys.METRICS_LISTENER)).thenReturn(metricsAttr);
  }

  /** Tears down system properties and reloads configuration after each test. */
  @After
  public void tearDown() {
    System.clearProperty("webtransport4j.datagram.mailbox.capacity");
    WebTransportConfig.reload();
  }

  @Test
  public void testPriorityOrdering() {
    final List<String> payloads = Collections.synchronizedList(new ArrayList<>());
    final List<Runnable> tasks = new ArrayList<>();
    final ExecutorService queuedExecutor = mock(ExecutorService.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              tasks.add(invocation.getArgument(0));
              return null;
            })
        .when(queuedExecutor)
        .execute(any(Runnable.class));

    final DatagramMailbox.FrameDispatcher dispatcher =
        (ch, sessionId, frame) -> payloads.add(frame.content().toString(StandardCharsets.UTF_8));

    final DatagramMailbox mailbox = new DatagramMailbox(mockChannel, queuedExecutor, dispatcher);

    final ByteBuf b1 = Unpooled.copiedBuffer("norm-1".getBytes(StandardCharsets.UTF_8));
    final ByteBuf b2 = Unpooled.copiedBuffer("norm-2".getBytes(StandardCharsets.UTF_8));
    final ByteBuf bPri = Unpooled.copiedBuffer("high-pri".getBytes(StandardCharsets.UTF_8));

    final WebTransportDatagramFrame f1 = new WebTransportDatagramFrame(10L, b1);
    final WebTransportDatagramFrame f2 = new WebTransportDatagramFrame(10L, b2);
    final WebTransportDatagramFrame fPri = new WebTransportDatagramFrame(10L, bPri);

    // Enqueue normal 1, normal 2, then high priority
    mailbox.enqueue(f1, false);
    mailbox.enqueue(f2, false);
    mailbox.enqueue(fPri, true);

    assertEquals(1, tasks.size());
    tasks.get(0).run();

    // Clean up caller references
    f1.release();
    f2.release();
    fPri.release();

    assertEquals(3, payloads.size());
    // High priority must be dispatched first!
    assertEquals("high-pri", payloads.get(0));
    assertEquals("norm-1", payloads.get(1));
    assertEquals("norm-2", payloads.get(2));
  }

  @Test
  public void testNormalFrameEvictionForHighPriorityWhenFull() {
    System.setProperty("webtransport4j.datagram.mailbox.capacity", "2");
    WebTransportConfig.reload();

    final List<Runnable> tasks = new ArrayList<>();
    final ExecutorService queuedExecutor = mock(ExecutorService.class);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              tasks.add(invocation.getArgument(0));
              return null;
            })
        .when(queuedExecutor)
        .execute(any(Runnable.class));

    final List<String> payloads = new ArrayList<>();
    final DatagramMailbox.FrameDispatcher dispatcher =
        (ch, sessionId, frame) -> payloads.add(frame.content().toString(StandardCharsets.UTF_8));

    final DatagramMailbox mailbox = new DatagramMailbox(mockChannel, queuedExecutor, dispatcher);

    final ByteBuf b1 = Unpooled.copiedBuffer("norm-1".getBytes(StandardCharsets.UTF_8));
    final ByteBuf b2 = Unpooled.copiedBuffer("norm-2".getBytes(StandardCharsets.UTF_8));
    final ByteBuf bPri = Unpooled.copiedBuffer("high-pri".getBytes(StandardCharsets.UTF_8));

    final WebTransportDatagramFrame f1 = new WebTransportDatagramFrame(20L, b1);
    final WebTransportDatagramFrame f2 = new WebTransportDatagramFrame(20L, b2);
    final WebTransportDatagramFrame fPri = new WebTransportDatagramFrame(20L, bPri);

    // Enqueue 2 normal frames -> capacity reached
    mailbox.enqueue(f1, false);
    mailbox.enqueue(f2, false);
    assertEquals(2, f1.refCnt());
    assertEquals(2, f2.refCnt());

    // Enqueue high-priority frame -> evicts f1
    mailbox.enqueue(fPri, true);

    // f1 should have been evicted and its retained ref released (refCnt back to caller's 1)
    assertEquals(1, f1.refCnt());
    // f2 and fPri remain retained
    assertEquals(2, f2.refCnt());
    assertEquals(2, fPri.refCnt());

    // Verify eviction metric fired
    verify(mockMetrics).onDatagramDiscarded(20L, "evicted_for_high_priority");

    // Execute the mailbox task
    assertEquals(1, tasks.size());
    tasks.get(0).run();

    // Verify dispatched frames: high-pri, then norm-2
    assertEquals(2, payloads.size());
    assertEquals("high-pri", payloads.get(0));
    assertEquals("norm-2", payloads.get(1));

    // Cleanup caller references
    f1.release();
    f2.release();
    fPri.release();
    assertEquals(0, f1.refCnt());
    assertEquals(0, f2.refCnt());
    assertEquals(0, fPri.refCnt());
  }
}
