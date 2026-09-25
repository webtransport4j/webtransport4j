package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link WebTransportSessionManager} session lifecycle, bulk cleanup,
 * and global session counter management under connection teardown.
 */
public class WebTransportSessionManagerTest {

  private WebTransportSessionManager sessionManager;
  private AtomicInteger globalActiveSessions;
  private QuicChannel mockQuicChannel;

  /** Set up test state before each test. */
  @Before
  public void setUp() {
    sessionManager = new WebTransportSessionManager();
    globalActiveSessions = new AtomicInteger(0);
    mockQuicChannel = Mockito.mock(QuicChannel.class);

    // Set up channel attribute mock for GLOBAL_SESSION_COUNT
    io.netty.util.Attribute<AtomicInteger> mockAttr = Mockito.mock(io.netty.util.Attribute.class);
    when(mockAttr.get()).thenReturn(globalActiveSessions);
    when(mockQuicChannel.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT)).thenReturn(mockAttr);
  }

  @Test
  public void testCloseAllWithNullParentConnectStream() {
    // Simulate 5 registered sessions whose parent quic channel was initially present
    QuicStreamChannel[] mockStreams = new QuicStreamChannel[5];
    for (int i = 0; i < 5; i++) {
      long id = i + 1;
      mockStreams[i] = Mockito.mock(QuicStreamChannel.class);
      when(mockStreams[i].streamId()).thenReturn(id);
      when(mockStreams[i].parent()).thenReturn(mockQuicChannel);

      sessionManager.register(mockStreams[i]);
    }

    // Assert global sessions incremented to 5 during registration
    assertEquals(5, globalActiveSessions.get());
    assertEquals(5, sessionManager.getSessions().size());

    // Now simulate Netty un-parenting all connectStreams during fast connection teardown
    for (QuicStreamChannel mockStream : mockStreams) {
      when(mockStream.parent()).thenReturn(null);
    }

    // Call closeAll with explicit quicChannel parameter
    sessionManager.closeAll(mockQuicChannel);

    // Verify sessions map cleared AND globalActiveSessions decremented back to 0
    assertEquals(0, sessionManager.getSessions().size());
    assertEquals(0, globalActiveSessions.get());
  }

  @Test
  public void testCloseAllWithoutQuicChannelParameter() {
    for (long id = 1; id <= 3; id++) {
      QuicStreamChannel mockStream = Mockito.mock(QuicStreamChannel.class);
      when(mockStream.streamId()).thenReturn(id);
      when(mockStream.parent()).thenReturn(mockQuicChannel);

      sessionManager.register(mockStream);
    }

    assertEquals(3, globalActiveSessions.get());

    // Call fallback closeAll()
    sessionManager.closeAll();
    assertEquals(0, sessionManager.getSessions().size());
    assertEquals(0, globalActiveSessions.get());
  }

  @Test
  public void testUnregisterNoDoubleDecrement() {
    QuicStreamChannel mockStream = Mockito.mock(QuicStreamChannel.class);
    when(mockStream.streamId()).thenReturn(100L);
    when(mockStream.parent()).thenReturn(mockQuicChannel);

    sessionManager.register(mockStream);
    assertEquals(1, globalActiveSessions.get());

    // First unregister call
    sessionManager.unregister(mockStream);
    assertEquals(0, globalActiveSessions.get());

    // Duplicate unregister call for same stream ID
    sessionManager.unregister(mockStream);
    assertEquals(0, globalActiveSessions.get());
  }

  @Test
  public void testReservationsTransferIntoActiveCapacityAndReleaseOnClose() {
    AtomicInteger globalSlots = new AtomicInteger(1);
    io.netty.util.Attribute<AtomicInteger> slotsAttr = Mockito.mock(io.netty.util.Attribute.class);
    when(slotsAttr.get()).thenReturn(globalSlots);
    when(mockQuicChannel.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS)).thenReturn(slotsAttr);

    assertTrue(sessionManager.reserveSession(1));
    assertFalse(sessionManager.reserveSession(1));
    assertEquals(0, globalActiveSessions.get());

    QuicStreamChannel stream = Mockito.mock(QuicStreamChannel.class);
    when(stream.streamId()).thenReturn(4L);
    when(stream.parent()).thenReturn(mockQuicChannel);
    sessionManager.registerReserved(stream);

    assertEquals(1, globalActiveSessions.get());
    assertEquals(1, globalSlots.get());
    assertFalse(sessionManager.reserveSession(1));

    sessionManager.unregister(stream);
    assertEquals(0, globalActiveSessions.get());
    assertEquals(0, globalSlots.get());
    assertTrue(sessionManager.reserveSession(1));
    sessionManager.releaseReservation();
  }

  @Test
  public void testPendingReservationReleasedWithoutRegistration() {
    assertTrue(sessionManager.reserveSession(1));
    assertFalse(sessionManager.reserveSession(1));
    sessionManager.releaseReservation();
    assertTrue(sessionManager.reserveSession(1));
    sessionManager.releaseReservation();
    assertEquals(0, sessionManager.sessionsSize());
    assertEquals(0, globalActiveSessions.get());
  }

  @Test
  public void testCloseAllAndUnregisterNoDoubleDecrement() {
    AtomicInteger globalSlots = new AtomicInteger(0);
    io.netty.util.Attribute<AtomicInteger> slotsAttr = Mockito.mock(io.netty.util.Attribute.class);
    when(slotsAttr.get()).thenReturn(globalSlots);
    when(mockQuicChannel.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS)).thenReturn(slotsAttr);

    QuicStreamChannel[] streams = new QuicStreamChannel[3];
    for (int i = 0; i < 3; i++) {
      streams[i] = Mockito.mock(QuicStreamChannel.class);
      when(streams[i].streamId()).thenReturn((long) (i + 1));
      when(streams[i].parent()).thenReturn(mockQuicChannel);
      sessionManager.register(streams[i]);
    }

    assertEquals(3, sessionManager.sessionsSize());
    assertEquals(3, sessionManager.getOccupiedSlots());
    assertEquals(3, globalActiveSessions.get());
    assertEquals(3, globalSlots.get());

    // 1. First, call closeAll
    sessionManager.closeAll(mockQuicChannel);

    assertEquals(0, sessionManager.sessionsSize());
    assertEquals(0, sessionManager.getOccupiedSlots());
    assertEquals(0, globalActiveSessions.get());
    assertEquals(0, globalSlots.get());

    // 2. Simulate subsequent Netty stream close listener invoking unregister on each stream
    for (QuicStreamChannel stream : streams) {
      sessionManager.unregister(stream);
    }

    // Verify slots and counts never underflow/double-decrement
    assertEquals(0, sessionManager.sessionsSize());
    assertEquals(0, sessionManager.getOccupiedSlots());
    assertEquals(0, globalActiveSessions.get());
    assertEquals(0, globalSlots.get());
  }

  @Test
  public void testInterleavedUnregisterAndCloseAll() {
    AtomicInteger globalSlots = new AtomicInteger(0);
    io.netty.util.Attribute<AtomicInteger> slotsAttr = Mockito.mock(io.netty.util.Attribute.class);
    when(slotsAttr.get()).thenReturn(globalSlots);
    when(mockQuicChannel.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS)).thenReturn(slotsAttr);

    QuicStreamChannel[] streams = new QuicStreamChannel[4];
    for (int i = 0; i < 4; i++) {
      streams[i] = Mockito.mock(QuicStreamChannel.class);
      when(streams[i].streamId()).thenReturn((long) (i + 10));
      when(streams[i].parent()).thenReturn(mockQuicChannel);
      sessionManager.register(streams[i]);
    }

    assertEquals(4, sessionManager.getOccupiedSlots());
    assertEquals(4, globalActiveSessions.get());
    assertEquals(4, globalSlots.get());

    // Unregister stream 0 and 1 explicitly first
    sessionManager.unregister(streams[0]);
    sessionManager.unregister(streams[1]);

    assertEquals(2, sessionManager.sessionsSize());
    assertEquals(2, sessionManager.getOccupiedSlots());
    assertEquals(2, globalActiveSessions.get());
    assertEquals(2, globalSlots.get());

    // Now call closeAll for remaining sessions
    sessionManager.closeAll(mockQuicChannel);

    assertEquals(0, sessionManager.sessionsSize());
    assertEquals(0, sessionManager.getOccupiedSlots());
    assertEquals(0, globalActiveSessions.get());
    assertEquals(0, globalSlots.get());

    // Re-unregister all streams (simulating delayed Netty close handlers)
    for (QuicStreamChannel stream : streams) {
      sessionManager.unregister(stream);
    }

    assertEquals(0, sessionManager.sessionsSize());
    assertEquals(0, sessionManager.getOccupiedSlots());
    assertEquals(0, globalActiveSessions.get());
    assertEquals(0, globalSlots.get());
  }
}
