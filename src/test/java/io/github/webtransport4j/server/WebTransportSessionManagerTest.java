package io.github.webtransport4j.server;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WebTransportSessionManager} session lifecycle, bulk cleanup,
 * and global session counter management under connection teardown.
 */
public class WebTransportSessionManagerTest {

  private WebTransportSessionManager sessionManager;
  private AtomicInteger globalActiveSessions;
  private QuicChannel mockQuicChannel;

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
}
