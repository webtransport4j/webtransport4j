package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class StreamBufferingTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testOutOfOrderStreamFailsImmediately() throws Exception {
    // 1. Mock Parent QUIC Channel and Session Manager
    QuicChannel mockParent = mock(QuicChannel.class);
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // 2. Mock Stream Channel and context
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(200L);
    io.netty.handler.codec.quic.QuicStreamChannelConfig mockConfig =
        mock(io.netty.handler.codec.quic.QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockStream.pipeline()).thenReturn(mockPipeline);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockCtx.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    // Setup channel attributes
    Attribute<Long> typeAttr = mock(Attribute.class);
    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(typeAttr);
    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    // 3. Prepare data with WT Headers (Session ID 100, BIDI stream type 0x41)
    ByteBuf data = Unpooled.buffer();
    WebTransportUtils.writeVarInt(data, 0x41);
    WebTransportUtils.writeVarInt(data, 100);
    data.writeBytes("Payload".getBytes(StandardCharsets.UTF_8));

    RawWebTransportHandler handler = new RawWebTransportHandler();

    // 4. Inbound read when CONNECT session has NOT been registered yet
    handler.channelRead(mockCtx, data);

    // Verify: stream was shut down with error code WT_BUFFERED_STREAM_REJECTED (0x3994bd84)
    verify(mockStream)
        .shutdown(
            eq(WebTransportUtils.WT_BUFFERED_STREAM_REJECTED),
            any(io.netty.channel.ChannelPromise.class));
    // Verify: data was released
    assertEquals(0, data.refCnt());
    // Verify: no data was fired downstream
    verify(mockCtx, never()).fireChannelRead(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamLimitExceededClosesSession() throws Exception {
    QuicChannel mockParent = mock(QuicChannel.class);

    // Setup Session Manager with a session
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Mock DEFAULT connection limits so they are read by register()
    Attribute<Long> defaultBidiAttr = mock(Attribute.class);
    when(defaultBidiAttr.get()).thenReturn(1L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(defaultBidiAttr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));
    mgr.register(mockConnectStream);

    // Setup current stream count = 1 (already at limit before incrementing)
    WebTransportSession session = mgr.get(100L);
    session.setClientInitiatedStreamsBidi(1L);

    // New incoming stream
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.type()).thenReturn(io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL);
    when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    // Simulate WebTransportServer.java / RawWebTransportHandler.java stream init logic
    boolean isBidi = mockStream.type() == io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL;
    long value =
        isBidi
            ? session.incrementAndGetClientInitiatedStreamsBidi()
            : session.incrementAndGetClientInitiatedStreamsUni();

    long maxAllowed =
        isBidi ? session.getSettingsMaxStreamsBidi() : session.getSettingsMaxStreamsUni();

    if (value > maxAllowed) {
      mgr.closeSessionWithFlowControlError(100L);
      mockStream.shutdown(WebTransportUtils.WT_FLOW_CONTROL_ERROR, mockStream.newPromise());
    }

    // Verify: CONNECT stream was shut down with WT_FLOW_CONTROL_ERROR (0x045d4487)
    verify(mockConnectStream)
        .shutdown(
            eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR),
            any(io.netty.channel.ChannelPromise.class));
    // Verify: the offending stream was shut down with WT_FLOW_CONTROL_ERROR (0x045d4487)
    verify(mockStream)
        .shutdown(
            eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR),
            any(io.netty.channel.ChannelPromise.class));
    // Verify: parent connection was NOT closed
    verify(mockParent, never()).close();
  }

  @Test
  public void testWebTransportConfigFallbackDefaults() {
    assertEquals(
        "default-string", WebTransportConfig.get("nonexistent.key.string", "default-string"));
    assertEquals(42, WebTransportConfig.getInt("nonexistent.key.int", 42));
    assertEquals(100L, WebTransportConfig.getLong("nonexistent.key.long", 100L));
    assertTrue(WebTransportConfig.getBoolean("nonexistent.key.bool", true));
    assertFalse(WebTransportConfig.getBoolean("nonexistent.key.bool.false", false));
  }

  @Test
  public void testConfigValidationSuccess() {
    // QUIC limits are greater than or equal to WT limits
    WebTransportServer.validateConfig(10, 5, 20, 10, 1000, 500);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigValidationBidiMismatch() {
    // QUIC bidi is smaller than WT bidi
    WebTransportServer.validateConfig(4, 5, 20, 10, 1000, 500);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigValidationUniMismatch() {
    // QUIC uni is smaller than WT uni
    WebTransportServer.validateConfig(10, 5, 9, 10, 1000, 500);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigValidationDataMismatch() {
    // QUIC initial max data is smaller than WT initial max data
    WebTransportServer.validateConfig(10, 5, 20, 10, 499, 500);
  }
}
