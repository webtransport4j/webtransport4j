package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.github.webtransport4j.api.WebTransportSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/** Test cases for stream buffering. */
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

  @SuppressWarnings("unchecked")
  @Test
  public void testFragmentedHeaderParsing() throws Exception {
    // 1. Mock Parent QUIC Channel and Session Manager
    QuicChannel mockParent = mock(QuicChannel.class);
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Mock DEFAULT connection limits so they are read by register()
    Attribute<Long> defaultBidiAttr = mock(Attribute.class);
    when(defaultBidiAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(defaultBidiAttr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));
    mgr.register(mockConnectStream);

    // 2. Mock Stream Channel and context
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(200L);
    when(mockStream.type()).thenReturn(io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL);
    io.netty.handler.codec.quic.QuicStreamChannelConfig mockConfig =
        mock(io.netty.handler.codec.quic.QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockStream.pipeline()).thenReturn(mockPipeline);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);
    when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockCtx.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    // Setup channel attributes
    Attribute<Long> typeAttr = mock(Attribute.class);
    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(typeAttr);
    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    // 3. Prepare data with WT Headers (Session ID 100, BIDI stream type 0x41) + Payload
    ByteBuf data = Unpooled.buffer();
    WebTransportUtils.writeVarInt(data, 0x41);
    WebTransportUtils.writeVarInt(data, 100);
    data.writeBytes("Hello".getBytes(StandardCharsets.UTF_8));

    int totalBytes = data.readableBytes();
    assertTrue(totalBytes > 4);

    RawWebTransportHandler handler = new RawWebTransportHandler();

    // Send first piece
    ByteBuf piece1 = data.readRetainedSlice(1); // 1st byte of streamType
    handler.channelRead(mockCtx, piece1);
    // Verify no fireChannelRead happened yet
    verify(mockCtx, never()).fireChannelRead(any());

    // Send second piece
    ByteBuf piece2 = data.readRetainedSlice(1); // 1st byte of sessionId
    handler.channelRead(mockCtx, piece2);
    // Verify no fireChannelRead happened yet
    verify(mockCtx, never()).fireChannelRead(any());

    // Mock capturing fired object
    final ByteBuf[] firedPayload = new ByteBuf[1];
    doAnswer(
            invocation -> {
              firedPayload[0] = invocation.getArgument(0);
              return null;
            })
        .when(mockCtx)
        .fireChannelRead(any());

    // Send third piece (completes headers + delivers payload)
    ByteBuf piece3 = data.readRetainedSlice(totalBytes - 2); // rest of sessionId + payload
    handler.channelRead(mockCtx, piece3);

    // Verify fireChannelRead was called once
    verify(mockCtx, times(1)).fireChannelRead(any());
    assertNotNull(firedPayload[0]);
    assertEquals("Hello", firedPayload[0].toString(StandardCharsets.UTF_8));

    // Clean up
    firedPayload[0].release();
    data.release();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testByteByByteHeaderParsing() throws Exception {
    // 1. Mock Parent QUIC Channel and Session Manager
    QuicChannel mockParent = mock(QuicChannel.class);
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Mock DEFAULT connection limits
    Attribute<Long> defaultBidiAttr = mock(Attribute.class);
    when(defaultBidiAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(defaultBidiAttr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(50L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));
    mgr.register(mockConnectStream);

    // 2. Mock Stream Channel and context
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(200L);
    when(mockStream.type()).thenReturn(io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL);
    io.netty.handler.codec.quic.QuicStreamChannelConfig mockConfig =
        mock(io.netty.handler.codec.quic.QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockStream.pipeline()).thenReturn(mockPipeline);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);
    when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockCtx.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    // Setup channel attributes
    Attribute<Long> typeAttr = mock(Attribute.class);
    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(typeAttr);
    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    // 3. Prepare data: Stream Type 0x41 (Bidi, 1 byte) + Session ID 50 (fits in 1 byte) + Payload
    // "Hi"
    ByteBuf data = Unpooled.buffer();
    WebTransportUtils.writeVarInt(data, 0x41);
    WebTransportUtils.writeVarInt(data, 50);
    data.writeBytes("Hi".getBytes(StandardCharsets.UTF_8));

    // 0x41 (65) is written as 2 bytes, 50 is written as 1 byte, "Hi" is 2 bytes. Total = 5 bytes.
    int totalBytes = data.readableBytes();
    assertEquals(5, totalBytes);

    RawWebTransportHandler handler = new RawWebTransportHandler();

    // Feed piece 1 (Stream Type Part 1)
    ByteBuf p1 = data.readRetainedSlice(1); // 0x41 Part 1
    handler.channelRead(mockCtx, p1);
    verify(mockCtx, never()).fireChannelRead(any());

    // Feed piece 2 (Stream Type Part 2)
    ByteBuf p2 = data.readRetainedSlice(1); // 0x41 Part 2
    handler.channelRead(mockCtx, p2);
    verify(mockCtx, never()).fireChannelRead(any());

    // Feed piece 3 (Session ID) -> Completes headers, but no payload is present in this write, so
    // no fire yet
    ByteBuf p3 = data.readRetainedSlice(1); // 0x32 (Session ID)
    handler.channelRead(mockCtx, p3);
    verify(mockCtx, never()).fireChannelRead(any());

    // Mock capturing fired object
    final java.util.List<ByteBuf> firedPayloads = new java.util.ArrayList<>();
    doAnswer(
            invocation -> {
              firedPayloads.add(invocation.getArgument(0));
              return null;
            })
        .when(mockCtx)
        .fireChannelRead(any());

    // Feed piece 4 ('H') -> Fired immediately because headers are already consumed
    ByteBuf p4 = data.readRetainedSlice(1); // 'H' (Payload)
    handler.channelRead(mockCtx, p4);
    assertEquals(1, firedPayloads.size());
    assertEquals("H", firedPayloads.get(0).toString(StandardCharsets.UTF_8));

    // Feed piece 5 ('i') -> Fired immediately
    ByteBuf p5 = data.readRetainedSlice(1); // 'i' (Payload)
    handler.channelRead(mockCtx, p5);
    assertEquals(2, firedPayloads.size());
    assertEquals("i", firedPayloads.get(1).toString(StandardCharsets.UTF_8));

    // Clean up
    for (ByteBuf buf : firedPayloads) {
      buf.release();
    }
    data.release();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLargeSessionIdHeaderFragmentation() throws Exception {
    // 1. Mock Parent QUIC Channel and Session Manager
    QuicChannel mockParent = mock(QuicChannel.class);
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Mock DEFAULT connection limits
    Attribute<Long> defaultBidiAttr = mock(Attribute.class);
    when(defaultBidiAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(defaultBidiAttr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100000L); // Session ID requires 4 bytes
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));
    mgr.register(mockConnectStream);

    // 2. Mock Stream Channel and context
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(200L);
    when(mockStream.type()).thenReturn(io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL);
    io.netty.handler.codec.quic.QuicStreamChannelConfig mockConfig =
        mock(io.netty.handler.codec.quic.QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockStream.pipeline()).thenReturn(mockPipeline);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);
    when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockCtx.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    // Setup channel attributes
    Attribute<Long> typeAttr = mock(Attribute.class);
    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(typeAttr);
    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    // 3. Prepare data: Stream Type 0x41 (1 byte) + Session ID 100000 (4 bytes) + Payload "World"
    ByteBuf data = Unpooled.buffer();
    WebTransportUtils.writeVarInt(data, 0x41);
    WebTransportUtils.writeVarInt(data, 100000L);
    data.writeBytes("World".getBytes(StandardCharsets.UTF_8));

    // 0x41 (65) is written as 2 bytes, 100000 is written as 4 bytes, "World" is 5 bytes. Total = 11
    // bytes.
    int totalBytes = data.readableBytes();
    assertEquals(11, totalBytes);

    RawWebTransportHandler handler = new RawWebTransportHandler();

    // Feed pieces 1 to 6 one by one
    ByteBuf piece1 = data.readRetainedSlice(1); // Type part 1
    handler.channelRead(mockCtx, piece1);
    verify(mockCtx, never()).fireChannelRead(any());

    ByteBuf piece2 = data.readRetainedSlice(1); // Type part 2
    handler.channelRead(mockCtx, piece2);
    verify(mockCtx, never()).fireChannelRead(any());

    // Mock capturing fired object
    final ByteBuf[] firedPayload = new ByteBuf[1];
    doAnswer(
            invocation -> {
              firedPayload[0] = invocation.getArgument(0);
              return null;
            })
        .when(mockCtx)
        .fireChannelRead(any());

    ByteBuf piece3 = data.readRetainedSlice(1); // Session ID byte 1
    handler.channelRead(mockCtx, piece3);
    verify(mockCtx, never()).fireChannelRead(any());

    ByteBuf piece4 = data.readRetainedSlice(1); // Session ID byte 2
    handler.channelRead(mockCtx, piece4);
    verify(mockCtx, never()).fireChannelRead(any());

    ByteBuf piece5 = data.readRetainedSlice(1); // Session ID byte 3
    handler.channelRead(mockCtx, piece5);
    verify(mockCtx, never()).fireChannelRead(any());

    ByteBuf piece6 = data.readRetainedSlice(1); // Session ID byte 4
    handler.channelRead(mockCtx, piece6);
    verify(mockCtx, never()).fireChannelRead(any());

    // Feed piece 7 (completes header + has remaining payload "World" in the same write)
    ByteBuf piece7 = data.readRetainedSlice(5); // Payload "World"
    handler.channelRead(mockCtx, piece7);

    // Verify payload is fired
    verify(mockCtx, times(1)).fireChannelRead(any());
    assertNotNull(firedPayload[0]);
    assertEquals("World", firedPayload[0].toString(StandardCharsets.UTF_8));

    // Clean up
    firedPayload[0].release();
    data.release();
  }
}
