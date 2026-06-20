package io.github.webtransport4j.incubator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StreamsBlockedCapsuleTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testBidiStreamsBlockedExtendsLimit() throws Exception {
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 100L) with initial limits (maxStreamsUni = 10, maxStreamsBidi = 5)
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    when(mockConnectStream.attr(WebTransportUtils.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));

    Attribute<Long> limitAttr = mock(Attribute.class);
    when(limitAttr.get()).thenReturn(5L);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(limitAttr);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_UNI)).thenReturn(limitAttr);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_DATA)).thenReturn(limitAttr);

    mgr.register(mockConnectStream);

    WebTransportSession session = mgr.get(100L);
    assertNotNull(session);
    assertEquals(5L, session.getInitialMaxStreamsBidi());

    // Simulate 2 active client-initiated bidi streams (remaining active slots = 5 - 2 = 3)
    session.getActiveClientInitiatedBi().add(mock(QuicStreamChannel.class));
    session.getActiveClientInitiatedBi().add(mock(QuicStreamChannel.class));
    assertEquals(2, session.getActiveClientInitiatedBi().size());

    // Construct WT_STREAMS_BLOCKED capsule (BIDI = 0x190B4D43L) with maxStreams = 10
    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 10);
    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x190B4D43L, payload);

    dispatcher.channelRead(mockCtx, capsule);

    // Assert: New limit = 10 (blocked max) + 3 (remaining allowed) = 13
    assertEquals(13L, session.getSettingsMaxStreamsBidi());

    // Verify WT_MAX_STREAMS capsule was sent back to connectStream
    ArgumentCaptor<Object> writeCaptor = ArgumentCaptor.forClass(Object.class);
    verify(mockConnectStream).writeAndFlush(writeCaptor.capture());
    assertNotNull(writeCaptor.getValue());

    // Capsule was released
    assertEquals(0, capsule.refCnt());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUniStreamsBlockedExtendsLimit() throws Exception {
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 200L) with initial limits (maxStreamsUni = 8, maxStreamsBidi = 5)
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(200L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    when(mockConnectStream.attr(WebTransportUtils.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));

    Attribute<Long> uniLimitAttr = mock(Attribute.class);
    when(uniLimitAttr.get()).thenReturn(8L);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(uniLimitAttr);

    Attribute<Long> bidiLimitAttr = mock(Attribute.class);
    when(bidiLimitAttr.get()).thenReturn(5L);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(bidiLimitAttr);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_DATA)).thenReturn(bidiLimitAttr);

    mgr.register(mockConnectStream);

    WebTransportSession session = mgr.get(200L);
    assertNotNull(session);
    assertEquals(8L, session.getInitialMaxStreamsUni());

    // Simulate 3 active client-initiated uni streams (remaining active slots = 8 - 3 = 5)
    session.getActiveClientInitiatedUni().add(mock(QuicStreamChannel.class));
    session.getActiveClientInitiatedUni().add(mock(QuicStreamChannel.class));
    session.getActiveClientInitiatedUni().add(mock(QuicStreamChannel.class));
    assertEquals(3, session.getActiveClientInitiatedUni().size());

    // Construct WT_STREAMS_BLOCKED capsule (UNI = 0x190B4D44L) with maxStreams = 20
    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 20);
    WebTransportCapsule capsule = new WebTransportCapsule(200L, 0x190B4D44L, payload);

    dispatcher.channelRead(mockCtx, capsule);

    // Assert: New limit = 20 (blocked max) + 5 (remaining allowed) = 25
    assertEquals(25L, session.getSettingsMaxStreamsUni());

    // Verify WT_MAX_STREAMS capsule was sent back to connectStream
    verify(mockConnectStream).writeAndFlush(any());

    // Capsule was released
    assertEquals(0, capsule.refCnt());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamsBlockedNoExtensionIfAtLimit() throws Exception {
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 300L) with initial limits (maxStreamsBidi = 2)
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(300L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    when(mockConnectStream.attr(WebTransportUtils.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));

    Attribute<Long> limitAttr = mock(Attribute.class);
    when(limitAttr.get()).thenReturn(2L);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(limitAttr);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_UNI)).thenReturn(limitAttr);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_DATA)).thenReturn(limitAttr);

    mgr.register(mockConnectStream);

    WebTransportSession session = mgr.get(300L);
    assertNotNull(session);
    assertEquals(2L, session.getInitialMaxStreamsBidi());

    // Simulate 2 active client-initiated bidi streams (remaining active slots = 2 - 2 = 0)
    session.getActiveClientInitiatedBi().add(mock(QuicStreamChannel.class));
    session.getActiveClientInitiatedBi().add(mock(QuicStreamChannel.class));
    assertEquals(2, session.getActiveClientInitiatedBi().size());

    // Construct WT_STREAMS_BLOCKED capsule (BIDI = 0x190B4D43L) with maxStreams = 2
    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 2);
    WebTransportCapsule capsule = new WebTransportCapsule(300L, 0x190B4D43L, payload);

    dispatcher.channelRead(mockCtx, capsule);

    // Assert: Limit was NOT extended, remains at 2 (since remaining allowed active slots was 0)
    assertEquals(2L, session.getSettingsMaxStreamsBidi());

    // Verify WT_MAX_STREAMS capsule was NOT sent back
    verify(mockConnectStream, never()).writeAndFlush(any());

    // Capsule was released
    assertEquals(0, capsule.refCnt());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSequentialLimitExtensions() throws Exception {
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 400L) with initial limits (maxStreamsBidi = 5)
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(400L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    when(mockConnectStream.attr(WebTransportUtils.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));

    Attribute<Long> limitAttr = mock(Attribute.class);
    when(limitAttr.get()).thenReturn(5L);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(limitAttr);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_STREAMS_UNI)).thenReturn(limitAttr);
    when(mockParent.attr(WebTransportConfig.LOCAL_SETTINGS_MAX_DATA)).thenReturn(limitAttr);

    mgr.register(mockConnectStream);

    WebTransportSession session = mgr.get(400L);
    assertNotNull(session);
    assertEquals(5L, session.getInitialMaxStreamsBidi());

    // Step 1: Simulate 2 active client-initiated bidi streams (remaining = 5 - 2 = 3)
    QuicStreamChannel s1 = mock(QuicStreamChannel.class);
    QuicStreamChannel s2 = mock(QuicStreamChannel.class);
    session.getActiveClientInitiatedBi().add(s1);
    session.getActiveClientInitiatedBi().add(s2);
    assertEquals(2, session.getActiveClientInitiatedBi().size());

    // Construct WT_STREAMS_BLOCKED at 10
    ByteBuf payload1 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload1, 10);
    WebTransportCapsule capsule1 = new WebTransportCapsule(400L, 0x190B4D43L, payload1);

    dispatcher.channelRead(mockCtx, capsule1);

    // Assert: New limit = 10 (blocked max) + 3 (remaining allowed) = 13
    assertEquals(13L, session.getSettingsMaxStreamsBidi());
    verify(mockConnectStream, times(1)).writeAndFlush(any());
    assertEquals(0, capsule1.refCnt());

    // Step 2: Peer closes 1 stream (active count becomes 1, remaining = 5 - 1 = 4)
    session.getActiveClientInitiatedBi().remove(s1);
    assertEquals(1, session.getActiveClientInitiatedBi().size());

    // Construct WT_STREAMS_BLOCKED at 13
    ByteBuf payload2 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload2, 13);
    WebTransportCapsule capsule2 = new WebTransportCapsule(400L, 0x190B4D43L, payload2);

    dispatcher.channelRead(mockCtx, capsule2);

    // Assert: New limit = 13 (blocked max) + 4 (remaining allowed) = 17
    assertEquals(17L, session.getSettingsMaxStreamsBidi());
    verify(mockConnectStream, times(2)).writeAndFlush(any());
    assertEquals(0, capsule2.refCnt());
  }
}
