package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import io.github.webtransport4j.server.*;
import io.github.webtransport4j.example.*;


import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import org.junit.Test;

public class SessionFlowControlTest {

  @SuppressWarnings("unchecked")
  private <T> Attribute<T> mockAttribute(T value) {
    Attribute<T> attr = mock(Attribute.class);
    when(attr.get()).thenReturn(value);
    return attr;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReceiveMaxDataCapsuleUpdatesPeerLimit() throws Exception {
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(123L);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 100L) with initial limits
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    Attribute<Long> sessIdAttr = mockAttribute(100L);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    Attribute<Long> bidiLimitAttr = mockAttribute(5L);
    Attribute<Long> uniLimitAttr = mockAttribute(5L);
    Attribute<Long> localDataLimitAttr = mockAttribute(1000L);
    Attribute<Long> peerDataLimitAttr = mockAttribute(1000L);

    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(bidiLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(uniLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(localDataLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)).thenReturn(peerDataLimitAttr);

    mgr.register(mockConnectStream);

    WebTransportSession session = mgr.get(100L);
    assertNotNull(session);
    assertEquals(1000L, session.getPeerSettingsMaxData());

    // Construct WT_MAX_DATA capsule (0x190B4D3DL) with maxData = 2000
    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 2000);
    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x190B4D3DL, payload);

    dispatcher.channelRead(mockCtx, capsule);

    // Assert: Peer max data updated
    assertEquals(2000L, session.getPeerSettingsMaxData());
    assertEquals(0, capsule.refCnt());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReceiveSmallerMaxDataCapsuleClosesSession() throws Exception {
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(123L);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 100L) with initial limits
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    Attribute<Long> sessIdAttr = mockAttribute(100L);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    Attribute<Long> bidiLimitAttr = mockAttribute(5L);
    Attribute<Long> uniLimitAttr = mockAttribute(5L);
    Attribute<Long> localDataLimitAttr = mockAttribute(1000L);
    Attribute<Long> peerDataLimitAttr = mockAttribute(1000L);

    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(bidiLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(uniLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(localDataLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)).thenReturn(peerDataLimitAttr);

    mgr.register(mockConnectStream);

    // Construct WT_MAX_DATA capsule (0x190B4D3DL) with maxData = 500 (less than 1000)
    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 500);
    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x190B4D3DL, payload);

    dispatcher.channelRead(mockCtx, capsule);

    // Assert: Connect stream shutdown with WT_FLOW_CONTROL_ERROR (0x045d4487)
    verify(mockConnectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
    assertEquals(0, capsule.refCnt());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testIncomingDataExceedingLimitClosesSession() throws Exception {
    RawWebTransportHandler handler = new RawWebTransportHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(123L);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    when(mockCtx.newPromise()).thenReturn(mock(ChannelPromise.class));

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 100L) with settingsMaxData = 100
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    Attribute<Long> sessIdAttr = mockAttribute(100L);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    Attribute<Long> bidiLimitAttr = mockAttribute(5L);
    Attribute<Long> uniLimitAttr = mockAttribute(5L);
    Attribute<Long> localDataLimitAttr = mockAttribute(100L);
    Attribute<Long> peerDataLimitAttr = mockAttribute(1000L);

    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(bidiLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(uniLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(localDataLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)).thenReturn(peerDataLimitAttr);

    mgr.register(mockConnectStream);

    // Set up stream properties
    Attribute<Long> streamSessIdAttr = mockAttribute(100L);
    Attribute<Long> streamTypeAttr = mockAttribute(0x41L);
    Attribute<Boolean> serverInitAttr = mockAttribute(true);

    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(streamSessIdAttr);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);
    when(mockStream.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY)).thenReturn(serverInitAttr);

    // Send 50 bytes (below limit of 100)
    ByteBuf data1 = Unpooled.copiedBuffer(new byte[50]);
    handler.channelRead(mockCtx, data1);
    verify(mockCtx).fireChannelRead(data1);

    // Send 51 bytes (total 101, exceeds limit of 100)
    ByteBuf data2 = Unpooled.copiedBuffer(new byte[51]);
    handler.channelRead(mockCtx, data2);

    // Verify: Connect stream shutdown with WT_FLOW_CONTROL_ERROR (0x045d4487)
    verify(mockConnectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
    // Verify: Stream channel itself shutdown with WT_FLOW_CONTROL_ERROR
    verify(mockStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
    assertEquals(0, data2.refCnt());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOutgoingDataExceedingLimitBuffersAndFlushes() throws Exception {
    RawWebTransportHandler handler = new RawWebTransportHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(123L);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    io.netty.channel.ChannelFuture mockCloseFuture = mock(io.netty.channel.ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);

    io.netty.channel.EventLoop mockEventLoop = mock(io.netty.channel.EventLoop.class);
    when(mockStream.eventLoop()).thenReturn(mockEventLoop);
    when(mockEventLoop.inEventLoop()).thenReturn(true);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 100L) with peerSettingsMaxData = 100
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    Attribute<Long> sessIdAttr = mockAttribute(100L);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    Attribute<Long> bidiLimitAttr = mockAttribute(5L);
    Attribute<Long> uniLimitAttr = mockAttribute(5L);
    Attribute<Long> localDataLimitAttr = mockAttribute(1000L);
    Attribute<Long> peerDataLimitAttr = mockAttribute(100L);

    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(bidiLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(uniLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(localDataLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)).thenReturn(peerDataLimitAttr);

    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(100L);

    // Set up stream properties
    Attribute<Long> streamSessIdAttr = mockAttribute(100L);
    Attribute<Boolean> serverInitAttr = mockAttribute(false);

    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(streamSessIdAttr);
    when(mockStream.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY)).thenReturn(serverInitAttr);

    // Write 60 bytes (below limit of 100)
    ChannelPromise promise1 = mock(ChannelPromise.class);
    ByteBuf data1 = Unpooled.copiedBuffer(new byte[60]);
    handler.write(mockCtx, data1, promise1);
    verify(mockCtx).write(data1, promise1);

    // Write 50 bytes (total 110, exceeds limit of 100)
    ChannelPromise promise2 = mock(ChannelPromise.class);
    ByteBuf data2 = Unpooled.copiedBuffer(new byte[50]);
    handler.write(mockCtx, data2, promise2);

    // Verify: promise2 is NOT failed, write is buffered
    verify(promise2, never()).setFailure(any());
    assertEquals(1, session.getPendingWrites().size());
    assertEquals(data2, session.getPendingWrites().peek().getData());

    // Verify: WT_DATA_BLOCKED capsule was sent back to connectStream
    verify(mockConnectStream).writeAndFlush(any());

    // Now update limit to 200 via WT_MAX_DATA capsule
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 200);
    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x190B4D3DL, payload);

    dispatcher.channelRead(mockCtx, capsule);

    // Verify: pending write was flushed (written to stream channel)
    verify(mockStream).writeAndFlush(data2, promise2);
    assertEquals(0, session.getPendingWrites().size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReceiveDataBlockedExtendsLimit() throws Exception {
    WebTransportCapsuleHandler dispatcher = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(123L);
    when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    // Session Manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Register session (ID = 100L) with initialMaxData = 1000
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    Attribute<Long> sessIdAttr = mockAttribute(100L);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    Attribute<Long> bidiLimitAttr = mockAttribute(5L);
    Attribute<Long> uniLimitAttr = mockAttribute(5L);
    Attribute<Long> localDataLimitAttr = mockAttribute(1000L);
    Attribute<Long> peerDataLimitAttr = mockAttribute(1000L);

    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(bidiLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(uniLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(localDataLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)).thenReturn(peerDataLimitAttr);

    mgr.register(mockConnectStream);

    WebTransportSession session = mgr.get(100L);
    assertNotNull(session);
    assertEquals(1000L, session.getSettingsMaxData());

    // Construct WT_DATA_BLOCKED capsule (0x190B4D41L)
    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 1000);
    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x190B4D41L, payload);

    dispatcher.channelRead(mockCtx, capsule);

    // Assert: settingsMaxData extended from 1000 by the configured extend amount
    long extendAmount =
        WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.extend.data", 10000L);
    assertEquals(1000L + extendAmount, session.getSettingsMaxData());

    // Verify: WT_MAX_DATA capsule sent to connectStream
    verify(mockConnectStream).writeAndFlush(any());
    assertEquals(0, capsule.refCnt());
  }
}
