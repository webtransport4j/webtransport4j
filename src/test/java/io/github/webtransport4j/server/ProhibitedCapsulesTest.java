package io.github.webtransport4j.server;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import org.junit.Test;

/**
 * Tests enforcing draft-16 Section 5.4 prohibition of stream-level flow control
 * capsules (WT_MAX_STREAM_DATA and WT_STREAM_DATA_BLOCKED).
 */
public class ProhibitedCapsulesTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testMaxStreamDataCapsuleClosesSessionWithFlowControlError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(200L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    Attribute<Long> limitAttr = mock(Attribute.class);
    when(limitAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(limitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(limitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA)).thenReturn(limitAttr);

    mgr.register(mockConnectStream);
    assertNotNull(mgr.get(200L));

    // Send WT_MAX_STREAM_DATA capsule (0x190B4D3E)
    // Constructor: WebTransportCapsule(sessionId, capsuleType, payload)
    ByteBuf payload = Unpooled.buffer();
    payload.writeByte(1); // stream ID
    payload.writeByte(100); // max stream data
    WebTransportCapsule capsule = new WebTransportCapsule(200L, 0x190B4D3EL, payload);

    WebTransportCapsuleHandler.INSTANCE.channelRead0(mockCtx, capsule);

    // Verify connectStream was shut down with WT_FLOW_CONTROL_ERROR (0x045d4487)
    verify(mockConnectStream).shutdown(eq(0x045d4487), any());
    payload.release();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamDataBlockedCapsuleClosesSessionWithFlowControlError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(204L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    Attribute<Long> limitAttr = mock(Attribute.class);
    when(limitAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(limitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(limitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA)).thenReturn(limitAttr);

    mgr.register(mockConnectStream);
    assertNotNull(mgr.get(204L));

    // Send WT_STREAM_DATA_BLOCKED capsule (0x190B4D42)
    // Constructor: WebTransportCapsule(sessionId, capsuleType, payload)
    ByteBuf payload = Unpooled.buffer();
    payload.writeByte(1); // stream ID
    payload.writeByte(100); // stream data limit
    WebTransportCapsule capsule = new WebTransportCapsule(204L, 0x190B4D42L, payload);

    WebTransportCapsuleHandler.INSTANCE.channelRead0(mockCtx, capsule);

    // Verify connectStream was shut down with WT_FLOW_CONTROL_ERROR (0x045d4487)
    verify(mockConnectStream).shutdown(eq(0x045d4487), any());
    payload.release();
  }
}
