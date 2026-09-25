package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Test;

/**
 * Tests verifying late HTTP/3 SETTINGS flow-control initialization and server-initiated stream accounting.
 */
public class Http3ControlStreamAndAccountingTest {

  @Test
  public void testLateSettingsEnablesFlowControlAndAppliesFallbacks() {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockParent.hasAttr(any())).thenAnswer(inv -> parentAttrMap.hasAttr(inv.getArgument(0)));

    // Local server settings declared with non-zero values
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(100L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(10000L);

    // Initial state: peer settings not yet received
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(false);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    // Connect stream for early-arriving session
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    DefaultChannelPromise writePromise =
        new DefaultChannelPromise(mockConnectStream, ImmediateEventExecutor.INSTANCE);
    when(mockConnectStream.writeAndFlush(any())).thenAnswer(inv -> {
      io.netty.util.ReferenceCountUtil.release(inv.getArgument(0));
      return writePromise;
    });

    // Register session before peer settings arrive (flowControlEnabled will be false)
    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(0L);
    org.junit.Assert.assertNotNull(session);
    session.setSettingsMaxStreamsUni(0L);
    session.setSettingsMaxStreamsBidi(0L);
    session.setSettingsMaxData(0L);

    assertFalse(session.isFlowControlEnabled());

    // Peer SETTINGS arrive with H3 datagram and flow control enabled
    Http3Settings peerSettings = new Http3Settings((id, value) -> true);
    peerSettings.enableH3Datagram(true);
    peerSettings.put(0x2b64L, 50L); // peer uni
    peerSettings.put(0x2b65L, 50L); // peer bidi
    peerSettings.put(0x2b61L, 50000L); // peer data

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead0(mockCtx, new DefaultHttp3SettingsFrame(peerSettings));

    // Verify session transitioned to flow control enabled
    assertTrue(session.isFlowControlEnabled());
    assertEquals(50L, session.getPeerSettingsMaxStreamsUni());
    assertEquals(50L, session.getPeerSettingsMaxStreamsBidi());
    assertEquals(50000L, session.getPeerSettingsMaxData());

    // Verify fallback limits were applied for zero initial limits
    assertTrue(session.getSettingsMaxStreamsUni() > 0L);
    assertTrue(session.getSettingsMaxStreamsBidi() > 0L);
    assertTrue(session.getSettingsMaxData() > 0L);

    // Verify capsules were sent back
    verify(mockConnectStream, atLeastOnce()).writeAndFlush(any());
  }

  @Test
  public void testServerInitiatedStreamAccountingIncrementsWhenFlowControlDisabledOrBypassed() {
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);
    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockParent.eventLoop()).thenReturn(mockEventLoop);
    when(mockEventLoop.newPromise()).thenReturn(
        new io.netty.util.concurrent.DefaultPromise<>(ImmediateEventExecutor.INSTANCE));

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockParent.hasAttr(any())).thenAnswer(inv -> parentAttrMap.hasAttr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    DefaultChannelPromise writePromise =
        new DefaultChannelPromise(mockConnectStream, ImmediateEventExecutor.INSTANCE);
    when(mockConnectStream.writeAndFlush(any())).thenAnswer(inv -> {
      io.netty.util.ReferenceCountUtil.release(inv.getArgument(0));
      return writePromise;
    });

    io.netty.util.concurrent.Promise<QuicStreamChannel> futureChannel =
        new io.netty.util.concurrent.DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
    when(mockParent.createStream(any(), any())).thenReturn(futureChannel);

    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(0L);
    org.junit.Assert.assertNotNull(session);
    session.setFlowControlEnabled(false);

    assertEquals(0L, session.getServerInitiatedStreamsBidi());
    assertEquals(0L, session.getServerInitiatedStreamsUni());

    // Create stream with flow control disabled
    ChannelHandler noopHandler = mock(ChannelHandler.class);
    WebTransportUtils.createBiStream(mockConnectStream, false, noopHandler);
    assertEquals(1L, session.getServerInitiatedStreamsBidi());

    WebTransportUtils.createUniStream(mockConnectStream, false, noopHandler);
    assertEquals(1L, session.getServerInitiatedStreamsUni());

    // Create stream with bypassLimit = true
    WebTransportUtils.createBiStream(mockConnectStream, true, noopHandler);
    assertEquals(2L, session.getServerInitiatedStreamsBidi());

    WebTransportUtils.createUniStream(mockConnectStream, true, noopHandler);
    assertEquals(2L, session.getServerInitiatedStreamsUni());
  }
}
