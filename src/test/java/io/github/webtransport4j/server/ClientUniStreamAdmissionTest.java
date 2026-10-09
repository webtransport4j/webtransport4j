package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Tests for client-initiated WebTransport unidirectional stream admission, accounting, quota
 * enforcement, and active stream registration.
 */
public class ClientUniStreamAdmissionTest {

  @Test
  public void testUniStreamAdmittedAndRegisteredInActiveSet() throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicStreamChannel mockUniStream = mock(QuicStreamChannel.class);

    DefaultAttributeMap quicAttrs = new DefaultAttributeMap();
    DefaultAttributeMap connectAttrs = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();

    when(mockQuic.attr(any())).thenAnswer(inv -> quicAttrs.attr(inv.getArgument(0)));
    when(mockConnectStream.attr(any())).thenAnswer(inv -> connectAttrs.attr(inv.getArgument(0)));
    when(mockUniStream.attr(any())).thenAnswer(inv -> streamAttrs.attr(inv.getArgument(0)));

    when(mockUniStream.parent()).thenReturn(mockQuic);
    when(mockUniStream.streamId()).thenReturn(7L);
    when(mockUniStream.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);

    ChannelPromise closePromise =
        new DefaultChannelPromise(mockUniStream, ImmediateEventExecutor.INSTANCE);
    when(mockUniStream.closeFuture()).thenReturn(closePromise);

    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    quicAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);
    quicAttrs.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set("/test");
    quicAttrs.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);
    mgr.register(mockConnectStream);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.channel()).thenReturn(mockUniStream);

    ByteBuf in = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in, 0L); // sessionId = 0
    in.writeBytes(new byte[] {1, 2, 3});

    WebTransportUniStreamHeaderDecoder decoder =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);
    List<Object> out = new ArrayList<>();
    decoder.decode(mockCtx, in, out);

    // Verify session state and active registration
    NettyWebTransportSession session = mgr.get(0L);
    assertEquals(1L, session.getClientInitiatedStreamsUni());
    assertTrue(session.getActiveClientInitiatedUni().contains(mockUniStream));
    assertEquals(Long.valueOf(0L), streamAttrs.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get());
    assertEquals(
        Long.valueOf(WebTransportUtils.UNI_STREAM_TYPE),
        streamAttrs.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).get());
    assertEquals("/test", streamAttrs.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).get());

    // Out contains payload [1, 2, 3]
    assertEquals(1, out.size());
    ByteBuf payload = (ByteBuf) out.get(0);
    assertEquals(3, payload.readableBytes());
    payload.release();
    in.release();

    // Verify cleanup upon stream closure
    closePromise.setSuccess();
    assertFalse(session.getActiveClientInitiatedUni().contains(mockUniStream));
  }

  @Test
  public void testUniStreamExceedingMaxStreamsRejectedWithFlowControlError() throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);

    DefaultAttributeMap quicAttrs = new DefaultAttributeMap();
    DefaultAttributeMap connectAttrs = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> quicAttrs.attr(inv.getArgument(0)));
    when(mockConnectStream.attr(any())).thenAnswer(inv -> connectAttrs.attr(inv.getArgument(0)));

    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    quicAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    mgr.register(mockConnectStream);
    NettyWebTransportSession session = mgr.get(0L);
    session.setSettingsMaxStreamsUni(1L);

    // Stream 1 (admitted)
    QuicStreamChannel stream1 = mock(QuicStreamChannel.class);
    DefaultAttributeMap stream1Attrs = new DefaultAttributeMap();
    when(stream1.attr(any())).thenAnswer(inv -> stream1Attrs.attr(inv.getArgument(0)));
    when(stream1.parent()).thenReturn(mockQuic);
    when(stream1.streamId()).thenReturn(3L);
    when(stream1.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    when(stream1.closeFuture()).thenReturn(new DefaultChannelPromise(stream1));
    when(stream1.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(stream1.shutdown(any(Integer.class), any())).thenReturn(mock(ChannelFuture.class));

    ChannelHandlerContext ctx1 = mock(ChannelHandlerContext.class);
    when(ctx1.channel()).thenReturn(stream1);

    WebTransportUniStreamHeaderDecoder decoder1 =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);
    ByteBuf in1 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in1, 0L);
    List<Object> out1 = new ArrayList<>();
    decoder1.decode(ctx1, in1, out1);

    assertEquals(1L, session.getClientInitiatedStreamsUni());
    assertTrue(session.getActiveClientInitiatedUni().contains(stream1));
    in1.release();

    // Stream 2 (exceeds limit 1 -> rejected)
    QuicStreamChannel stream2 = mock(QuicStreamChannel.class);
    DefaultAttributeMap stream2Attrs = new DefaultAttributeMap();
    when(stream2.attr(any())).thenAnswer(inv -> stream2Attrs.attr(inv.getArgument(0)));
    when(stream2.parent()).thenReturn(mockQuic);
    when(stream2.streamId()).thenReturn(7L);
    when(stream2.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    when(stream2.newPromise()).thenReturn(mock(ChannelPromise.class));

    ChannelHandlerContext ctx2 = mock(ChannelHandlerContext.class);
    when(ctx2.channel()).thenReturn(stream2);

    WebTransportUniStreamHeaderDecoder decoder2 =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);
    ByteBuf in2 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in2, 0L);
    List<Object> out2 = new ArrayList<>();
    decoder2.decode(ctx2, in2, out2);

    // Verify stream2 was shutdown with WT_FLOW_CONTROL_ERROR
    verify(stream2).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
    // Verify connectStream was shutdown with WT_FLOW_CONTROL_ERROR
    verify(mockConnectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
    assertFalse(session.getActiveClientInitiatedUni().contains(stream2));
    assertEquals(0, out2.size());
    in2.release();
  }

  @Test
  public void testUniStreamWithUnknownSessionIdRejectedWithBufferedStreamRejected()
      throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap quicAttrs = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> quicAttrs.attr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    quicAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    QuicStreamChannel mockUniStream = mock(QuicStreamChannel.class);
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();
    when(mockUniStream.attr(any())).thenAnswer(inv -> streamAttrs.attr(inv.getArgument(0)));
    when(mockUniStream.parent()).thenReturn(mockQuic);
    when(mockUniStream.streamId()).thenReturn(11L);
    when(mockUniStream.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    when(mockUniStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockUniStream);

    WebTransportUniStreamHeaderDecoder decoder =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);

    ByteBuf in = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in, 999L); // non-existent session
    List<Object> out = new ArrayList<>();
    decoder.decode(ctx, in, out);

    verify(mockUniStream).shutdown(eq(WebTransportUtils.WT_BUFFERED_STREAM_REJECTED), any());
    assertEquals(0, out.size());
    in.release();
  }

  @Test
  public void testUniStreamToClosedSessionRejectedWithSessionGone() throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);

    DefaultAttributeMap quicAttrs = new DefaultAttributeMap();
    DefaultAttributeMap connectAttrs = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> quicAttrs.attr(inv.getArgument(0)));
    when(mockConnectStream.attr(any())).thenAnswer(inv -> connectAttrs.attr(inv.getArgument(0)));

    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    quicAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    mgr.register(mockConnectStream);
    NettyWebTransportSession session = mgr.get(0L);
    session.setOnClosedCallback(null);
    session.close();

    QuicStreamChannel mockUniStream = mock(QuicStreamChannel.class);
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();
    when(mockUniStream.attr(any())).thenAnswer(inv -> streamAttrs.attr(inv.getArgument(0)));
    when(mockUniStream.parent()).thenReturn(mockQuic);
    when(mockUniStream.streamId()).thenReturn(15L);
    when(mockUniStream.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    when(mockUniStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockUniStream);

    WebTransportUniStreamHeaderDecoder decoder =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);

    ByteBuf in = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in, 0L);
    List<Object> out = new ArrayList<>();
    decoder.decode(ctx, in, out);

    verify(mockUniStream).shutdown(eq(WebTransportUtils.WT_SESSION_GONE), any());
    assertFalse(session.getActiveClientInitiatedUni().contains(mockUniStream));
    assertEquals(0, out.size());
    in.release();
  }

  @Test
  public void testSessionUnregisterClosesAllActiveClientUniStreams() throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);

    DefaultAttributeMap quicAttrs = new DefaultAttributeMap();
    DefaultAttributeMap connectAttrs = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> quicAttrs.attr(inv.getArgument(0)));
    when(mockConnectStream.attr(any())).thenAnswer(inv -> connectAttrs.attr(inv.getArgument(0)));

    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    quicAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);
    quicAttrs.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);

    mgr.register(mockConnectStream);

    // Register two uni streams
    QuicStreamChannel stream1 = mock(QuicStreamChannel.class);
    DefaultAttributeMap stream1Attrs = new DefaultAttributeMap();
    when(stream1.attr(any())).thenAnswer(inv -> stream1Attrs.attr(inv.getArgument(0)));
    when(stream1.parent()).thenReturn(mockQuic);
    when(stream1.streamId()).thenReturn(3L);
    when(stream1.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    when(stream1.closeFuture()).thenReturn(new DefaultChannelPromise(stream1));

    QuicStreamChannel stream2 = mock(QuicStreamChannel.class);
    DefaultAttributeMap stream2Attrs = new DefaultAttributeMap();
    when(stream2.attr(any())).thenAnswer(inv -> stream2Attrs.attr(inv.getArgument(0)));
    when(stream2.parent()).thenReturn(mockQuic);
    when(stream2.streamId()).thenReturn(7L);
    when(stream2.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    when(stream2.closeFuture()).thenReturn(new DefaultChannelPromise(stream2));

    ChannelHandlerContext ctx1 = mock(ChannelHandlerContext.class);
    when(ctx1.channel()).thenReturn(stream1);
    WebTransportUniStreamHeaderDecoder decoder1 =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);
    ByteBuf in1 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in1, 0L);
    decoder1.decode(ctx1, in1, new ArrayList<>());
    in1.release();

    ChannelHandlerContext ctx2 = mock(ChannelHandlerContext.class);
    when(ctx2.channel()).thenReturn(stream2);
    WebTransportUniStreamHeaderDecoder decoder2 =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);
    ByteBuf in2 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in2, 0L);
    decoder2.decode(ctx2, in2, new ArrayList<>());
    in2.release();

    NettyWebTransportSession session = mgr.get(0L);
    assertEquals(2, session.getActiveClientInitiatedUni().size());

    // Unregister session
    mgr.unregister(0L, mockQuic);

    // Verify both client uni streams were closed
    verify(stream1).close();
    verify(stream2).close();
  }

  @Test
  public void testUniStreamMissingParentRejected() throws Exception {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockStream.parent()).thenReturn(null);
    ChannelPromise newPromise = mock(ChannelPromise.class);
    when(mockStream.newPromise()).thenReturn(newPromise);

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockStream);
    when(ctx.newPromise()).thenReturn(newPromise);

    boolean admitted =
        WebTransportUtils.initializeClientStream(ctx, WebTransportUtils.UNI_STREAM_TYPE, 0L);
    assertFalse(admitted);
    verify(mockStream).shutdown(eq(WebTransportUtils.WT_BUFFERED_STREAM_REJECTED), eq(newPromise));
  }

  @Test
  public void testUniStreamNonQuicChannelRejected() throws Exception {
    io.netty.channel.Channel genericChannel = mock(io.netty.channel.Channel.class);
    when(genericChannel.parent()).thenReturn(mock(QuicChannel.class));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(genericChannel);

    boolean admitted =
        WebTransportUtils.initializeClientStream(ctx, WebTransportUtils.UNI_STREAM_TYPE, 0L);
    assertFalse(admitted);
    verify(ctx).close();
  }

  @Test
  public void testConcurrentSessionCloseDuringStreamRegistrationRejectedAndCleanedUp()
      throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicStreamChannel mockUniStream = mock(QuicStreamChannel.class);

    DefaultAttributeMap quicAttrs = new DefaultAttributeMap();
    DefaultAttributeMap connectAttrs = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();

    when(mockQuic.attr(any())).thenAnswer(inv -> quicAttrs.attr(inv.getArgument(0)));
    when(mockConnectStream.attr(any())).thenAnswer(inv -> connectAttrs.attr(inv.getArgument(0)));
    when(mockUniStream.attr(any())).thenAnswer(inv -> streamAttrs.attr(inv.getArgument(0)));

    when(mockUniStream.parent()).thenReturn(mockQuic);
    when(mockUniStream.streamId()).thenReturn(7L);
    when(mockUniStream.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    ChannelPromise streamPromise = new DefaultChannelPromise(mockUniStream);
    when(mockUniStream.newPromise()).thenReturn(streamPromise);
    when(mockUniStream.closeFuture()).thenReturn(new DefaultChannelPromise(mockUniStream));

    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    quicAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);
    quicAttrs.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);

    mgr.register(mockConnectStream);
    NettyWebTransportSession session = mgr.get(0L);

    // Verify registering directly on a closed session returns false and does not retain stream
    session.close();
    boolean directRegistered = session.registerActiveClientStream(mockUniStream, false);
    assertFalse(directRegistered);
    assertFalse(session.getActiveClientInitiatedUni().contains(mockUniStream));

    // Verify initializeClientStream shuts down with WT_SESSION_GONE if registerActiveClientStream returns false
    NettyWebTransportSession mockSession = mock(NettyWebTransportSession.class);
    when(mockSession.isOpen()).thenReturn(true);
    when(mockSession.incrementAndGetClientInitiatedStreamsUni()).thenReturn(1L);
    when(mockSession.getSettingsMaxStreamsUni()).thenReturn(100L);
    when(mockSession.registerActiveClientStream(any(), eq(false))).thenReturn(false);

    WebTransportSessionManager mockMgr = mock(WebTransportSessionManager.class);
    when(mockMgr.hasSession(0L)).thenReturn(true);
    when(mockMgr.get(0L)).thenReturn(mockSession);
    quicAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mockMgr);

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockUniStream);
    when(ctx.newPromise()).thenReturn(streamPromise);

    boolean admitted =
        WebTransportUtils.initializeClientStream(ctx, WebTransportUtils.UNI_STREAM_TYPE, 0L);
    assertFalse(admitted);
    verify(mockUniStream).shutdown(eq(WebTransportUtils.WT_SESSION_GONE), any());
  }
}
