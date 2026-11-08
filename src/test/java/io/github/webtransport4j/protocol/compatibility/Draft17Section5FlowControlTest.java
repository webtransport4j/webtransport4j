package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.DefaultWebTransportSession;
import io.github.webtransport4j.server.Http3InboundControlStreamHandler;
import io.github.webtransport4j.server.NettyWebTransportSession;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportCapsule;
import io.github.webtransport4j.server.WebTransportCapsuleHandler;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import org.junit.jupiter.api.Test;

/**
 * Mocked unit tests for {@code draft-ietf-webtrans-http3-17} Section 5: Flow Control.
 */
public class Draft17Section5FlowControlTest {

  @Test
  public void testSection5_1_FlowControlNegotiationCriteriaInApplication() {
    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap attrMap = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> attrMap.attr(inv.getArgument(0)));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
    assertFalse(WebTransportSessionManager.isFlowControlNegotiated(mockQuic));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    attrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(0L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(0L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(0L);
    assertFalse(WebTransportSessionManager.isFlowControlNegotiated(mockQuic));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(50L);
    assertTrue(WebTransportSessionManager.isFlowControlNegotiated(mockQuic));
  }

  @Test
  public void testSection5_1_SingleSessionLimitWhenFlowControlDisabledInApplication() {
    final WebTransportSessionManager mgr = new WebTransportSessionManager();
    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap attrMap = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> attrMap.attr(inv.getArgument(0)));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    attrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(0L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(0L);

    assertTrue(mgr.reserveSession(mockQuic, 10));
    assertFalse(mgr.reserveSession(mockQuic, 10));
  }

  @Test
  public void testSection5_3_ConnectStreamExcludedFromStreamCountInApplication() {
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.isOpen()).thenReturn(true);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockConnectStream, "/wt", 100L, 100L, 10000L, 100L, 100L, 10000L, true, true);

    assertEquals(0L, session.getClientInitiatedStreamsUni());
    assertEquals(0L, session.getClientInitiatedStreamsBidi());
  }

  @Test
  public void testSection5_4_ProhibitedCapsulesEnforcedInApplication() throws Exception {
    Fixture fx = newFixture(null, null, null);

    ByteBuf content = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content, 1000L);
    WebTransportCapsuleHandler.INSTANCE.channelRead(
        fx.ctx, new WebTransportCapsule(0L, 0x190b4d3eL, content));
    verify(fx.connectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());

    ByteBuf content2 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content2, 1000L);
    WebTransportCapsuleHandler.INSTANCE.channelRead(
        fx.ctx, new WebTransportCapsule(0L, 0x190b4d42L, content2));
    verify(fx.connectStream, atLeastOnce())
        .shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
  }

  @Test
  public void testSection5_5_FlowControlSettingsApplicationParsing() throws Exception {
    final Fixture fx = newFixture(null, null, null);

    Http3Settings settings = new Http3Settings((id, value) -> true);
    settings.enableH3Datagram(true);
    settings.put(0x2b64L, 50L);
    settings.put(0x2b65L, 60L);
    settings.put(0x2b61L, 10000L);

    ChannelHandlerContext controlCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel controlStream = mock(QuicStreamChannel.class);
    when(controlCtx.channel()).thenReturn(controlStream);
    when(controlStream.parent()).thenReturn(fx.quic);

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead(controlCtx, new DefaultHttp3SettingsFrame(settings));

    assertEquals(50L, fx.session.getPeerSettingsMaxStreamsUni());
    assertEquals(60L, fx.session.getPeerSettingsMaxStreamsBidi());
    assertEquals(10000L, fx.session.getPeerSettingsMaxData());
  }

  @Test
  public void testSection5_6_2_MaxStreamsMustStrictlyIncreaseOverInitialSettingAndPriorCapsule()
      throws Exception {
    // 1. First WT_MAX_STREAMS bidi equal to SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (100 == 100) -> err
    Fixture eqInitialBidi = newFixture(50L, 100L, 5000L);
    sendVarIntCapsule(eqInitialBidi.ctx, 0x190b4d3fL, 100L);
    verify(eqInitialBidi.connectStream)
        .shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());

    // 2. First WT_MAX_STREAMS uni equal to omitted default 0 (0 == 0) -> err
    Fixture eqDefaultZeroUni = newFixture(null, 100L, 5000L);
    sendVarIntCapsule(eqDefaultZeroUni.ctx, 0x190b4d40L, 0L);
    verify(eqDefaultZeroUni.connectStream)
        .shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());

    // 3. Strictly increasing (100 -> 150) succeeds, then duplicate equal (150 == 150) -> err
    Fixture progressive = newFixture(50L, 100L, 5000L);
    sendVarIntCapsule(progressive.ctx, 0x190b4d3fL, 150L);
    verify(progressive.connectStream, never()).shutdown(anyInt(), any());
    assertEquals(150L, progressive.session.getPeerSettingsMaxStreamsBidi());

    sendVarIntCapsule(progressive.ctx, 0x190b4d3fL, 150L);
    verify(progressive.connectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());

    // 4. Decreasing value (50 -> 40) on uni -> err
    Fixture decreasingUni = newFixture(50L, 100L, 5000L);
    sendVarIntCapsule(decreasingUni.ctx, 0x190b4d40L, 40L);
    verify(decreasingUni.connectStream)
        .shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
  }

  @Test
  public void testSection5_6_4_MaxDataMustStrictlyIncreaseOverInitialSettingAndPriorCapsule()
      throws Exception {
    // 1. First WT_MAX_DATA equal to SETTINGS_WT_INITIAL_MAX_DATA (5000 == 5000) -> err
    Fixture eqInitial = newFixture(50L, 50L, 5000L);
    sendVarIntCapsule(eqInitial.ctx, 0x190b4d3dL, 5000L);
    verify(eqInitial.connectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());

    // 2. First WT_MAX_DATA less than late-arriving SETTINGS_WT_INITIAL_MAX_DATA (4000 < 5000) -> err
    Fixture lateSettings = newFixture(null, null, null);
    lateSettings.session.setPeerSettingsMaxData(5000L);
    sendVarIntCapsule(lateSettings.ctx, 0x190b4d3dL, 4000L);
    verify(lateSettings.connectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());

    // 3. First WT_MAX_DATA equal to omitted default 0 (0 == 0) -> err
    Fixture eqDefaultZero = newFixture(50L, 50L, null);
    sendVarIntCapsule(eqDefaultZero.ctx, 0x190b4d3dL, 0L);
    verify(eqDefaultZero.connectStream)
        .shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());

    // 4. Strictly increasing (5000 -> 8000) succeeds, then duplicate (8000 == 8000) -> err
    Fixture progressive = newFixture(50L, 50L, 5000L);
    sendVarIntCapsule(progressive.ctx, 0x190b4d3dL, 8000L);
    verify(progressive.connectStream, never()).shutdown(anyInt(), any());
    assertEquals(8000L, progressive.session.getPeerSettingsMaxData());

    sendVarIntCapsule(progressive.ctx, 0x190b4d3dL, 8000L);
    verify(progressive.connectStream).shutdown(eq(WebTransportUtils.WT_FLOW_CONTROL_ERROR), any());
  }

  private static void sendVarIntCapsule(ChannelHandlerContext ctx, long capsuleType, long value)
      throws Exception {
    ByteBuf content = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content, value);
    WebTransportCapsuleHandler.INSTANCE.channelRead(
        ctx, new WebTransportCapsule(0L, capsuleType, content));
  }

  private static final class Fixture {
    final ChannelHandlerContext ctx;
    final QuicStreamChannel connectStream;
    final QuicChannel quic;
    final NettyWebTransportSession session;

    Fixture(
        ChannelHandlerContext ctx,
        QuicStreamChannel connectStream,
        QuicChannel quic,
        NettyWebTransportSession session) {
      this.ctx = ctx;
      this.connectStream = connectStream;
      this.quic = quic;
      this.session = session;
    }
  }

  private static Fixture newFixture(Long peerUni, Long peerBidi, Long peerData) {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockConnectStream);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(100L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(10000L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(peerUni);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(peerBidi);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(peerData);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any()))
        .thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);
    NettyWebTransportSession session = mgr.get(0L);
    assertNotNull(session);
    return new Fixture(mockCtx, mockConnectStream, mockQuic, session);
  }
}
