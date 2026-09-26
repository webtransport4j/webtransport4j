package io.github.webtransport4j.protocol.compatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.Http3InboundControlStreamHandler;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportCapsule;
import io.github.webtransport4j.server.WebTransportCapsuleHandler;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import org.junit.Test;

/**
 * Protocol compatibility tests for draft-ietf-webtrans-http3-16 Section 5: Flow Control.
 * Directly exercises application components: {@link WebTransportSessionManager},
 * {@link Http3InboundControlStreamHandler}, and {@link WebTransportCapsuleHandler}.
 */
public class Draft16Section5FlowControlTest {

  /**
   * Section 5.1: Negotiating the Use of Flow Control.
   * "Flow control is enabled when both endpoints declare their intent to use flow control by taking
   * any of the following actions:
   * - Sending SETTINGS_WT_INITIAL_MAX_STREAMS_UNI with any value other than '0'.
   * - Sending SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI with any value other than '0'.
   * - Sending SETTINGS_WT_INITIAL_MAX_DATA with any value other than '0'."
   * MANDATORY: Test WebTransportSessionManager.isFlowControlNegotiated requires non-zero declaration.
   */
  @Test
  public void testSection5_1_FlowControlNegotiationCriteriaInApplication() {
    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap attrMap = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> attrMap.attr(inv.getArgument(0)));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
    assertFalse("Flow control must not be negotiated before peer settings arrive",
        WebTransportSessionManager.isFlowControlNegotiated(mockQuic));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    attrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(0L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(0L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(0L);
    assertFalse("Flow control must not be negotiated if peer declared only 0 limits",
        WebTransportSessionManager.isFlowControlNegotiated(mockQuic));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(50L);
    assertTrue("Flow control must be negotiated when both sides declare non-zero limits",
        WebTransportSessionManager.isFlowControlNegotiated(mockQuic));
  }

  /**
   * Section 5.1: Negotiating the Use of Flow Control.
   * "If flow control is disabled, an endpoint MUST NOT open more than one session on a given HTTP/3
   * connection."
   * MANDATORY: Test WebTransportSessionManager restricts sessions to 1 when flow control is disabled.
   */
  @Test
  public void testSection5_1_SingleSessionLimitWhenFlowControlDisabledInApplication() {
    final WebTransportSessionManager mgr = new WebTransportSessionManager();
    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap attrMap = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> attrMap.attr(inv.getArgument(0)));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    attrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(0L);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(0L);

    assertTrue("First session must be allowed", mgr.reserveSession(mockQuic, 10));
    assertFalse("Second session MUST be rejected when flow control is disabled",
        mgr.reserveSession(mockQuic, 10));
  }

  /**
   * Section 5.2: Relationship Between QUIC Flow Control and Session Flow Control.
   * "WebTransport endpoints SHOULD implement rate limiting..."
   * OPTIONAL: Rate limiting across connection.
   */
  @Test
  public void testSection5_2_RateLimitingOptional_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 5.3: Limiting the Number of Streams Within a Session.
   * "The stream limits described in this section do not apply to the CONNECT stream that established
   * the session."
   * MANDATORY: Test CONNECT stream is excluded from active stream counters.
   */
  @Test
  public void testSection5_3_ConnectStreamExcludedFromStreamCountInApplication() {
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(0L);

    WebTransportSession session = new WebTransportSession(
        0L, mockConnectStream, "/wt", 100L, 100L, 10000L, 100L, 100L, 10000L, true, true);

    assertEquals("Client-initiated Uni count must start at 0", 0L,
        session.getClientInitiatedStreamsUni());
    assertEquals("Client-initiated Bidi count must start at 0", 0L,
        session.getClientInitiatedStreamsBidi());
  }

  /**
   * Section 5.4: Data Limits.
   * "Stream-level flow control is handled directly by QUIC frames (MAX_STREAM_DATA).
   * Therefore, the capsules WT_MAX_STREAM_DATA and WT_STREAM_DATA_BLOCKED MUST NOT appear
   * on the CONNECT stream. If received, the endpoint MUST reset the session with
   * WT_FLOW_CONTROL_ERROR."
   * MANDATORY: Test WebTransportCapsuleHandler resets session with WT_FLOW_CONTROL_ERROR
   * upon receiving prohibited capsules 0x190b4d3e or 0x190b4d42.
   */
  @Test
  public void testSection5_4_ProhibitedCapsulesEnforcedInApplication() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockConnectStream);
    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);

    // Test prohibited WT_MAX_STREAM_DATA (0x190b4d3e)
    ByteBuf content = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content, 1000L);
    WebTransportCapsule prohibitedMaxStreamData =
        new WebTransportCapsule(0L, 0x190b4d3eL, content);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, prohibitedMaxStreamData);

    // Verify session closed with WT_FLOW_CONTROL_ERROR (0x045d4487)
    verify(mockConnectStream).shutdown(eq(0x045d4487), any());

    // Test prohibited WT_STREAM_DATA_BLOCKED (0x190b4d42)
    ByteBuf content2 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content2, 1000L);
    WebTransportCapsule prohibitedDataBlocked =
        new WebTransportCapsule(0L, 0x190b4d42L, content2);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, prohibitedDataBlocked);

    verify(mockConnectStream, org.mockito.Mockito.atLeastOnce()).shutdown(eq(0x045d4487), any());
  }

  /**
   * Section 5.5: Flow Control SETTINGS.
   * "SETTINGS_WT_INITIAL_MAX_STREAMS_UNI (0x2b64),
   *  SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (0x2b65),
   *  SETTINGS_WT_INITIAL_MAX_DATA (0x2b61)"
   * MANDATORY: Test Http3InboundControlStreamHandler parses and applies these settings to sessions.
   */
  @Test
  public void testSection5_5_FlowControlSettingsApplicationParsing() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);

    Http3Settings settings = new Http3Settings((id, value) -> true);
    settings.enableH3Datagram(true);
    settings.put(0x2b64L, 50L);
    settings.put(0x2b65L, 60L);
    settings.put(0x2b61L, 10000L);

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(settings));

    WebTransportSession session = mgr.get(0L);
    assertNotNull(session);
    assertEquals(50L, session.getPeerSettingsMaxStreamsUni());
    assertEquals(60L, session.getPeerSettingsMaxStreamsBidi());
    assertEquals(10000L, session.getPeerSettingsMaxData());
  }

  /**
   * Section 5.6: Flow Control Capsules.
   * "If an endpoint receives a WT_MAX_STREAMS capsule with a Maximum Streams value less than a
   * previously received value, it MUST close the WebTransport session by resetting the connect
   * stream with the WT_FLOW_CONTROL_ERROR error code."
   * MANDATORY: Test WebTransportCapsuleHandler decreases detection triggers WT_FLOW_CONTROL_ERROR.
   */
  @Test
  public void testSection5_6_MaxStreamsDecreasingValueEnforcementInApplication() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockConnectStream);
    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(0L);
    assertNotNull(session);
    session.setPeerSettingsMaxStreamsBidi(100L);

    // Send WT_MAX_STREAMS bidi with lower value (90 < 100)
    ByteBuf content = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content, 90L);
    WebTransportCapsule lowerMaxStreamsCapsule =
        new WebTransportCapsule(0L, 0x190b4d3fL, content);

    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, lowerMaxStreamsCapsule);

    // Connect stream must be shutdown with WT_FLOW_CONTROL_ERROR
    verify(mockConnectStream).shutdown(eq(0x045d4487), any());
  }

  /**
   * Section 5.6: Flow Control Capsules.
   * "If an endpoint receives a WT_MAX_DATA capsule with a Maximum Data value less than a
   * previously received value, it MUST close the WebTransport session by resetting the connect
   * stream with the WT_FLOW_CONTROL_ERROR error code."
   * MANDATORY: Test WebTransportCapsuleHandler enforces monotonicity of WT_MAX_DATA.
   */
  @Test
  public void testSection5_6_MaxDataDecreasingValueEnforcementInApplication() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockConnectStream);
    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(0L);
    assertNotNull(session);
    session.markPeerMaxDataCapsuleReceived();
    session.setPeerSettingsMaxData(5000L);

    // Send WT_MAX_DATA with lower value (4000 < 5000)
    ByteBuf content = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content, 4000L);
    WebTransportCapsule lowerMaxDataCapsule =
        new WebTransportCapsule(0L, 0x190b4d3dL, content);

    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, lowerMaxDataCapsule);

    verify(mockConnectStream).shutdown(eq(0x045d4487), any());
  }

  /**
   * Section 5.6: Flow Control Capsules.
   * "Maximum Streams: This value cannot exceed 2^60, as it is not possible to encode stream IDs
   * larger than 2^62-1."
   * MANDATORY: Test WebTransportCapsuleHandler closes session when WT_MAX_STREAMS > 2^60.
   */
  @Test
  public void testSection5_6_MaxStreamsLimitExceeding2Power60InApplication() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    ByteBuf content = Unpooled.buffer();
    WebTransportUtils.writeVarInt(content, (1L << 60) + 1L);
    WebTransportCapsule exceedingCapsule =
        new WebTransportCapsule(0L, 0x190b4d3fL, content);

    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, exceedingCapsule);
    verify(mockCtx).close();
  }

  /**
   * Section 5.6.1: Flow Control and Intermediaries.
   * "Intermediaries MUST consume WT_MAX_STREAMS capsules for flow control purposes..."
   * OPTIONAL: Intermediary proxy flow control behavior.
   */
  @Test
  public void testSection5_6_IntermediariesFlowControl_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }
}
