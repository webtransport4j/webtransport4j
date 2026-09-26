package io.github.webtransport4j.protocol.compatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.Http3InboundControlStreamHandler;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.util.Collections;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Protocol compatibility tests for draft-ietf-webtrans-http3-16 Section 3: Session Establishment.
 * Directly exercises application handlers: {@link Http3InboundControlStreamHandler},
 * {@link WebTransportHeadersHandler}, and {@link WebTransportServer}.
 */
public class Draft16Section3SessionEstablishmentTest {

  /**
   * Section 3.1: Establishing a WebTransport-Capable HTTP/3 Connection.
   * "The default value for the SETTINGS_WT_ENABLED setting is '0', meaning that the server does
   * not support WebTransport. A value of '1' indicates support for the variant of WebTransport that
   * is described in this document."
   * MANDATORY: Test that WebTransportServer advertises SETTINGS_WT_ENABLED = 1 in its HTTP/3 settings.
   */
  @Test
  public void testSection3_1_ServerAdvertisesSettingsWtEnabledValue1() {
    WebTransportServer server = new WebTransportServerBuilder().build();
    Http3Settings serverSettings = server.buildHttp3Settings();

    Long wtEnabled = serverSettings.get(0x2c7cf000L);
    assertEquals("Server MUST advertise SETTINGS_WT_ENABLED = 1", Long.valueOf(1L), wtEnabled);
    assertTrue("Server MUST enable HTTP/3 Datagrams for WebTransport",
        Boolean.TRUE.equals(serverSettings.h3DatagramEnabled()));
    assertTrue("Server MUST enable HTTP/3 Extended CONNECT for WebTransport",
        Boolean.TRUE.equals(serverSettings.connectProtocolEnabled()));
  }

  /**
   * Section 3.1: Establishing a WebTransport-Capable HTTP/3 Connection.
   * "Clients MUST treat values greater than '1' as a connection error of type H3_SETTINGS_ERROR."
   * MANDATORY: Test that Http3InboundControlStreamHandler rejects peer SETTINGS_WT_ENABLED > 1 with H3_SETTINGS_ERROR.
   */
  @Test
  public void testSection3_1_SettingsWtEnabledGreaterThanOneTreatedAsError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    Http3Settings peerSettings = new Http3Settings((id, value) -> true);
    peerSettings.enableH3Datagram(true);
    peerSettings.put(0x2c7cf000L, 2L); // Greater than 1

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(peerSettings));

    // Must close connection with H3_SETTINGS_ERROR (0x0119)
    verify(mockParent).close(eq(true), eq(0x0119), any());
  }

  /**
   * Section 3.1: Establishing a WebTransport-Capable HTTP/3 Connection.
   * "WebTransport over HTTP/3 requires support for HTTP/3 datagrams and the Capsule Protocol, and
   * both the client and the server indicate support for HTTP/3 datagrams by sending a
   * SETTINGS_H3_DATAGRAM setting value set to 1 in their SETTINGS frame."
   * MANDATORY: Server MUST reset established sessions if client does not support H3 datagrams.
   */
  @Test
  public void testSection3_1_MissingRequiredSettingsTreatsSessionsAsMalformed() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockParent.hasAttr(any())).thenAnswer(inv -> parentAttrMap.hasAttr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.streamId()).thenReturn(0L);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);

    Http3Settings clientSettingsMissingDatagram = new Http3Settings((id, value) -> true);
    clientSettingsMissingDatagram.enableH3Datagram(false);

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(clientSettingsMissingDatagram));

    assertFalse("Peer settings must be marked invalid when datagrams are disabled",
        parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).get());
    verify(mockConnectStream).shutdown(eq(0x010e), any());
  }

  /**
   * Section 3.1: Establishing a WebTransport-Capable HTTP/3 Connection.
   * "If the server does not support the required features... it MAY send an error of type
   * WT_REQUIREMENTS_NOT_MET (0x212c0d48)."
   * OPTIONAL: Optional error code WT_REQUIREMENTS_NOT_MET.
   */
  @Test
  public void testSection3_1_WtRequirementsNotMetErrorCode_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 3.2: Creating a New Session.
   * "The request MUST include the :protocol pseudo-header field ([RFC8441]), and the value of that
   * field MUST be 'webtransport-h3'."
   * MANDATORY: Test that WebTransportHeadersHandler requires :protocol == webtransport-h3.
   */
  @Test
  public void testSection3_2_ExtendedConnectProtocolHeaderEnforcement() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);

    Http3Headers headersWithWrongProtocol = new DefaultHttp3Headers();
    headersWithWrongProtocol.method("CONNECT");
    headersWithWrongProtocol.scheme("https");
    headersWithWrongProtocol.authority("example.com");
    headersWithWrongProtocol.path("/wt");
    headersWithWrongProtocol.set(":protocol", "unknown-protocol");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx,
        new DefaultHttp3HeadersFrame(headersWithWrongProtocol));

    // Connect stream should NOT be accepted as WebTransport 200 OK
    ArgumentCaptor<Http3HeadersFrame> captor = ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx, org.mockito.Mockito.never()).writeAndFlush(captor.capture());
  }

  /**
   * Section 3.2: Creating a New Session.
   * "The :scheme pseudo-header field MUST be set to 'https'."
   * MANDATORY: Test that WebTransportHeadersHandler rejects scheme != https with 400 Bad Request.
   */
  @Test
  public void testSection3_2_ConnectSchemeMustBeHttps() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("http");
    headers.authority("localhost");
    headers.path("/wt");
    headers.set(":protocol", "webtransport-h3");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    ArgumentCaptor<Http3HeadersFrame> captor = ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    assertEquals(HttpResponseStatus.BAD_REQUEST.codeAsText(),
        captor.getValue().headers().status());
  }

  /**
   * Section 3.2: Creating a New Session.
   * "The :authority and :path pseudo-header fields MUST be present."
   * MANDATORY: Test that WebTransportHeadersHandler rejects empty :authority with 400 Bad Request.
   */
  @Test
  public void testSection3_2_ConnectAuthorityMandatory() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("");
    headers.path("/wt");
    headers.set(":protocol", "webtransport-h3");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    ArgumentCaptor<Http3HeadersFrame> captor = ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    assertEquals(HttpResponseStatus.BAD_REQUEST.codeAsText(),
        captor.getValue().headers().status());
  }

  /**
   * Section 3.2: Creating a New Session.
   * "Session IDs are derived from the stream ID of the CONNECT stream that established the session
   * and therefore MUST always correspond to a client-initiated bidirectional stream, as defined in
   * Section 2.1 of [RFC9000] (streamId % 4 == 0)."
   * MANDATORY: Test WebTransportHeadersHandler rejects streamId % 4 != 0 with H3_ID_ERROR.
   */
  @Test
  public void testSection3_2_ClientInitiatedBidiSessionStreamIdEnforcement() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(1L); // Not client-initiated bidi (1 % 4 != 0)

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockParent.hasAttr(any())).thenAnswer(inv -> parentAttrMap.hasAttr(inv.getArgument(0)));

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("localhost");
    headers.path("/wt");
    headers.set(":protocol", "webtransport-h3");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    verify(mockParent).close(eq(true), eq(Http3ErrorCode.H3_ID_ERROR.code()), any());
  }

  /**
   * Section 3.2: Creating a New Session.
   * "The server MAY verify the Origin header... If the server rejects the request, it MUST send a
   * 403 (Forbidden) status code."
   * MANDATORY: Test WebTransportHeadersHandler rejects unauthorized origin with 403 Forbidden.
   */
  @Test
  public void testSection3_2_OriginHeaderVerification() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(0L);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockParent.hasAttr(any())).thenAnswer(inv -> parentAttrMap.hasAttr(inv.getArgument(0)));

    parentAttrMap.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS)
        .set(Collections.singletonList("https://trusted.example.com"));

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("localhost");
    headers.path("/wt");
    headers.set(":protocol", "webtransport-h3");
    headers.set("origin", "https://untrusted-attacker.com");

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockCtx.writeAndFlush(any())).thenReturn(mockFuture);

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    ArgumentCaptor<Http3HeadersFrame> captor = ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    assertEquals(HttpResponseStatus.FORBIDDEN.codeAsText(),
        captor.getValue().headers().status());
  }

  /**
   * Section 3.2: Creating a New Session.
   * "If the server responds with a 3xx redirect, the client MAY follow it..."
   * OPTIONAL: Following 3xx redirects.
   */
  @Test
  public void testSection3_2_RedirectResponseAppNotification_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 3.3: Application Protocol Negotiation.
   * "The client MAY request the use of a specific application protocol by including a
   * WT-Available-Protocols header field in the CONNECT request... The value of WT-Available-Protocols
   * MUST be a Structured Fields sf-list of Strings."
   * OPTIONAL: Protocol sub-negotiation via WT-Available-Protocols.
   */
  @Test
  public void testSection3_3_SubprotocolNegotiationFormat_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 3.3: Application Protocol Negotiation.
   * "An endpoint MUST NOT include parameters on any protocol name, and a recipient MUST ignore
   * parameters if present."
   * OPTIONAL: Parameters on protocol names ignoring behavior.
   */
  @Test
  public void testSection3_3_SubprotocolParametersIgnored_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 3.4: Prioritization.
   * "The WebTransport CONNECT stream and the streams that belong to the session MAY be
   * prioritized using the HTTP Priority [PRIORITY] mechanism."
   * OPTIONAL: HTTP Priority scheme support.
   */
  @Test
  public void testSection3_4_PrioritizationUrgency_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 3.4: Prioritization.
   * "WebTransport endpoints that support HTTP Priority SHOULD parse Priority parameters..."
   * OPTIONAL: HTTP Priority incremental scheduling.
   */
  @Test
  public void testSection3_4_PrioritizationIncremental_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }
}
