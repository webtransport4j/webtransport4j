package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.client.Http3ClientControlStreamHandler;
import io.github.webtransport4j.server.Http3InboundControlStreamHandler;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Mocked unit tests for {@code draft-ietf-webtrans-http3-17} Sections 2.2 and 3: Session
 * Establishment.
 */
public class Draft17Section3SessionEstablishmentTest {

  @Test
  public void testSection3_1_ServerAdvertisesStrictDraft17SettingsWtEnabledOnly() {
    WebTransportServer server = new WebTransportServerBuilder().build();
    Http3Settings serverSettings = server.buildHttp3Settings();

    assertEquals(
        Long.valueOf(1L),
        serverSettings.get(WebTransportProtocolSpec.SETTING_WT_ENABLED),
        "Server MUST advertise Draft-17 SETTINGS_WT_ENABLED (0x20b8933b) = 1");
    assertNull(
        serverSettings.get(0x2c7cf000L),
        "Strict Draft-17 server MUST NOT advertise legacy Draft-16 codepoint 0x2c7cf000");
    assertTrue(
        Boolean.TRUE.equals(serverSettings.h3DatagramEnabled()),
        "Server MUST enable HTTP/3 Datagrams for WebTransport");
    assertTrue(
        Boolean.TRUE.equals(serverSettings.connectProtocolEnabled()),
        "Server MUST enable HTTP/3 Extended CONNECT for WebTransport");
  }

  @Test
  public void testSection3_1_ServerMustIgnoreClientSettingsWtEnabledIncludingGreaterThanOne()
      throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    Http3Settings peerSettings = new Http3Settings((id, value) -> true);
    peerSettings.enableH3Datagram(true);
    peerSettings.put(WebTransportProtocolSpec.SETTING_WT_ENABLED, 42L);
    peerSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI, 16L);

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(peerSettings));

    verify(mockParent, never()).close(anyBoolean(), anyInt(), any());
    verify(mockCtx, never()).close();
    assertTrue(
        Boolean.TRUE.equals(
            parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).get()));
    assertTrue(
        Boolean.TRUE.equals(
            parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).get()));
    assertEquals(
        Long.valueOf(16L),
        parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get());
  }

  @Test
  public void testSection3_1_ServerAcceptsClientOmittingSettingsWtEnabled() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    Http3Settings peerSettings = new Http3Settings((id, value) -> true);
    peerSettings.enableH3Datagram(true);

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(peerSettings));

    verify(mockParent, never()).close(anyBoolean(), anyInt(), any());
    assertTrue(
        Boolean.TRUE.equals(
            parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).get()));
  }

  @Test
  public void testSection3_1_ClientTreatsServerSettingsWtEnabledGreaterThanOneAsSettingsError()
      throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    Http3Settings serverSettings = new Http3Settings((id, value) -> true);
    serverSettings.enableH3Datagram(true);
    serverSettings.enableConnectProtocol(true);
    serverSettings.put(WebTransportProtocolSpec.SETTING_WT_ENABLED, 2L);

    Http3ClientControlStreamHandler clientHandler = new Http3ClientControlStreamHandler();
    clientHandler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(serverSettings));

    verify(mockParent)
        .close(eq(true), eq(WebTransportProtocolSpec.H3_SETTINGS_ERROR), any());
  }

  @Test
  public void testSection3_2_ClientDiscardsRetainedZeroRttSettingsAndUsesCurrentConnectionValues()
      throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockControlStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockControlStream);
    when(mockControlStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    // Simulate retained settings from a prior connection before resumption
    parentAttrMap.attr(WebTransportAttributeKeys.WT_ENABLED).set(true);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(1000L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(500L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(1_000_000L);

    Http3ClientControlStreamHandler clientHandler = new Http3ClientControlStreamHandler();
    clientHandler.handlerAdded(mockCtx);

    assertFalse(parentAttrMap.attr(WebTransportAttributeKeys.WT_ENABLED).get());
    assertFalse(parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).get());
    assertNull(parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).get());
    assertNull(parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get());
    assertNull(parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).get());

    // Resumed connection advertises reduced WebTransport limits and omits MAX_STREAMS_UNI
    Http3Settings reducedServerSettings = new Http3Settings((id, value) -> true);
    reducedServerSettings.enableH3Datagram(true);
    reducedServerSettings.enableConnectProtocol(true);
    reducedServerSettings.put(WebTransportProtocolSpec.SETTING_WT_ENABLED, 1L);
    reducedServerSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI, 10L);
    reducedServerSettings.put(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_DATA, 4096L);

    clientHandler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(reducedServerSettings));

    verify(mockParent, never()).close(anyBoolean(), anyInt(), any());
    assertTrue(parentAttrMap.attr(WebTransportAttributeKeys.WT_ENABLED).get());
    assertNull(parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).get());
    assertEquals(
        Long.valueOf(10L),
        parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get());
    assertEquals(
        Long.valueOf(4096L),
        parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).get());
  }

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
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(mockConnectStream.isOpen()).thenReturn(true);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any()))
        .thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);

    Http3Settings clientSettingsMissingDatagram = new Http3Settings((id, value) -> true);
    clientSettingsMissingDatagram.enableH3Datagram(false);

    Http3InboundControlStreamHandler handler = new Http3InboundControlStreamHandler();
    handler.channelRead(mockCtx, new DefaultHttp3SettingsFrame(clientSettingsMissingDatagram));

    assertFalse(
        parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).get(),
        "Peer settings must be marked invalid when datagrams are disabled");
    verify(mockConnectStream).shutdown(eq(0x010e), any());
  }

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

    WebTransportHeadersHandler.INSTANCE.channelRead(
        mockCtx, new DefaultHttp3HeadersFrame(headersWithWrongProtocol));

    ArgumentCaptor<Http3HeadersFrame> captor = ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    assertEquals(HttpResponseStatus.NOT_FOUND.codeAsText(), captor.getValue().headers().status());
  }

  @Test
  public void testSection2_2_And_3_2_HttpsUriSchemeAuthorityAndPathValidation() throws Exception {
    assertTrue(WebTransportProtocolSpec.isValidHttpsRequestTarget("https", "example.com", "/wt"));
    assertFalse(WebTransportProtocolSpec.isValidHttpsRequestTarget("http", "example.com", "/wt"));
    assertFalse(WebTransportProtocolSpec.isValidHttpsRequestTarget("https", "", "/wt"));
    assertFalse(WebTransportProtocolSpec.isValidHttpsRequestTarget("https", "example.com", ""));
    assertFalse(
        WebTransportProtocolSpec.isValidHttpsRequestTarget("https", "example.com", "no-slash"));

    // Invalid scheme -> 400
    assertConnectRejectedWithBadRequest("http", "localhost", "/wt");
    // Empty authority -> 400
    assertConnectRejectedWithBadRequest("https", "", "/wt");
    // Empty path -> 400 per RFC 9110 Section 4.2.2 HTTPS URI
    assertConnectRejectedWithBadRequest("https", "localhost", "");
    // Non-origin-form path -> 400
    assertConnectRejectedWithBadRequest("https", "localhost", "relative/path");
  }

  private static void assertConnectRejectedWithBadRequest(
      String scheme, String authority, String path) throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme(scheme);
    headers.authority(authority);
    headers.path(path);
    headers.set(":protocol", "webtransport-h3");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    ArgumentCaptor<Http3HeadersFrame> captor = ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    assertEquals(
        HttpResponseStatus.BAD_REQUEST.codeAsText(), captor.getValue().headers().status());
  }

  @Test
  public void testSection3_2_ClientInitiatedBidiSessionStreamIdEnforcement() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(1L);

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

    parentAttrMap
        .attr(WebTransportAttributeKeys.ALLOWED_ORIGINS)
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
    assertEquals(HttpResponseStatus.FORBIDDEN.codeAsText(), captor.getValue().headers().status());
  }
}

