package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportConfig;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Mocked unit tests for {@code draft-ietf-webtrans-http3-17} Section 8: Security Considerations
 * (Resource Starvation, Shared Connection Deadlock Prevention, and Session Limits).
 */
public class Draft17Section8SecurityConsiderationsTest {

  @Test
  public void testSection8_DefaultAllowedSettingsIncludesDraft17AndFlowControlSettings() {
    String allowed =
        WebTransportConfig.get(
            "webtransport4j.http3.settings.nonstandardallowed",
            WebTransportProtocolSpec.DEFAULT_ALLOWED_SETTINGS);
    assertNotNull(allowed);
    assertTrue(
        allowed.contains("0x20b8933b"),
        "Default allowed non-standard settings MUST include Draft-17 SETTINGS_WT_ENABLED");
    assertFalse(
        allowed.contains("0x2c7cf000"),
        "Default allowed non-standard settings MUST NOT advertise Draft-16 0x2c7cf000");
    assertTrue(allowed.contains("0x2b61"));
    assertTrue(allowed.contains("0x2b64"));
    assertTrue(allowed.contains("0x2b65"));
  }

  @Test
  public void testSection8_RejectsConcurrentSessionWithoutSessionFlowControlToPreventStarvation()
      throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream2 = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);
    WebTransportServer mockServer = mock(WebTransportServer.class);

    DefaultAttributeMap parentAttrs = new DefaultAttributeMap();
    DefaultAttributeMap stream2Attrs = new DefaultAttributeMap();

    WebTransportSessionManager sessionManager = new WebTransportSessionManager();
    parentAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(sessionManager);
    parentAttrs.attr(WebTransportAttributeKeys.SERVER_KEY).set(mockServer);
    parentAttrs.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    parentAttrs.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(true);
    parentAttrs.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).set(Collections.emptyList());
    // Flow control NOT negotiated (0 / null initial settings): shared connection starvation risk.
    parentAttrs.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(0L);
    parentAttrs.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(0L);
    parentAttrs.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(0L);

    when(mockServer.isAcceptingSessions()).thenReturn(true);
    when(mockStream2.parent()).thenReturn(mockParent);
    when(mockStream2.streamId()).thenReturn(4L);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrs.attr(inv.getArgument(0)));
    when(mockStream2.attr(any())).thenAnswer(inv -> stream2Attrs.attr(inv.getArgument(0)));

    ChannelFuture mockWriteFuture = mock(ChannelFuture.class);
    when(mockCtx.writeAndFlush(any())).thenReturn(mockWriteFuture);
    when(mockStream2.closeFuture()).thenReturn(mock(ChannelFuture.class));
    when(mockStream2.newPromise()).thenReturn(mock(ChannelPromise.class));

    // Even when configuredLimit = 10, without flow control at most 1 session is permitted (§8)
    assertTrue(sessionManager.reserveSession(mockParent, 10));
    assertFalse(
        sessionManager.reserveSession(mockParent, 10),
        "Session manager MUST cap concurrent sessions to 1 when flow control is not negotiated");

    // Attempt a second concurrent session on stream 4 via WebTransportHeadersHandler
    when(mockCtx.channel()).thenReturn(mockStream2);
    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.set(":protocol", WebTransportHeadersHandler.UPGRADE_TOKEN_H3);
    headers.scheme("https");
    headers.authority("example.com");
    headers.path("/wt");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    ArgumentCaptor<Http3HeadersFrame> responseCaptor =
        ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(responseCaptor.capture());
    assertEquals(
        HttpResponseStatus.TOO_MANY_REQUESTS.codeAsText(),
        responseCaptor.getValue().headers().status(),
        "Concurrent sessions without session flow control MUST be rejected to prevent starvation");
  }
}
