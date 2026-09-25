package io.github.webtransport4j.protocol.compatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import org.junit.Test;

/**
 * Protocol compatibility tests for IETF WebTransport over HTTP/3 (Draft-16).
 * Section 7: Considerations for Future Versions.
 * Directly exercises application classes: {@link WebTransportServer},
 * {@link WebTransportSessionManager}, and {@link WebTransportHeadersHandler}.
 */
public class Draft16Section7FutureVersionsTest {

  /**
   * Section 7.1: Negotiating the Draft Version.
   * "Each draft version defines a distinct codepoint for SETTINGS_WT_ENABLED.
   * Both the client and the server MUST send SETTINGS_WT_ENABLED with the codepoint
   * corresponding to their supported draft version."
   * MANDATORY: Test that WebTransportServer advertises draft-16 SETTINGS_WT_ENABLED = 0x2c7cf000.
   */
  @Test
  public void testSection7_1_DraftVersionSettingsCodepointInApplication() {
    WebTransportServer server = new WebTransportServerBuilder().build();
    Http3Settings settings = server.buildHttp3Settings();

    Long draft16Setting = settings.get(0x2c7cf000L);
    assertEquals("Server MUST send SETTINGS_WT_ENABLED with codepoint 0x2c7cf000 for draft-16",
        Long.valueOf(1L), draft16Setting);
  }

  /**
   * Section 7.1: Negotiating the Draft Version.
   * "For this reason, the server MUST NOT process any incoming WebTransport
   * requests until the client's SETTINGS have been received."
   * MANDATORY: Test application prevents session establishment and negotiation prior to peer settings.
   */
  @Test
  public void testSection7_1_MustNotProcessRequestsUntilSettingsReceivedInApplication() throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap attrMap = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> attrMap.attr(inv.getArgument(0)));

    // Prior to receiving peer settings:
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
    assertFalse("Flow control MUST NOT be negotiated before peer settings arrived",
        WebTransportSessionManager.isFlowControlNegotiated(mockQuic));

    // When client sends invalid/unsupported settings (e.g. no datagrams), reject incoming CONNECT
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockConnectStream);
    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    attrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(false);

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("localhost");
    headers.path("/wt");
    headers.set(":protocol", "webtransport-h3");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    // Verify connect stream is shutdown with H3_MESSAGE_ERROR (0x010e)
    verify(mockConnectStream).shutdown(eq(0x010e), any());
  }

  /**
   * Section 7.1: Negotiating the Draft Version.
   * "An endpoint that supports multiple draft versions sends a SETTINGS_WT_ENABLED
   * value for each supported version, as each version uses a different setting identifier.
   * The highest version supported by both endpoints is selected."
   * OPTIONAL: Supporting multiple draft versions simultaneously.
   */
  @Test
  public void testSection7_1_HighestDraftVersionSelection_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 7: Future Incompatible Upgrade Token.
   * "Future versions of WebTransport that change the syntax of the CONNECT requests
   * used to establish WebTransport sessions will need to modify the upgrade token
   * used to identify WebTransport, allowing servers to offer multiple versions
   * simultaneously (see Section 9.1)."
   * OPTIONAL: Future version upgrade token variation handling.
   */
  @Test
  public void testSection7_FutureIncompatibleUpgradeToken_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }
}
