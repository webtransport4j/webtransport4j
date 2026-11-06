package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import org.junit.jupiter.api.Test;

/**
 * Mocked unit tests for {@code draft-ietf-webtrans-http3-17} Section 7: Negotiating Draft Versions
 * and Future Versions.
 */
public class Draft17Section7FutureVersionsTest {

  @Test
  public void testSection7_ServerAdvertisesDraft17CodepointAndDropsDraft16Codepoint() {
    WebTransportServer server = new WebTransportServerBuilder().build();
    Http3Settings settings = server.buildHttp3Settings();

    assertEquals(
        Long.valueOf(1L),
        settings.get(WebTransportProtocolSpec.SETTING_WT_ENABLED),
        "Server MUST advertise Draft-17 SETTINGS_WT_ENABLED (0x20b8933b) = 1");
    assertNull(
        settings.get(0x2c7cf000L),
        "Strict Draft-17 server MUST NOT advertise Draft-16 SETTINGS_WT_ENABLED (0x2c7cf000)");
  }

  @Test
  public void testSection7_ServerRejectsConnectWhenPeerSettingsInvalid() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrs = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();
    parentAttrs.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    parentAttrs.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(false);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrs.attr(inv.getArgument(0)));
    when(mockStream.attr(any())).thenAnswer(inv -> streamAttrs.attr(inv.getArgument(0)));
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.set(":protocol", WebTransportHeadersHandler.UPGRADE_TOKEN_H3);
    headers.scheme("https");
    headers.authority("example.com");
    headers.path("/wt");

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    verify(mockStream).shutdown(eq((int) Http3ErrorCode.H3_MESSAGE_ERROR.code()), any());
  }
}

