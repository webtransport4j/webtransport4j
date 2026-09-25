package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamChannelConfig;
import io.netty.util.Attribute;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for Application-Layer Protocol Negotiation (WT-Available-Protocols / WT-Protocol)
 * per draft-16 § 3.3.
 */
public class SubprotocolNegotiationTest {

  @Test
  public void testParseAvailableProtocols() {
    assertTrue(WebTransportUtils.parseAvailableProtocols(null).isEmpty());
    assertTrue(WebTransportUtils.parseAvailableProtocols("").isEmpty());
    assertTrue(WebTransportUtils.parseAvailableProtocols("   ").isEmpty());

    List<String> protos = WebTransportUtils.parseAvailableProtocols("\"my-protocol\", \"chat-v1\"");
    assertEquals(Arrays.asList("my-protocol", "chat-v1"), protos);

    List<String> unquoted = WebTransportUtils.parseAvailableProtocols("proto-a, proto-b");
    assertEquals(Arrays.asList("proto-a", "proto-b"), unquoted);
    assertEquals(2, unquoted.size());
    assertEquals("proto-a", unquoted.get(0));
    assertEquals("proto-b", unquoted.get(1));

    List<String> escaped = WebTransportUtils.parseAvailableProtocols("\"chat\\\"v2\"");
    assertEquals(Collections.singletonList("chat\"v2"), escaped);
  }

  @Test
  public void testFormatProtocolHeader() {
    assertEquals("\"chat-v1\"", WebTransportUtils.formatProtocolHeader("chat-v1"));
    assertEquals("\"chat\\\"v2\"", WebTransportUtils.formatProtocolHeader("chat\"v2"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testHeadersHandlerSelectsSubprotocolAndEmitsHeader() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(0L); // client-initiated bidi stream
    QuicStreamChannelConfig mockConfig = mock(QuicStreamChannelConfig.class);
    when(mockConfig.isAutoRead()).thenReturn(true);
    when(mockStream.config()).thenReturn(mockConfig);

    DefaultChannelPromise closeFuture = new DefaultChannelPromise(mockStream, ImmediateEventExecutor.INSTANCE);
    when(mockStream.closeFuture()).thenReturn(closeFuture);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockParent.hasAttr(any())).thenAnswer(inv -> parentAttrMap.hasAttr(inv.getArgument(0)));

    when(mockParent.remoteSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));

    // Settings attributes
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(true);

    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(10L);

    // Allowed origins: empty = allow all
    parentAttrMap.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).set(Collections.emptyList());

    // Session manager setup
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    // Global session slots
    parentAttrMap.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS).set(new AtomicInteger(0));

    // WebTransportServer setup with custom handler selecting a protocol
    WebTransportServer mockServer = mock(WebTransportServer.class);
    WebTransportHandler customHandler = new WebTransportHandler() {
      @Override
      public @Nullable String selectSubprotocol(@NonNull List<String> availableProtocols) {
        if (availableProtocols.contains("chat-v2")) {
          return "chat-v2";
        }
        return null;
      }
    };
    when(mockServer.getHandler("/chat")).thenReturn(customHandler);
    parentAttrMap.attr(WebTransportAttributeKeys.SERVER_KEY).set(mockServer);

    // Write and flush capture
    ChannelFuture successFuture = new DefaultChannelPromise(mockStream, ImmediateEventExecutor.INSTANCE);
    ((DefaultChannelPromise) successFuture).setSuccess();
    when(mockCtx.writeAndFlush(any())).thenReturn(successFuture);

    // Incoming Extended CONNECT headers
    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("example.com");
    headers.path("/chat");
    headers.set(":protocol", "webtransport-h3");
    headers.set("wt-available-protocols", "\"chat-v1\", \"chat-v2\"");

    WebTransportHeadersHandler headersHandler = new WebTransportHeadersHandler();
    headersHandler.channelRead(mockCtx, new DefaultHttp3HeadersFrame(headers));

    // Capture response headers frame written
    ArgumentCaptor<Http3HeadersFrame> captor = ArgumentCaptor.forClass(Http3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());

    Http3HeadersFrame responseFrame = captor.getValue();
    assertNotNull(responseFrame);
    assertEquals(HttpResponseStatus.OK.codeAsText(), responseFrame.headers().status());

    // Verify WT-Protocol header is present with selected protocol
    CharSequence wtProtocolHeader = responseFrame.headers().get("wt-protocol");
    assertNotNull("WT-Protocol header must be present in 200 response", wtProtocolHeader);
    assertEquals("\"chat-v2\"", wtProtocolHeader.toString());

    // Verify session received the selected subprotocol
    WebTransportSession session = mgr.get(0L);
    assertNotNull(session);
    assertEquals("chat-v2", session.getSubprotocol());
  }
}
