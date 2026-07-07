package io.github.webtransport4j.server;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Bounded unit tests verifying WebTransport session resumption flows. */
public class SessionResumptionTest {

  @Before
  public void setUp() {
    System.setProperty("webtransport4j.session.resumption.timeout.seconds", "60");
    System.setProperty("webtransport4j.webtransport.max_sessions_per_connection", "5");
  }

  @After
  public void tearDown() {
    System.clearProperty("webtransport4j.session.resumption.timeout.seconds");
    System.clearProperty("webtransport4j.webtransport.max_sessions_per_connection");
  }

  @SuppressWarnings("unchecked")
  private WebTransportHandler setupQuicParentAttributes(QuicChannel parent, WebTransportSessionManager mgr) {
    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(parent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    Attribute<List<String>> originsAttr = mock(Attribute.class);
    when(originsAttr.get()).thenReturn(Collections.singletonList("*"));
    when(parent.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS)).thenReturn(originsAttr);

    Attribute<java.util.concurrent.atomic.AtomicInteger> globalCountAttr = mock(Attribute.class);
    when(globalCountAttr.get()).thenReturn(new java.util.concurrent.atomic.AtomicInteger(0));
    when(parent.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT)).thenReturn(globalCountAttr);

    Attribute<Boolean> peerSettingsReceivedAttr = mock(Attribute.class);
    when(peerSettingsReceivedAttr.get()).thenReturn(true);
    when(parent.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED)).thenReturn(peerSettingsReceivedAttr);

    Attribute<Boolean> peerSettingsValidAttr = mock(Attribute.class);
    when(peerSettingsValidAttr.get()).thenReturn(true);
    when(parent.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID)).thenReturn(peerSettingsValidAttr);

    Attribute<String> pathAttr = mock(Attribute.class);
    when(pathAttr.get()).thenReturn("/test");
    when(parent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

    Attribute<Long> limitAttr = mock(Attribute.class);
    when(limitAttr.get()).thenReturn(100L);
    when(parent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(limitAttr);
    when(parent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI)).thenReturn(limitAttr);
    when(parent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA)).thenReturn(limitAttr);

    Attribute<WebTransportServer> serverAttr = mock(Attribute.class);
    WebTransportServer mockServer = mock(WebTransportServer.class);
    WebTransportHandler mockHandler = mock(WebTransportHandler.class);
    when(mockServer.getHandler(any())).thenReturn(mockHandler);
    when(serverAttr.get()).thenReturn(mockServer);
    when(parent.attr(WebTransportAttributeKeys.SERVER_KEY)).thenReturn(serverAttr);

    return mockHandler;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSuccessfulSessionResumption() throws Exception {
    // 1. Setup a session to be resumed
    QuicStreamChannel oldConnectStream = mock(QuicStreamChannel.class);
    when(oldConnectStream.streamId()).thenReturn(100L);
    when(oldConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    WebTransportSession session = new WebTransportSession(
        100L, oldConnectStream, "/test", 10, 10, 10000, 10, 10, 10000, false, false);
    String token = session.getResumptionToken();

    // Register it as orphaned
    SessionResumptionManager.getInstance().registerOrphanedSession(token, session);

    // 2. Mock new connect handshake on a different connection
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel newConnectStream = mock(QuicStreamChannel.class);
    QuicChannel newParent = mock(QuicChannel.class);
    ChannelPipeline pipeline = mock(ChannelPipeline.class);

    when(mockCtx.channel()).thenReturn(newConnectStream);
    when(mockCtx.pipeline()).thenReturn(pipeline);
    when(newConnectStream.streamId()).thenReturn(200L);
    when(newConnectStream.parent()).thenReturn(newParent);
    when(newConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(newConnectStream.closeFuture()).thenReturn(mockCloseFuture);

    WebTransportSessionManager newMgr = new WebTransportSessionManager();
    WebTransportHandler mockHandler = setupQuicParentAttributes(newParent, newMgr);

    when(newConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(mock(Attribute.class));

    // Mock incoming headers frame with resumption token
    Http3HeadersFrame headersFrame = mock(Http3HeadersFrame.class);
    Http3Headers headers = new io.netty.handler.codec.http3.DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("localhost");
    headers.path("/test");
    headers.set(":protocol", "webtransport");
    headers.set("webtransport-resumption-token", token);
    when(headersFrame.headers()).thenReturn(headers);

    WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
    handler.channelRead(mockCtx, headersFrame);

    // 3. Verifications
    // Verify session was retrieved and removed from the cache
    assertNull(SessionResumptionManager.getInstance().retrieveAndRemove(token));

    // Verify session was added to the new manager
    assertNotNull(newMgr.get(200L));
    assertEquals(session, newMgr.get(200L));

    // Verify onSessionResumed callback was triggered on the handler
    verify(mockHandler).onSessionResumed(session);

    // Verify response headers contain 200 OK and the custom header
    ArgumentCaptor<Object> writeCaptor = ArgumentCaptor.forClass(Object.class);
    verify(mockCtx).writeAndFlush(writeCaptor.capture());
    DefaultHttp3HeadersFrame responseFrame = (DefaultHttp3HeadersFrame) writeCaptor.getValue();
    assertEquals("200", responseFrame.headers().status().toString());
    assertNotEquals(token, responseFrame.headers().get("sec-webtransport-resumption-token").toString());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInvalidTokenFallsbackToNewSession() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel newConnectStream = mock(QuicStreamChannel.class);
    QuicChannel newParent = mock(QuicChannel.class);
    ChannelPipeline pipeline = mock(ChannelPipeline.class);

    when(mockCtx.channel()).thenReturn(newConnectStream);
    when(mockCtx.pipeline()).thenReturn(pipeline);
    when(newConnectStream.streamId()).thenReturn(300L);
    when(newConnectStream.parent()).thenReturn(newParent);
    when(newConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(newConnectStream.closeFuture()).thenReturn(mockCloseFuture);

    WebTransportSessionManager newMgr = new WebTransportSessionManager();
    WebTransportHandler mockHandler = setupQuicParentAttributes(newParent, newMgr);

    when(newConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(mock(Attribute.class));

    // Mock incoming headers frame with invalid token
    Http3HeadersFrame headersFrame = mock(Http3HeadersFrame.class);
    Http3Headers headers = new io.netty.handler.codec.http3.DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("localhost");
    headers.path("/test");
    headers.set(":protocol", "webtransport");
    headers.set("webtransport-resumption-token", "invalid-token-12345");
    when(headersFrame.headers()).thenReturn(headers);

    WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
    handler.channelRead(mockCtx, headersFrame);

    // Verify a brand new session was registered instead
    WebTransportSession newSession = newMgr.get(300L);
    assertNotNull(newSession);
    assertNotEquals("invalid-token-12345", newSession.getResumptionToken());

    // Verify onSessionReady callback was triggered on the handler for the new session
    verify(mockHandler).onSessionReady(newSession);

    // Verify response headers contain the newly generated resumption token
    ArgumentCaptor<Object> writeCaptor = ArgumentCaptor.forClass(Object.class);
    verify(mockCtx).writeAndFlush(writeCaptor.capture());
    DefaultHttp3HeadersFrame responseFrame = (DefaultHttp3HeadersFrame) writeCaptor.getValue();
    assertEquals("200", responseFrame.headers().status().toString());
    assertEquals(newSession.getResumptionToken(), responseFrame.headers().get("sec-webtransport-resumption-token").toString());
  }

  @Test
  public void testSessionEvictionOnExpiration() throws Exception {
    QuicStreamChannel oldConnectStream = mock(QuicStreamChannel.class);
    when(oldConnectStream.streamId()).thenReturn(400L);
    when(oldConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

    WebTransportSession session = new WebTransportSession(
        400L, oldConnectStream, "/test", 10, 10, 10000, 10, 10, 10000, false, false);
    String token = session.getResumptionToken();

    // Register with short custom timeout using system property
    System.setProperty("webtransport4j.session.resumption.timeout.seconds", "0");
    WebTransportConfig.reload();
    try {
      SessionResumptionManager.getInstance().registerOrphanedSession(token, session);

      // Sleep briefly to ensure the system clock moves forward beyond the registered expiration time
      Thread.sleep(10);

      // Invoke expiration cleanup using reflection
      java.lang.reflect.Method cleanMethod = SessionResumptionManager.class.getDeclaredMethod("cleanExpiredSessions");
      cleanMethod.setAccessible(true);
      cleanMethod.invoke(SessionResumptionManager.getInstance());

      // Assert session has been evicted and returns null
      assertNull(SessionResumptionManager.getInstance().retrieveAndRemove(token));
    } finally {
      System.setProperty("webtransport4j.session.resumption.timeout.seconds", "60");
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testSessionResumptionDisabledOption() throws Exception {
    System.setProperty("webtransport4j.session.resumption.enabled", "false");
    WebTransportConfig.reload();

    try {
      // 1. Setup a session
      QuicStreamChannel oldConnectStream = mock(QuicStreamChannel.class);
      when(oldConnectStream.streamId()).thenReturn(500L);
      QuicChannel parent = mock(QuicChannel.class);
      when(oldConnectStream.parent()).thenReturn(parent);
      when(oldConnectStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

      WebTransportSessionManager mgr = new WebTransportSessionManager();
      setupQuicParentAttributes(parent, mgr);

      WebTransportSession session = new WebTransportSession(
          500L, oldConnectStream, "/test", 10, 10, 10000, 10, 10, 10000, false, false);
      
      // Attempt to unregister (which would normally register it in resumption manager)
      mgr.register(oldConnectStream);
      mgr.unregister(oldConnectStream);

      // Verify session was NOT registered in resumption manager
      assertNull(SessionResumptionManager.getInstance().retrieveAndRemove(session.getResumptionToken()));

      // 2. Setup mock handshake with a token
      ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
      QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
      QuicChannel mockParent = mock(QuicChannel.class);
      when(mockStream.parent()).thenReturn(mockParent);
      ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
      when(mockStream.closeFuture()).thenReturn(mockCloseFuture);
      when(mockCtx.channel()).thenReturn(mockStream);
      setupQuicParentAttributes(mockParent, mgr);

      Http3HeadersFrame headersFrame = mock(Http3HeadersFrame.class);
      Http3Headers headers = new io.netty.handler.codec.http3.DefaultHttp3Headers();
      headers.method("CONNECT");
      headers.scheme("https");
      headers.authority("localhost");
      headers.path("/test");
      headers.set(":protocol", "webtransport");
      headers.set("webtransport-resumption-token", "some-token");
      when(headersFrame.headers()).thenReturn(headers);

      // Simulate handshake in headers handler
      WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
      handler.channelRead(mockCtx, headersFrame);

      // Verify no resumption was performed (response headers did not contain resumption token)
      ArgumentCaptor<DefaultHttp3HeadersFrame> captor = ArgumentCaptor.forClass(DefaultHttp3HeadersFrame.class);
      verify(mockCtx).writeAndFlush(captor.capture());
      assertFalse(captor.getValue().headers().contains("sec-webtransport-resumption-token"));

    } finally {
      System.clearProperty("webtransport4j.session.resumption.enabled");
      WebTransportConfig.reload();
    }
  }
}
