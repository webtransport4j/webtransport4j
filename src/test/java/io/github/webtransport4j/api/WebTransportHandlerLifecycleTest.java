package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.resilience.OverloadProtectionPolicy;
import io.github.webtransport4j.server.DefaultMessageDispatcher;
import io.github.webtransport4j.server.DefaultSessionRequestContext;
import io.github.webtransport4j.server.NettyWebTransportSession;
import io.github.webtransport4j.server.QuicChannelInitializer;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicPathEvent;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;

/** Test suite for WebTransportHandler default methods and enhanced lifecycle callbacks. */
public class WebTransportHandlerLifecycleTest {

  @Test
  public void testDefaultMethodBehaviors() {
    AtomicBoolean legacyClosedCalled = new AtomicBoolean(false);
    WebTransportHandler handler =
        new WebTransportHandler() {
          @Override
          public void onSessionClosed(@NonNull WebTransportSession session) {
            legacyClosedCalled.set(true);
          }
        };

    WebTransportSession mockSession = mock(WebTransportSession.class);
    when(mockSession.path()).thenReturn("/test");
    when(mockSession.getSessionStreamId()).thenReturn(4L);

    // Default onSessionRequest must permit by default
    SessionRequestContext mockReq = mock(SessionRequestContext.class);
    assertTrue("Default onSessionRequest must return true", handler.onSessionRequest(mockReq));

    // Default selectSubprotocol returns null
    assertNull(handler.selectSubprotocol(java.util.Collections.singletonList("proto1")));

    // onSessionClosed with code and reason delegates to legacy onSessionClosed
    handler.onSessionClosed(mockSession, 0, "Clean");
    assertTrue(
        "Overloaded onSessionClosed should delegate to legacy onSessionClosed",
        legacyClosedCalled.get());

    // Default onError and onConnectionMigration do not throw
    handler.onError(mockSession, new RuntimeException("test"));
    handler.onConnectionMigration(
        mockSession, new InetSocketAddress(1234), new InetSocketAddress(5678));
  }

  @Test
  public void testDefaultSessionRequestContext() {
    Http3Headers headers = new DefaultHttp3Headers();
    headers.path("/chat?room=dev&tag=quic&tag=webtrans");
    headers.authority("localhost:443");
    headers.add("origin", "https://example.com");
    headers.add("authorization", "Bearer token-xyz");
    headers.add("cookie", "sid=123; theme=dark; quoted=\"my-val\"");

    SocketAddress remote = new InetSocketAddress("192.168.1.100", 50000);
    SessionRequestContext ctx =
        new DefaultSessionRequestContext(headers, "/chat?room=dev&tag=quic&tag=webtrans", remote);

    assertEquals("/chat?room=dev&tag=quic&tag=webtrans", ctx.path());
    assertEquals("/chat", ctx.basePath());
    assertEquals("room=dev&tag=quic&tag=webtrans", ctx.query());
    assertEquals("dev", ctx.queryParam("room"));
    assertEquals(Arrays.asList("quic", "webtrans"), ctx.queryParams().get("tag"));
    assertNull(ctx.queryParam("nonexistent"));

    assertEquals("https://example.com", ctx.origin());
    assertEquals("localhost:443", ctx.authority());
    assertEquals(remote, ctx.remoteAddress());
    assertEquals("Bearer token-xyz", ctx.header("Authorization"));
    assertEquals("Bearer token-xyz", ctx.header("authorization"));
    assertEquals("123", ctx.cookie("sid"));
    assertEquals("dark", ctx.cookie("theme"));
    assertEquals("my-val", ctx.cookie("quoted"));
    assertNull(ctx.cookie("missing"));
  }

  @Test
  public void testHandlerRejectSessionRequest() throws Exception {
    WebTransportHandler rejectingHandler =
        new WebTransportHandler() {
          @Override
          public boolean onSessionRequest(@NonNull SessionRequestContext request) {
            return false; // Reject all incoming sessions
          }
        };

    WebTransportServer server = mock(WebTransportServer.class);
    when(server.isAcceptingSessions()).thenReturn(true);
    when(server.getHandler("/secure")).thenReturn(rejectingHandler);

    WebTransportSessionManager sessionManager = mock(WebTransportSessionManager.class);
    when(sessionManager.reserveSession(any(), any(Integer.class))).thenReturn(true);

    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockStream.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    parentAttrMap.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(sessionManager);
    parentAttrMap.attr(WebTransportAttributeKeys.CONNECTION_DRAINING).set(false);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(true);

    OverloadProtectionPolicy overloadPolicy = mock(OverloadProtectionPolicy.class);
    when(overloadPolicy.tryAcquire(any(Integer.class)))
        .thenReturn(OverloadProtectionPolicy.AdmissionResult.allowed());
    parentAttrMap.attr(WebTransportAttributeKeys.OVERLOAD_POLICY).set(overloadPolicy);

    when(mockStream.streamId()).thenReturn(0L); // client-initiated bidi stream id
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockCtx.writeAndFlush(any())).thenReturn(mockFuture);

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("localhost");
    headers.path("/secure");
    headers.set(":protocol", "webtransport");
    Http3HeadersFrame frame = new DefaultHttp3HeadersFrame(headers);

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, frame);

    // Verify rejection: 403 Forbidden sent, reservation released, and close listener attached
    verify(sessionManager).releaseReservation();
    verify(overloadPolicy, times(0)).release();
    ArgumentCaptor<DefaultHttp3HeadersFrame> captor =
        ArgumentCaptor.forClass(DefaultHttp3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    assertEquals("403", captor.getValue().headers().status().toString());
    verify(mockFuture).addListener(ChannelFutureListener.CLOSE);

    // Simulate subsequent channel close to verify closeFuture listener releases overloadPolicy
    // and does not double-release sessionManager reservation
    ArgumentCaptor<io.netty.util.concurrent.GenericFutureListener> closeListenerCaptor =
        ArgumentCaptor.forClass(io.netty.util.concurrent.GenericFutureListener.class);
    verify(mockCloseFuture).addListener(closeListenerCaptor.capture());
    closeListenerCaptor.getValue().operationComplete(mockCloseFuture);
    verify(sessionManager, times(1)).releaseReservation();
    verify(overloadPolicy, times(1)).release();
  }

  @Test
  public void testSessionClosedDeliversCloseCodeAndReason() {
    AtomicInteger observedCloseCode = new AtomicInteger(-1);
    AtomicReference<String> observedReason = new AtomicReference<>();

    WebTransportHandler handler =
        new WebTransportHandler() {
          @Override
          public void onSessionClosed(
              @NonNull WebTransportSession session, int closeCode, @Nullable String reason) {
            observedCloseCode.set(closeCode);
            observedReason.set(reason);
          }
        };

    WebTransportServer server = mock(WebTransportServer.class);
    when(server.getHandler("/chat")).thenReturn(handler);

    WebTransportSessionManager sessionManager = new WebTransportSessionManager();

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();

    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.streamId()).thenReturn(4L);
    when(mockConnectStream.closeFuture()).thenReturn(mock(ChannelFuture.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));

    parentAttrMap.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(sessionManager);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(10000L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(10L);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(10000L);
    parentAttrMap.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set("/chat");

    sessionManager.register(mockConnectStream);

    WebTransportSession session = sessionManager.get(4L);
    assertNotNull("Session should be registered", session);
    assertTrue(session instanceof NettyWebTransportSession);
    ((NettyWebTransportSession) session).setCloseCode(42);
    ((NettyWebTransportSession) session).setCloseReason("Kicked for inactivity");

    sessionManager.unregister(mockConnectStream);

    assertEquals(42, observedCloseCode.get());
    assertEquals("Kicked for inactivity", observedReason.get());
  }

  @Test
  public void testOnErrorDispatched() {
    AtomicReference<Throwable> observedError = new AtomicReference<>();
    WebTransportHandler handler =
        new WebTransportHandler() {
          @Override
          public void onError(@NonNull WebTransportSession session, @NonNull Throwable cause) {
            observedError.set(cause);
          }
        };

    WebTransportServer server = mock(WebTransportServer.class);
    when(server.getHandler("/chat")).thenReturn(handler);

    NettyWebTransportSession session = mock(NettyWebTransportSession.class);
    when(session.path()).thenReturn("/chat");
    when(session.getSessionStreamId()).thenReturn(4L);

    WebTransportSessionManager sessionManager = mock(WebTransportSessionManager.class);
    when(sessionManager.get(4L)).thenReturn(session);

    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));

    parentAttrMap.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(sessionManager);
    streamAttrMap.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(4L);

    RuntimeException ex = new RuntimeException("Stream decode failure");
    DefaultMessageDispatcher.INSTANCE.exceptionCaught(mockCtx, ex);

    assertEquals(ex, observedError.get());
  }

  @Test
  public void testConnectionMigrationDispatchedToHandler() throws Exception {
    AtomicReference<SocketAddress> observedOld = new AtomicReference<>();
    AtomicReference<SocketAddress> observedNew = new AtomicReference<>();

    WebTransportHandler handler =
        new WebTransportHandler() {
          @Override
          public void onConnectionMigration(
              @NonNull WebTransportSession session,
              @NonNull SocketAddress oldAddress,
              @NonNull SocketAddress newAddress) {
            observedOld.set(oldAddress);
            observedNew.set(newAddress);
          }
        };

    WebTransportServer server = mock(WebTransportServer.class);
    when(server.getHandler("/chat")).thenReturn(handler);

    NettyWebTransportSession session = mock(NettyWebTransportSession.class);
    when(session.path()).thenReturn("/chat");
    when(session.getSessionStreamId()).thenReturn(10L);

    WebTransportSessionManager mgr = mock(WebTransportSessionManager.class);
    when(mgr.getSessions()).thenReturn(java.util.Collections.singletonList(session));

    QuicChannel mockQuic = mock(QuicChannel.class);
    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    parentAttrMap.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    InetSocketAddress initialClientAddr = new InetSocketAddress("127.0.0.1", 12345);
    when(mockQuic.remoteSocketAddress()).thenReturn(initialClientAddr);

    ChannelInboundHandlerAdapter migrationHandler =
        QuicChannelInitializer.createMigrationHandler(mockQuic);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.channel()).thenReturn(mockQuic);

    InetSocketAddress newClientAddr = new InetSocketAddress("10.0.0.1", 54321);
    QuicPathEvent.PeerMigrated migratedEvent =
        new QuicPathEvent.PeerMigrated(new InetSocketAddress(443), newClientAddr);

    migrationHandler.userEventTriggered(mockCtx, migratedEvent);

    assertEquals(initialClientAddr, observedOld.get());
    assertEquals(newClientAddr, observedNew.get());
  }

  @Test
  public void testHandlerExceptionInOnSessionRequestRejectsWithoutLeak() throws Exception {
    WebTransportHandler throwingHandler =
        new WebTransportHandler() {
          @Override
          public boolean onSessionRequest(@NonNull SessionRequestContext request) {
            throw new RuntimeException("Simulated crash during admission");
          }
        };

    WebTransportServer server = mock(WebTransportServer.class);
    when(server.isAcceptingSessions()).thenReturn(true);
    when(server.getHandler("/crashing")).thenReturn(throwingHandler);

    WebTransportSessionManager sessionManager = mock(WebTransportSessionManager.class);
    when(sessionManager.reserveSession(any(), any(Integer.class))).thenReturn(true);

    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));
    when(mockStream.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    parentAttrMap.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(sessionManager);
    parentAttrMap.attr(WebTransportAttributeKeys.CONNECTION_DRAINING).set(false);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
    parentAttrMap.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(true);

    when(mockStream.streamId()).thenReturn(0L);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockCtx.writeAndFlush(any())).thenReturn(mockFuture);

    Http3Headers headers = new DefaultHttp3Headers();
    headers.method("CONNECT");
    headers.scheme("https");
    headers.authority("localhost");
    headers.path("/crashing");
    headers.set(":protocol", "webtransport");
    Http3HeadersFrame frame = new DefaultHttp3HeadersFrame(headers);

    WebTransportHeadersHandler.INSTANCE.channelRead(mockCtx, frame);

    // Verify rejection: 403 Forbidden sent, reservation released, and close listener attached
    verify(sessionManager).releaseReservation();
    ArgumentCaptor<DefaultHttp3HeadersFrame> captor =
        ArgumentCaptor.forClass(DefaultHttp3HeadersFrame.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    assertEquals("403", captor.getValue().headers().status().toString());
    verify(mockFuture).addListener(ChannelFutureListener.CLOSE);

    // Simulate subsequent channel close to verify closeFuture listener does not double-release
    ArgumentCaptor<io.netty.util.concurrent.GenericFutureListener> closeListenerCaptor =
        ArgumentCaptor.forClass(io.netty.util.concurrent.GenericFutureListener.class);
    verify(mockCloseFuture).addListener(closeListenerCaptor.capture());
    closeListenerCaptor.getValue().operationComplete(mockCloseFuture);
    verify(sessionManager, times(1)).releaseReservation();
  }

  @Test
  public void testServerPathResolutionWithQueryString() {
    WebTransportServer server = new WebTransportServer();
    WebTransportHandler chatHandler = mock(WebTransportHandler.class);
    server.registerHandler("/chat", chatHandler);

    assertEquals(chatHandler, server.getHandler("/chat"));
    assertEquals(chatHandler, server.getHandler("/chat?token=123"));
    assertEquals(chatHandler, server.getHandler("/chat/?token=123"));
    assertEquals(chatHandler, server.getHandler("/chat?room=general&user=alice"));
    assertFalse(chatHandler.equals(server.getHandler("/unknown?token=123")));
  }

  @Test
  public void testReactiveWebTransportHandlerAdapterLifecycle() {
    AtomicBoolean reqCalled = new AtomicBoolean(false);
    AtomicInteger closedCode = new AtomicInteger(-1);
    AtomicReference<String> closedReason = new AtomicReference<>();
    AtomicReference<Throwable> errorRef = new AtomicReference<>();
    AtomicReference<SocketAddress> migRef = new AtomicReference<>();

    ReactiveWebTransportHandler reactiveHandler =
        new ReactiveWebTransportHandler() {
          @Override
          public boolean onSessionRequest(@NonNull SessionRequestContext requestContext) {
            reqCalled.set(true);
            return true;
          }

          @Override
          public @NonNull Publisher<Void> onSessionClosed(
              @NonNull ReactiveWebTransportSession session, int closeCode, @Nullable String reason) {
            closedCode.set(closeCode);
            closedReason.set(reason);
            return EmptyPublisher.<Void>instance();
          }

          @Override
          public @NonNull Publisher<Void> onError(
              @NonNull ReactiveWebTransportSession session, @NonNull Throwable cause) {
            errorRef.set(cause);
            return EmptyPublisher.<Void>instance();
          }

          @Override
          public @NonNull Publisher<Void> onConnectionMigration(
              @NonNull ReactiveWebTransportSession session,
              @NonNull SocketAddress oldAddress,
              @NonNull SocketAddress newAddress) {
            migRef.set(newAddress);
            return EmptyPublisher.<Void>instance();
          }
        };

    ReactiveWebTransportHandlerAdapter adapter =
        new ReactiveWebTransportHandlerAdapter(reactiveHandler);

    SessionRequestContext reqCtx = mock(SessionRequestContext.class);
    assertTrue(adapter.onSessionRequest(reqCtx));
    assertTrue(reqCalled.get());

    WebTransportSession session = mock(WebTransportSession.class);
    when(session.getSessionStreamId()).thenReturn(8L);
    when(session.getCloseCode()).thenReturn(404);
    when(session.getCloseReason()).thenReturn("Gone");

    adapter.onSessionReady(session);

    Throwable err = new RuntimeException("reactive error");
    adapter.onError(session, err);
    assertEquals(err, errorRef.get());

    SocketAddress addr1 = new InetSocketAddress("1.1.1.1", 1111);
    SocketAddress addr2 = new InetSocketAddress("2.2.2.2", 2222);
    adapter.onConnectionMigration(session, addr1, addr2);
    assertEquals(addr2, migRef.get());

    adapter.onSessionClosed(session, 404, "Gone");
    assertEquals(404, closedCode.get());
    assertEquals("Gone", closedReason.get());
  }
}
