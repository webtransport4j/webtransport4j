package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WebTransportApiTest {

  private WebTransportServer server;

  @Before
  public void setUp() {
    server = new WebTransportServer();
  }

  @After
  public void tearDown() {
    server = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSessionLifecycleAndHandlerRegistration() throws Exception {
    AtomicBoolean sessionReadyCalled = new AtomicBoolean(false);
    AtomicBoolean sessionClosedCalled = new AtomicBoolean(false);
    AtomicReference<WebTransportSession> registeredSession = new AtomicReference<>();

    WebTransportHandler handler = new WebTransportHandler() {
      @Override
      public void onSessionReady(@NonNull WebTransportSession session) {
        sessionReadyCalled.set(true);
        registeredSession.set(session);
      }

      @Override
      public void onSessionClosed(@NonNull WebTransportSession session) {
        sessionClosedCalled.set(true);
      }

      @Override
      public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {}

      @Override
      public void onDatagramReceived(@NonNull WebTransportSession session, @NonNull ByteBuf data) {}
    };

    server.registerHandler("/test-api", handler);
    assertEquals(handler, server.getHandler("/test-api"));

    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));

    Attribute<WebTransportServer> serverAttr = mock(Attribute.class);
    when(serverAttr.get()).thenReturn(server);
    when(mockParent.attr(WebTransportAttributeKeys.SERVER_KEY)).thenReturn(serverAttr);

    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    Attribute<Long> localLimitAttr = mock(Attribute.class);
    when(localLimitAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(localLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI)).thenReturn(localLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA)).thenReturn(localLimitAttr);

    Attribute<Long> peerLimitAttr = mock(Attribute.class);
    when(peerLimitAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(peerLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI)).thenReturn(peerLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)).thenReturn(peerLimitAttr);

    Attribute<String> pathAttr = mock(Attribute.class);
    when(pathAttr.get()).thenReturn("/test-api");
    when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    mgr.register(mockConnectStream);

    assertTrue(sessionReadyCalled.get());
    assertNotNull(registeredSession.get());
    assertEquals("/test-api", registeredSession.get().path());

    mgr.unregister(mockConnectStream);

    assertTrue(sessionClosedCalled.get());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testIncomingStreamAndDataDispatch() throws Exception {
    AtomicReference<WebTransportStream> incomingStreamRef = new AtomicReference<>();
    AtomicReference<String> dataReceivedRef = new AtomicReference<>();

    WebTransportHandler handler = new WebTransportHandler() {
      @Override
      public void onSessionReady(@NonNull WebTransportSession session) {}

      @Override
      public void onSessionClosed(@NonNull WebTransportSession session) {}

      @Override
      public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
        incomingStreamRef.set(stream);
        stream.onData(buf -> {
          dataReceivedRef.set(buf.toString(StandardCharsets.UTF_8));
        });
      }

      @Override
      public void onDatagramReceived(@NonNull WebTransportSession session, @NonNull ByteBuf data) {}
    };

    server.registerHandler("/test-api", handler);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockConnectStream.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));

    Attribute<WebTransportServer> serverAttr = mock(Attribute.class);
    when(serverAttr.get()).thenReturn(server);
    when(mockParent.attr(WebTransportAttributeKeys.SERVER_KEY)).thenReturn(serverAttr);

    Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    Attribute<Long> localLimitAttr = mock(Attribute.class);
    when(localLimitAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(localLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI)).thenReturn(localLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA)).thenReturn(localLimitAttr);
    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(mock(Attribute.class));

    Attribute<String> pathAttr = mock(Attribute.class);
    when(pathAttr.get()).thenReturn("/test-api");
    when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

    mgr.register(mockConnectStream);

    MessageDispatcher dispatcher = new MessageDispatcher();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStreamChannel = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStreamChannel);
    when(mockStreamChannel.parent()).thenReturn(mockParent);
    when(mockConnectStream.streamId()).thenReturn(100L);
    when(mockStreamChannel.streamId()).thenReturn(200L);
    when(mockStreamChannel.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));

    Attribute<WebTransportStream> streamAttr = mock(Attribute.class);
    when(mockStreamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY)).thenReturn(streamAttr);

    Attribute<Boolean> notifiedAttr = mock(Attribute.class);
    when(mockStreamChannel.attr(WebTransportAttributeKeys.STREAM_NOTIFIED)).thenReturn(notifiedAttr);

    Attribute<Boolean> serverInitAttr = mock(Attribute.class);
    when(mockStreamChannel.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY)).thenReturn(serverInitAttr);

    when(mockParent.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR)).thenReturn(mock(Attribute.class));

    ByteBuf data = Unpooled.copiedBuffer("Hello API", StandardCharsets.UTF_8);
    WebTransportStreamFrame frame = new WebTransportStreamFrame(100L, 200L, true, data);

    dispatcher.channelRead(mockCtx, frame);

    // Wait for the asynchronous businessPool task execution to finish
    Thread.sleep(200);

    assertNotNull(incomingStreamRef.get());
    assertEquals("Hello API", dataReceivedRef.get());
  }

  @Test
  public void testPathNormalization() {
    WebTransportHandler handler = new WebTransportHandler() {};
    server.registerHandler("/chat/", handler);
    assertEquals(handler, server.getHandler("/chat"));
    assertEquals(handler, server.getHandler("/chat/"));

    server.registerHandler("/root", handler);
    assertEquals(handler, server.getHandler("/root/"));
  }
}
