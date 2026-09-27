package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.DefaultWebTransportSession;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamPriority;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.Attribute;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/** Tests for stream creation with {@link StreamPriority} in {@link WebTransportSession}. */
public class WebTransportSessionPriorityTest {

  private QuicStreamChannel mockConnectStream;
  private QuicChannel mockQuicChannel;
  private EventLoop mockEventLoop;
  private DefaultWebTransportSession session;
  private QuicStreamChannel mockCreatedStream;
  private ChannelFuture mockPriorityFuture;

  /** Sets up test mocks before each test. */
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    mockConnectStream = mock(QuicStreamChannel.class);
    mockQuicChannel = mock(QuicChannel.class);
    mockEventLoop = mock(EventLoop.class);
    mockCreatedStream = mock(QuicStreamChannel.class);
    mockPriorityFuture = mock(ChannelFuture.class);

    when(mockConnectStream.parent()).thenReturn(mockQuicChannel);
    when(mockConnectStream.streamId()).thenReturn(0L);

    when(mockQuicChannel.eventLoop()).thenReturn(mockEventLoop);
    when(mockEventLoop.newPromise()).thenAnswer(inv -> new DefaultPromise<>(ImmediateEventExecutor.INSTANCE));

    session =
        new DefaultWebTransportSession(
            0L,
            mockConnectStream,
            "/test",
            100L,
            100L,
            10000L,
            100L,
            100L,
            10000L,
            true,
            false);

    WebTransportSessionManager sessionManager = mock(WebTransportSessionManager.class);
    when(sessionManager.get(0L)).thenReturn(session);

    Attribute<WebTransportSessionManager> sessionMgrAttr = mock(Attribute.class);
    when(sessionMgrAttr.get()).thenReturn(sessionManager);
    when(mockQuicChannel.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(sessionMgrAttr);

    Attribute<Long> peerBidiAttr = mock(Attribute.class);
    when(peerBidiAttr.get()).thenReturn(100L);
    when(mockQuicChannel.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(peerBidiAttr);

    Attribute<Long> peerUniAttr = mock(Attribute.class);
    when(peerUniAttr.get()).thenReturn(100L);
    when(mockQuicChannel.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI)).thenReturn(peerUniAttr);

    // Setup mock created stream
    when(mockCreatedStream.streamId()).thenReturn(4L);
    when(mockCreatedStream.parent()).thenReturn(mockQuicChannel);
    when(mockCreatedStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    ChannelPromise closePromise = new DefaultChannelPromise(mockCreatedStream);
    when(mockCreatedStream.closeFuture()).thenReturn(closePromise);
    when(mockCreatedStream.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

    Attribute<WebTransportStream> streamAttr = mock(Attribute.class);
    when(mockCreatedStream.attr(WebTransportAttributeKeys.WT_STREAM_KEY)).thenReturn(streamAttr);

    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockCreatedStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    Attribute<Long> streamTypeAttr = mock(Attribute.class);
    when(mockCreatedStream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);

    Attribute<Boolean> srvInitAttr = mock(Attribute.class);
    when(mockCreatedStream.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY)).thenReturn(srvInitAttr);

    when(mockCreatedStream.updatePriority(any())).thenReturn(mockPriorityFuture);
    when(mockPriorityFuture.isDone()).thenReturn(true);
    when(mockPriorityFuture.isSuccess()).thenReturn(true);
  }

  private void mockCreateStreamSuccess(QuicStreamType expectedType) {
    when(mockCreatedStream.type()).thenReturn(expectedType);
    when(mockQuicChannel.createStream(any(), any())).thenAnswer(invocation -> {
      DefaultPromise<QuicStreamChannel> fut = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
      fut.setSuccess(mockCreatedStream);
      return fut;
    });
  }

  @Test
  public void testCreateUniStreamWithStreamPriority() throws Exception {
    mockCreateStreamSuccess(QuicStreamType.UNIDIRECTIONAL);

    CompletableFuture<WebTransportStream> future =
        session.createUniStream(StreamPriority.of(1, true));
    WebTransportStream stream = future.get();
    assertNotNull(stream);

    ArgumentCaptor<QuicStreamPriority> captor = ArgumentCaptor.forClass(QuicStreamPriority.class);
    verify(mockCreatedStream).updatePriority(captor.capture());
    assertEquals(1, captor.getValue().urgency());
    assertTrue(captor.getValue().isIncremental());
  }

  @Test
  public void testCreateUniStreamWithUrgencyAndIncremental() throws Exception {
    mockCreateStreamSuccess(QuicStreamType.UNIDIRECTIONAL);

    CompletableFuture<WebTransportStream> future = session.createUniStream(5, false);
    WebTransportStream stream = future.get();
    assertNotNull(stream);

    ArgumentCaptor<QuicStreamPriority> captor = ArgumentCaptor.forClass(QuicStreamPriority.class);
    verify(mockCreatedStream).updatePriority(captor.capture());
    assertEquals(5, captor.getValue().urgency());
    assertFalse(captor.getValue().isIncremental());
  }

  @Test
  public void testCreateBiStreamWithStreamPriority() throws Exception {
    mockCreateStreamSuccess(QuicStreamType.BIDIRECTIONAL);

    CompletableFuture<WebTransportStream> future =
        session.createBiStream(StreamPriority.of(0, false));
    WebTransportStream stream = future.get();
    assertNotNull(stream);

    ArgumentCaptor<QuicStreamPriority> captor = ArgumentCaptor.forClass(QuicStreamPriority.class);
    verify(mockCreatedStream).updatePriority(captor.capture());
    assertEquals(0, captor.getValue().urgency());
    assertFalse(captor.getValue().isIncremental());
  }

  @Test
  public void testCreateBiStreamWithUrgencyAndIncremental() throws Exception {
    mockCreateStreamSuccess(QuicStreamType.BIDIRECTIONAL);

    CompletableFuture<WebTransportStream> future = session.createBiStream(2, true);
    WebTransportStream stream = future.get();
    assertNotNull(stream);

    ArgumentCaptor<QuicStreamPriority> captor = ArgumentCaptor.forClass(QuicStreamPriority.class);
    verify(mockCreatedStream).updatePriority(captor.capture());
    assertEquals(2, captor.getValue().urgency());
    assertTrue(captor.getValue().isIncremental());
  }

  @Test
  public void testReactiveSessionPriorityStreamCreation() {
    mockCreateStreamSuccess(QuicStreamType.BIDIRECTIONAL);
    ReactiveWebTransportSession reactiveSession = new ReactiveWebTransportSession(session);

    AtomicReference<ReactiveWebTransportStream> streamRef = new AtomicReference<>();
    reactiveSession.createBiStream(StreamPriority.of(1, true)).subscribe(new Subscriber<ReactiveWebTransportStream>() {
      @Override
      public void onSubscribe(Subscription s) {
        s.request(1);
      }

      @Override
      public void onNext(ReactiveWebTransportStream s) {
        streamRef.set(s);
      }

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onComplete() {}
    });

    assertNotNull(streamRef.get());
    ArgumentCaptor<QuicStreamPriority> captor = ArgumentCaptor.forClass(QuicStreamPriority.class);
    verify(mockCreatedStream).updatePriority(captor.capture());
    assertEquals(1, captor.getValue().urgency());
    assertTrue(captor.getValue().isIncremental());
  }
}
