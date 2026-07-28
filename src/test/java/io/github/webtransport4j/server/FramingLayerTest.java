package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamChannelConfig;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for framing layer. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class FramingLayerTest {
  private static final Logger log = LoggerFactory.getLogger(FramingLayerTest.class);

  @Test
  public void testDatagramFraming() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebTransportDatagramDecoder());

    ByteBuf input = Unpooled.buffer();
    // Write Session ID (varint 40)
    WebTransportUtils.writeVarInt(input, 40);
    // Write Payload "Hello"
    input.writeBytes("Hello".getBytes(StandardCharsets.UTF_8));

    assertTrue(channel.writeInbound(input));

    Object output = channel.readInbound();
    assertTrue(output instanceof WebTransportDatagramFrame);

    WebTransportDatagramFrame frame = (WebTransportDatagramFrame) output;
    assertEquals(40L<<2, frame.sessionId());
    assertEquals("Hello", frame.content().toString(StandardCharsets.UTF_8));
    frame.release();
  }

  @Test
  public void testDataHandlerCapsuleParsing() {
    EmbeddedChannel channel = new EmbeddedChannel();
    channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(100L);
    channel
        .pipeline()
        .addLast(new Http3DataToByteBufHandler())
        .addLast(new WebTransportCapsuleDecoder());
    final WebTransportCapsule[] received = new WebTransportCapsule[1];
    channel
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof WebTransportCapsule) {
                  received[0] = (WebTransportCapsule) msg;
                } else {
                  ctx.fireChannelRead(msg);
                }
              }
            });

    ByteBuf input = Unpooled.buffer();
    // Capsule Type = 0x2843 (CLOSE_WEBTRANSPORT_SESSION)
    WebTransportUtils.writeVarInt(input, 0x2843);
    // Length = 0
    WebTransportUtils.writeVarInt(input, 0);

    Http3DataFrame frame = new DefaultHttp3DataFrame(input);
    channel.writeInbound(frame);

    assertNotNull(received[0]);
    assertEquals(100L, received[0].sessionId());
    assertEquals(0x2843, received[0].capsuleType());
    received[0].release();
  }

  @Test
  public void testDataHandlerCapsuleFragmentation() {
    EmbeddedChannel channel = new EmbeddedChannel();
    channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(100L);
    channel
        .pipeline()
        .addLast(new Http3DataToByteBufHandler())
        .addLast(new WebTransportCapsuleDecoder());
    final WebTransportCapsule[] received = new WebTransportCapsule[1];
    channel
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof WebTransportCapsule) {
                  received[0] = (WebTransportCapsule) msg;
                } else {
                  ctx.fireChannelRead(msg);
                }
              }
            });

    // Step 1: Send only 1 byte (incomplete capsule header)
    ByteBuf part1 = Unpooled.buffer();
    WebTransportUtils.writeVarInt(part1, 0x2843);
    int headerSize = part1.readableBytes();
    ByteBuf truncatedPart = part1.readSlice(headerSize - 1); // remove the last byte of header

    channel.writeInbound(new DefaultHttp3DataFrame(truncatedPart));
    assertNull(received[0]); // Verification: No capsule fired yet

    // Step 2: Send remaining bytes to complete the capsule header & value
    ByteBuf part2 = Unpooled.buffer();
    // Write the missing byte from previous varint (0x43)
    part2.writeByte(0x43);
    // Write length 5
    WebTransportUtils.writeVarInt(part2, 5);
    // Write content "Hello"
    part2.writeBytes("Hello".getBytes(StandardCharsets.UTF_8));

    channel.writeInbound(new DefaultHttp3DataFrame(part2));
    assertNotNull(received[0]);
    assertEquals(100L, received[0].sessionId());
    assertEquals(0x2843, received[0].capsuleType());
    assertEquals("Hello", received[0].content().toString(StandardCharsets.UTF_8));
    received[0].release();
  }

  @Test
  public void testDataHandlerMultipleCapsules() {
    EmbeddedChannel channel = new EmbeddedChannel();
    channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(100L);
    channel
        .pipeline()
        .addLast(new Http3DataToByteBufHandler())
        .addLast(new WebTransportCapsuleDecoder());

    final List<WebTransportCapsule> list = new ArrayList<>();
    channel
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof WebTransportCapsule) {
                  list.add((WebTransportCapsule) msg);
                } else {
                  ctx.fireChannelRead(msg);
                }
              }
            });

    ByteBuf input = Unpooled.buffer();
    // Capsule 1: DRAIN (0x2844), len = 2, value = "HI"
    WebTransportUtils.writeVarInt(input, 0x2844);
    WebTransportUtils.writeVarInt(input, 2);
    input.writeBytes("HI".getBytes(StandardCharsets.UTF_8));

    // Capsule 2: CLOSE (0x2843), len = 3, value = "BYE"
    WebTransportUtils.writeVarInt(input, 0x2843);
    WebTransportUtils.writeVarInt(input, 3);
    input.writeBytes("BYE".getBytes(StandardCharsets.UTF_8));

    channel.writeInbound(new DefaultHttp3DataFrame(input));

    assertEquals(2, list.size());
    assertEquals(0x2844, list.get(0).capsuleType());
    assertEquals("HI", list.get(0).content().toString(StandardCharsets.UTF_8));
    assertEquals(0x2843, list.get(1).capsuleType());
    assertEquals("BYE", list.get(1).content().toString(StandardCharsets.UTF_8));

    list.get(0).release();
    list.get(1).release();
  }

  @Test
  public void testStreamFrameDecoderDirectly() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);

    Attribute<Long> typeAttr = mock(Attribute.class);
    Attribute<Long> sessIdAttr = mock(Attribute.class);

    when(mockStream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(typeAttr);
    when(typeAttr.get()).thenReturn(0x41L); // BIDIRECTIONAL

    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);
    when(sessIdAttr.get()).thenReturn(42L);

    when(mockStream.streamId()).thenReturn(99L);

    ByteBuf input = Unpooled.copiedBuffer("Hello".getBytes(StandardCharsets.UTF_8));
    WebTransportStreamFrameDecoder decoder = new WebTransportStreamFrameDecoder();
    decoder.channelRead(mockCtx, input);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockCtx).fireChannelRead(captor.capture());

    Object output = captor.getValue();
    assertTrue(output instanceof WebTransportStreamFrame);
    WebTransportStreamFrame frame = (WebTransportStreamFrame) output;
    assertEquals(42L, frame.sessionId());
    assertEquals(99L, frame.streamId());
    assertTrue(frame.isBidirectional());
    assertEquals("Hello", frame.content().toString(StandardCharsets.UTF_8));

    frame.release();
  }

  @Test
  public void testMessageDispatcherStreamFrame() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockStream.eventLoop()).thenReturn(mockEventLoop);
    doAnswer(
            invocation -> {
              Runnable r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(mockEventLoop)
        .execute(any(Runnable.class));

    QuicStreamChannelConfig mockConfig =
        mock(QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    when(mockConfig.isAutoRead()).thenReturn(true);

    Attribute<StreamMailbox> mailboxAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_MAILBOX_KEY)).thenReturn(mailboxAttr);
    when(mailboxAttr.setIfAbsent(any(StreamMailbox.class))).thenReturn(null);

    Attribute<ExecutorService> execAttr =
        mock(Attribute.class);
    when(mockParent.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR)).thenReturn(execAttr);

    final boolean[] executed = new boolean[1];
    ExecutorService directExecutor =
        new AbstractExecutorService() {
          private boolean shutdown = false;

          @Override
          public void shutdown() {
            shutdown = true;
          }

          @Override
          public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
          }

          @Override
          public boolean isShutdown() {
            return shutdown;
          }

          @Override
          public boolean isTerminated() {
            return shutdown;
          }

          @Override
          public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
          }

          @Override
          public void execute(Runnable command) {
            executed[0] = true;
            command.run(); // execute synchronously
          }
        };
    when(execAttr.get()).thenReturn(directExecutor);

    Attribute<WebTransportSessionManager> sessionMgrAttr =
        mock(Attribute.class);
    WebTransportSessionManager mockSessionMgr = mock(WebTransportSessionManager.class);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(sessionMgrAttr);
    when(sessionMgrAttr.get()).thenReturn(mockSessionMgr);

    Attribute<String> pathAttr = mock(Attribute.class);
    when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);
    when(pathAttr.get()).thenReturn("/test-path");

    ByteBuf data = Unpooled.copiedBuffer("App Message".getBytes(StandardCharsets.UTF_8));
    WebTransportStreamFrame frame = new WebTransportStreamFrame(101L, 202L, true, data);

    MessageDispatcher dispatcher = new DefaultMessageDispatcher();
    dispatcher.channelRead(mockCtx, frame);

    assertTrue(executed[0]);
    assertEquals(0, frame.refCnt()); // Verified: Memory safely recycled
  }

  @Test
  public void testMessageDispatcherCloseCapsuleSync() throws Exception {
    WebTransportCapsuleHandler handler = new WebTransportCapsuleHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);

    ByteBuf data = Unpooled.buffer(0);
    WebTransportCapsule closeCapsule = new WebTransportCapsule(101L, 0x2843L, data);

    handler.channelRead(mockCtx, closeCapsule);

    // Verified: Closing happens synchronously on the EventLoop without offloading
    verify(mockCtx).close();
    assertEquals(0, closeCapsule.refCnt());
  }

  @Test
  public void testWebTransportHeadersHandlerHandshake() throws Exception {
    WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(100L);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);

    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    QuicStreamChannelConfig mockConfig =
        mock(QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    when(mockConfig.isAutoRead()).thenReturn(true);

    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockCtx.pipeline()).thenReturn(mockPipeline);
    when(mockPipeline.names()).thenReturn(Collections.emptyList());

    // Attributes on Parent (QuicChannel)
    Attribute<List<String>> allowedOriginsAttr =
        mock(Attribute.class);
    when(allowedOriginsAttr.get()).thenReturn(null); // allow all
    when(mockParent.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS)).thenReturn(allowedOriginsAttr);

    Attribute<String> pathAttr = mock(Attribute.class);
    when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr =
        mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    Attribute<Long> defaultBidiAttr = mock(Attribute.class);
    when(defaultBidiAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(defaultBidiAttr);

    Attribute<Long> defaultUniAttr = mock(Attribute.class);
    when(defaultUniAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(defaultUniAttr);

    Attribute<Long> defaultDataAttr = mock(Attribute.class);
    when(defaultDataAttr.get()).thenReturn(10000L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(defaultDataAttr);

    // Mock EventLoop and Promise for createUniStream / createBiStream
    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockParent.eventLoop()).thenReturn(mockEventLoop);
    Promise mockPromise = mock(Promise.class);
    when(mockEventLoop.newPromise()).thenReturn(mockPromise);
    when(mockPromise.addListener(any())).thenReturn(mockPromise);

    // Mock createStream for createUniStream / createBiStream
    Future mockFuture = mock(Future.class);
    when(mockParent.createStream(any(), any())).thenReturn(mockFuture);
    when(mockFuture.addListener(any())).thenReturn(mockFuture);

    // Http3HeadersFrame
    Http3Headers mockHeaders =
        new DefaultHttp3Headers();
    mockHeaders.method("CONNECT");
    mockHeaders.scheme("https");
    mockHeaders.authority("localhost");
    mockHeaders.path("/webtransport-test");
    mockHeaders.set(":protocol", "webtransport-h3");
    Http3HeadersFrame mockHeadersFrame =
        mock(Http3HeadersFrame.class);
    when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

    handler.channelRead(mockCtx, mockHeadersFrame);

    // Verify registration
    assertTrue(mgr.hasSession(100L));

    // Verify decrement of BIDI streams
    assertEquals(0L, mgr.get(100L).getClientInitiatedStreamsBidi());

    // Verify response sent
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockCtx).writeAndFlush(captor.capture());
    Http3HeadersFrame respFrame =
        (Http3HeadersFrame) captor.getValue();
    assertEquals("200", respFrame.headers().status().toString());
  }

  @Test
  public void testHandshakeWithWebTransportSettingsOverrides() throws Exception {
    WebTransportHeadersHandler handler = new WebTransportHeadersHandler();

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStreamChannel = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStreamChannel);
    when(mockStreamChannel.parent()).thenReturn(mockParent);
    when(mockStreamChannel.streamId()).thenReturn(200L);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStreamChannel.closeFuture()).thenReturn(mockCloseFuture);

    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockStreamChannel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    when(mockCtx.pipeline()).thenReturn(mock(ChannelPipeline.class));
    when(mockStreamChannel.config())
        .thenReturn(mock(QuicStreamChannelConfig.class));

    // Attributes on Parent (QuicChannel)
    Attribute<List<String>> allowedOriginsAttr =
        mock(Attribute.class);
    when(allowedOriginsAttr.get()).thenReturn(null); // allow all
    when(mockParent.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS)).thenReturn(allowedOriginsAttr);

    Attribute<String> pathAttr = mock(Attribute.class);
    when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr =
        mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Connection-level limits: 50L bidi, 60L uni, 50000L data
    Attribute<Long> defaultBidiAttr = mock(Attribute.class);
    when(defaultBidiAttr.get()).thenReturn(50L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(defaultBidiAttr);

    Attribute<Long> defaultUniAttr = mock(Attribute.class);
    when(defaultUniAttr.get()).thenReturn(60L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(defaultUniAttr);

    Attribute<Long> defaultDataAttr = mock(Attribute.class);
    when(defaultDataAttr.get()).thenReturn(50000L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(defaultDataAttr);

    // Mock EventLoop and Promise for createUniStream / createBiStream
    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockParent.eventLoop()).thenReturn(mockEventLoop);
    Promise mockPromise = mock(Promise.class);
    when(mockEventLoop.newPromise()).thenReturn(mockPromise);
    when(mockPromise.addListener(any())).thenReturn(mockPromise);

    // Mock createStream
    Future mockFuture = mock(Future.class);
    when(mockParent.createStream(any(), any())).thenReturn(mockFuture);
    when(mockFuture.addListener(any())).thenReturn(mockFuture);

    // Http3HeadersFrame
    Http3Headers mockHeaders =
        new DefaultHttp3Headers();
    mockHeaders.method("CONNECT");
    mockHeaders.scheme("https");
    mockHeaders.authority("localhost");
    mockHeaders.path("/webtransport-test");
    mockHeaders.set(":protocol", "webtransport-h3");
    Http3HeadersFrame mockHeadersFrame =
        mock(Http3HeadersFrame.class);
    when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

    handler.channelRead(mockCtx, mockHeadersFrame);

    // Verify registration
    assertTrue(mgr.hasSession(200L));
    WebTransportSession session = mgr.get(200L);
    assertNotNull(session);

    // Verify that settings overrode the connection-level limits
    assertEquals(50L, session.getSettingsMaxStreamsBidi());
    assertEquals(60L, session.getSettingsMaxStreamsUni());
    assertEquals(50000L, session.getSettingsMaxData());
  }

  @Test
  public void testWebTransportHeadersHandlerInvalidSessionId() throws Exception {
    WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    // streamId 101 % 4 = 1 != 0 (invalid)
    when(mockStream.streamId()).thenReturn(101L);

    // Http3HeadersFrame
    Http3Headers mockHeaders =
        new DefaultHttp3Headers();
    mockHeaders.method("CONNECT");
    mockHeaders.scheme("https");
    mockHeaders.authority("localhost");
    mockHeaders.path("/webtransport-test");
    mockHeaders.set(":protocol", "webtransport-h3");
    Http3HeadersFrame mockHeadersFrame =
        mock(Http3HeadersFrame.class);
    when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

    handler.channelRead(mockCtx, mockHeadersFrame);

    // Verify connection close with H3_ID_ERROR (0x0108) was called on mockParent
    verify(mockParent).close(eq(true), eq(0x0108), any(ByteBuf.class));
  }

  @Test
  public void testServerInitiatedStreamLimits() throws Exception {
    QuicChannel mockParent = mock(QuicChannel.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.streamId()).thenReturn(100L);

    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockParent.eventLoop()).thenReturn(mockEventLoop);
    Promise mockPromise = mock(Promise.class);
    when(mockEventLoop.newPromise()).thenReturn(mockPromise);

    // Setup Session Manager
    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr =
        mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    // Set up limits on parent so register() can read them
    Attribute<Long> localLimitAttr = mock(Attribute.class);
    when(localLimitAttr.get()).thenReturn(5L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(localLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(localLimitAttr);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(localLimitAttr);

    when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
        .thenReturn(mock(Attribute.class));

    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(100L);
    assertNotNull(session);

    // peerSettingsMaxStreamsUni = 2, peerSettingsMaxStreamsBidi = 1
    session.setPeerSettingsMaxStreamsUni(2L);
    session.setPeerSettingsMaxStreamsBidi(1L);

    // Mock createStream to return successful future
    QuicStreamChannel mockNewStream = mock(QuicStreamChannel.class);
    when(mockNewStream.attr(any(AttributeKey.class)))
        .thenReturn(mock(Attribute.class));
    when(mockNewStream.closeFuture()).thenReturn(mock(ChannelFuture.class));
      when(mockConnectStream.alloc()).thenReturn(PooledByteBufAllocator.DEFAULT);
      when(mockNewStream.alloc()).thenReturn(PooledByteBufAllocator.DEFAULT);
    Future<QuicStreamChannel> successFuture =
        mock(Future.class);
    when(successFuture.isSuccess()).thenReturn(true);
    when(successFuture.getNow()).thenReturn(mockNewStream);
    when(mockParent.createStream(any(), any())).thenReturn(successFuture);
    when(successFuture.addListener(any()))
        .thenAnswer(
            inv -> {
              GenericFutureListener listener = inv.getArgument(0);
              listener.operationComplete(successFuture);
              return successFuture;
            });

    // 1. Create Uni Stream - First creation should succeed
    WebTransportUtils.createUniStream(
        mockConnectStream, false, mock(ChannelHandler.class));

    // Create Uni Stream - Second creation should succeed
    WebTransportUtils.createUniStream(
        mockConnectStream, false, mock(ChannelHandler.class));

    // Create Uni Stream - Third creation should fail (exceeds limit 2)
    WebTransportUtils.createUniStream(
        mockConnectStream, false, mock(ChannelHandler.class));
    verify(mockPromise).setFailure(any(IllegalStateException.class));

    // 2. Create Bi Stream - First creation should succeed
    WebTransportUtils.createBiStream(
        mockConnectStream, false, mock(ChannelHandler.class));

    // Create Bi Stream - Second creation should fail (exceeds limit 1)
    WebTransportUtils.createBiStream(
        mockConnectStream, false, mock(ChannelHandler.class));
    verify(mockPromise, times(2)).setFailure(any(IllegalStateException.class));
  }

  @Test
  public void testAllowedOriginsAndAuthorityMatching() throws Exception {
    WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.streamId()).thenReturn(100L);
    ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
    when(mockStream.closeFuture()).thenReturn(mockCloseFuture);
    Attribute<Long> sessIdAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    QuicStreamChannelConfig mockConfig =
        mock(QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    when(mockConfig.isAutoRead()).thenReturn(true);

    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockCtx.pipeline()).thenReturn(mockPipeline);
    when(mockPipeline.names()).thenReturn(Collections.emptyList());

    // Configure allowed origins: [google.com, localhost]
    Attribute<List<String>> allowedOriginsAttr =
        mock(Attribute.class);
    when(allowedOriginsAttr.get()).thenReturn(Arrays.asList("google.com", "localhost"));
    when(mockParent.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS)).thenReturn(allowedOriginsAttr);

    Attribute<String> pathAttr = mock(Attribute.class);
    when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr =
        mock(Attribute.class);
    when(mgrAttr.get()).thenReturn(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    Attribute<Long> defaultBidiAttr = mock(Attribute.class);
    when(defaultBidiAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
        .thenReturn(defaultBidiAttr);

    Attribute<Long> defaultUniAttr = mock(Attribute.class);
    when(defaultUniAttr.get()).thenReturn(10L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
        .thenReturn(defaultUniAttr);

    Attribute<Long> defaultDataAttr = mock(Attribute.class);
    when(defaultDataAttr.get()).thenReturn(10000L);
    when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
        .thenReturn(defaultDataAttr);

    // Mock EventLoop and Promise for stream creation
    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockParent.eventLoop()).thenReturn(mockEventLoop);
    Promise mockPromise = mock(Promise.class);
    when(mockEventLoop.newPromise()).thenReturn(mockPromise);
    when(mockPromise.addListener(any())).thenReturn(mockPromise);
    Future mockFuture = mock(Future.class);
    when(mockParent.createStream(any(), any())).thenReturn(mockFuture);
    when(mockFuture.addListener(any())).thenReturn(mockFuture);

    // Case 1: Origin "https://localhost:4433" -> Should be ALLOWED
    {
      Http3Headers mockHeaders =
          new DefaultHttp3Headers();
      mockHeaders.method("CONNECT");
      mockHeaders.scheme("https");
      mockHeaders.authority("localhost:4433");
      mockHeaders.path("/webtransport-test");
      mockHeaders.set(":protocol", "webtransport-h3");
      mockHeaders.set("origin", "https://localhost:4433");
      Http3HeadersFrame mockHeadersFrame =
          mock(Http3HeadersFrame.class);
      when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

      handler.channelRead(mockCtx, mockHeadersFrame);

      ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
      verify(mockCtx, atLeastOnce()).writeAndFlush(captor.capture());
      Http3HeadersFrame respFrame =
          (Http3HeadersFrame)
              captor.getAllValues().get(captor.getAllValues().size() - 1);
      assertEquals("200", respFrame.headers().status().toString());
      mgr.closeAll();
    }

    // Case 2: Origin "https://evil.com" -> Should be FORBIDDEN
    {
      Http3Headers mockHeaders =
          new DefaultHttp3Headers();
      mockHeaders.method("CONNECT");
      mockHeaders.scheme("https");
      mockHeaders.authority("localhost:4433");
      mockHeaders.path("/webtransport-test");
      mockHeaders.set(":protocol", "webtransport-h3");
      mockHeaders.set("origin", "https://evil.com");
      Http3HeadersFrame mockHeadersFrame =
          mock(Http3HeadersFrame.class);
      when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

      handler.channelRead(mockCtx, mockHeadersFrame);

      ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
      verify(mockCtx, atLeastOnce()).writeAndFlush(captor.capture());
      Http3HeadersFrame respFrame =
          (Http3HeadersFrame)
              captor.getAllValues().get(captor.getAllValues().size() - 1);
      assertEquals("403", respFrame.headers().status().toString());
    }

    // Case 3: Origin absent, Authority "localhost:4433" (e.g. backend client) -> Should be ALLOWED
    {
      Http3Headers mockHeaders =
          new DefaultHttp3Headers();
      mockHeaders.method("CONNECT");
      mockHeaders.scheme("https");
      mockHeaders.authority("localhost:4433");
      mockHeaders.path("/webtransport-test");
      mockHeaders.set(":protocol", "webtransport-h3");
      Http3HeadersFrame mockHeadersFrame =
          mock(Http3HeadersFrame.class);
      when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

      handler.channelRead(mockCtx, mockHeadersFrame);

      ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
      verify(mockCtx, atLeastOnce()).writeAndFlush(captor.capture());
      Http3HeadersFrame respFrame =
          (Http3HeadersFrame)
              captor.getAllValues().get(captor.getAllValues().size() - 1);
      assertEquals("200", respFrame.headers().status().toString());
      mgr.closeAll();
    }
  }

  @Test
  public void testSimultaneousSessionLimitRejection() throws Exception {
    // Set maximum sessions limit to 1
    System.setProperty("webtransport4j.webtransport.max_sessions_per_connection", "1");
    try {
      WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
      ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
      QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
      QuicChannel mockParent = mock(QuicChannel.class);

      when(mockCtx.channel()).thenReturn(mockStream);
      when(mockStream.parent()).thenReturn(mockParent);
      when(mockStream.streamId()).thenReturn(100L);
      ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
      when(mockStream.closeFuture()).thenReturn(mockCloseFuture);
      Attribute<Long> sessIdAttr = mock(Attribute.class);
      when(mockStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

      QuicStreamChannelConfig mockConfig =
          mock(QuicStreamChannelConfig.class);
      when(mockStream.config()).thenReturn(mockConfig);
      when(mockConfig.isAutoRead()).thenReturn(true);

      ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
      when(mockCtx.pipeline()).thenReturn(mockPipeline);
      when(mockPipeline.names()).thenReturn(Collections.emptyList());

      // Mock ALLOWED_ORIGINS to allow everything
      Attribute<List<String>> allowedOriginsAttr =
          mock(Attribute.class);
      when(allowedOriginsAttr.get()).thenReturn(null);
      when(mockParent.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS))
          .thenReturn(allowedOriginsAttr);

      Attribute<String> pathAttr = mock(Attribute.class);
      when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

      // Setup session manager with 1 active session registered
      QuicStreamChannel existingStream = mock(QuicStreamChannel.class);
      when(existingStream.streamId()).thenReturn(40L);
      when(existingStream.parent()).thenReturn(mockParent);
      when(existingStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY))
          .thenReturn(mock(Attribute.class));
      WebTransportSessionManager mgr = new WebTransportSessionManager();
      mgr.register(existingStream);

      Attribute<WebTransportSessionManager> mgrAttr =
          mock(Attribute.class);
      when(mgrAttr.get()).thenReturn(mgr);
      when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

      Attribute<Long> defaultBidiAttr = mock(Attribute.class);
      when(defaultBidiAttr.get()).thenReturn(10L);
      when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI))
          .thenReturn(defaultBidiAttr);

      Attribute<Long> defaultUniAttr = mock(Attribute.class);
      when(defaultUniAttr.get()).thenReturn(10L);
      when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI))
          .thenReturn(defaultUniAttr);

      Attribute<Long> defaultDataAttr = mock(Attribute.class);
      when(defaultDataAttr.get()).thenReturn(10000L);
      when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA))
          .thenReturn(defaultDataAttr);

      // Setup new CONNECT request
      Http3Headers mockHeaders =
          new DefaultHttp3Headers();
      mockHeaders.method("CONNECT");
      mockHeaders.scheme("https");
      mockHeaders.authority("localhost:4433");
      mockHeaders.path("/webtransport-test");
      mockHeaders.set(":protocol", "webtransport-h3");
      Http3HeadersFrame mockHeadersFrame =
          mock(Http3HeadersFrame.class);
      when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

      // Execute channelRead — this should trigger simultaneous session limit (since 1 session is
      // already registered)
      handler.channelRead(mockCtx, mockHeadersFrame);

      // Verify status 429 Too Many Requests response is sent
      ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
      verify(mockCtx).writeAndFlush(captor.capture());
      Http3HeadersFrame respFrame =
          (Http3HeadersFrame) captor.getValue();
      assertEquals("429", respFrame.headers().status().toString());
    } finally {
      System.clearProperty("webtransport4j.webtransport.max_sessions_per_connection");
    }
  }

  @Test
  public void testApplicationErrorCodeMapping() {
    // Spec requirements check:
    // 0x00000000 corresponds to 0x52e4a40fa8db
    assertEquals(0x52e4a40fa8dbL, WebTransportUtils.webTransportCodeToHttpCode(0x00000000L));
    assertEquals(0x00000000L, WebTransportUtils.httpCodeToWebTransportCode(0x52e4a40fa8dbL));

    // 0xffffffff corresponds to 0x52e5ac983162
    assertEquals(0x52e5ac983162L, WebTransportUtils.webTransportCodeToHttpCode(0xffffffffL));
    assertEquals(0xffffffffL, WebTransportUtils.httpCodeToWebTransportCode(0x52e5ac983162L));

    // Round-trip verification for key and random values
    long[] testValues = {0L, 1L, 29L, 30L, 31L, 100L, 123456L, 0xffffffffL};
    for (long n : testValues) {
      long http = WebTransportUtils.webTransportCodeToHttpCode(n);
      assertTrue(WebTransportUtils.isWebTransportApplicationError(http));
      assertEquals(n, WebTransportUtils.httpCodeToWebTransportCode(http));
    }

    // Verify invalid inputs throw IllegalArgumentException
    try {
      WebTransportUtils.webTransportCodeToHttpCode(-1L);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }

    try {
      WebTransportUtils.webTransportCodeToHttpCode(0x100000000L);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }

    // Verify reserved codepoint throws IllegalArgumentException
    long actualReservedH = WebTransportUtils.WT_ERROR_FIRST + 30L;
    assertTrue((actualReservedH - 0x21L) % 31L == 0L);
    try {
      WebTransportUtils.httpCodeToWebTransportCode(actualReservedH);
      fail("Expected IllegalArgumentException for reserved codepoint");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testResetStreamApplicationErrorCodeMapping() throws Exception {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    ChannelPromise mockPromise = mock(ChannelPromise.class);
    when(mockStream.newPromise()).thenReturn(mockPromise);

    // Application error code = 500
    WebTransportUtils.resetStream(mockStream, 500L);

    // Under Netty QUIC limitation, the mapped httpErrorCode would be negative
    // (WT_ERROR_FIRST + 516 = 0x52e4a40faddfL, casted to int is -1542541857).
    // Since it is negative, it falls back to the unmapped appErrorCode (500) to prevent native JVM
    // crash.
    int expectedHttpCode = 500;

    // Verify Netty's shutdown was called with the fallback code
    verify(mockStream).shutdown(eq(expectedHttpCode), eq(mockPromise));
  }

  @Test
  public void testReflectionQuicStreamChannel() {
    log.info("=== REFLECTION: io.netty.handler.codec.quic.QuicStreamChannel ===");
    try {
      Class<?> clazz = Class.forName("io.netty.handler.codec.quic.QuicStreamChannel");
      for (Method method : clazz.getMethods()) {
        log.info(
            "Method: "
                + method.getReturnType().getSimpleName()
                + " "
                + method.getName()
                + " ("
                + Arrays.toString(method.getParameterTypes())
                + ")");
      }
    } catch (Exception e) {
      log.error("Exception caught", e);
    }
    log.info("==============================================================");
  }

  @Test
  public void testMessageDispatcherEventLoopMode() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    when(mockParent.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR)).thenReturn(mock(Attribute.class));

    Attribute<WebTransportSessionManager> sessionMgrAttr = mock(Attribute.class);
    WebTransportSessionManager mockSessionMgr = mock(WebTransportSessionManager.class);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(sessionMgrAttr);
    when(sessionMgrAttr.get()).thenReturn(mockSessionMgr);
    when(mockSessionMgr.get(any(Long.class))).thenReturn(null);

    ByteBuf data = Unpooled.copiedBuffer("EventLoop Msg".getBytes(StandardCharsets.UTF_8));
    WebTransportStreamFrame frame = new WebTransportStreamFrame(101L, 202L, true, data);

    MessageDispatcher dispatcher = new DefaultMessageDispatcher();
    dispatcher.channelRead(mockCtx, frame);

    assertEquals(0, frame.refCnt());
  }

  @Test
  public void testMessageDispatcherVirtualThreadsMode() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockStream.eventLoop()).thenReturn(mockEventLoop);
    doAnswer(
            invocation -> {
              Runnable r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(mockEventLoop)
        .execute(any(Runnable.class));

    QuicStreamChannelConfig mockConfig =
        mock(QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    when(mockConfig.isAutoRead()).thenReturn(true);

    Attribute<StreamMailbox> mailboxAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_MAILBOX_KEY)).thenReturn(mailboxAttr);
    when(mailboxAttr.setIfAbsent(any(StreamMailbox.class))).thenReturn(null);

    final boolean[] executed = new boolean[1];
    ExecutorService mockExecutor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              executed[0] = true;
              Runnable r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(mockExecutor)
        .execute(any(Runnable.class));

    Attribute<ExecutorService> execAttr =
        mock(Attribute.class);
    when(mockParent.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR)).thenReturn(execAttr);
    when(execAttr.get()).thenReturn(mockExecutor);

    Attribute<WebTransportSessionManager> sessionMgrAttr = mock(Attribute.class);
    WebTransportSessionManager mockSessionMgr = mock(WebTransportSessionManager.class);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(sessionMgrAttr);
    when(sessionMgrAttr.get()).thenReturn(mockSessionMgr);

    ByteBuf data = Unpooled.copiedBuffer("VT Msg".getBytes(StandardCharsets.UTF_8));
    WebTransportStreamFrame frame = new WebTransportStreamFrame(101L, 202L, true, data);

    MessageDispatcher dispatcher = new DefaultMessageDispatcher();
    dispatcher.channelRead(mockCtx, frame);

    assertTrue(executed[0]);
    assertEquals(0, frame.refCnt());
  }

  @Test
  public void testMessageDispatcherFixedThreadPoolMode() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockStream.eventLoop()).thenReturn(mockEventLoop);
    doAnswer(
            invocation -> {
              Runnable r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(mockEventLoop)
        .execute(any(Runnable.class));

    QuicStreamChannelConfig mockConfig =
        mock(QuicStreamChannelConfig.class);
    when(mockStream.config()).thenReturn(mockConfig);
    when(mockConfig.isAutoRead()).thenReturn(true);

    Attribute<StreamMailbox> mailboxAttr = mock(Attribute.class);
    when(mockStream.attr(WebTransportAttributeKeys.STREAM_MAILBOX_KEY)).thenReturn(mailboxAttr);
    when(mailboxAttr.setIfAbsent(any(StreamMailbox.class))).thenReturn(null);

    final boolean[] executed = new boolean[1];
    ThreadPoolExecutor mockFixedPool = mock(ThreadPoolExecutor.class);
    doAnswer(
            invocation -> {
              executed[0] = true;
              Runnable r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(mockFixedPool)
        .execute(any(Runnable.class));

    Attribute<ExecutorService> execAttr =
        mock(Attribute.class);
    when(mockParent.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR)).thenReturn(execAttr);
    when(execAttr.get()).thenReturn(mockFixedPool);

    Attribute<WebTransportSessionManager> sessionMgrAttr = mock(Attribute.class);
    WebTransportSessionManager mockSessionMgr = mock(WebTransportSessionManager.class);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(sessionMgrAttr);
    when(sessionMgrAttr.get()).thenReturn(mockSessionMgr);

    ByteBuf data = Unpooled.copiedBuffer("Fixed Msg".getBytes(StandardCharsets.UTF_8));
    WebTransportStreamFrame frame = new WebTransportStreamFrame(101L, 202L, true, data);

    MessageDispatcher dispatcher = new DefaultMessageDispatcher();
    dispatcher.channelRead(mockCtx, frame);

    assertTrue(executed[0]);
    assertEquals(0, frame.refCnt());
  }

  @Test
  public void testStreamMailboxBackpressureThrottling() throws Exception {
    System.setProperty("webtransport4j.mailbox.high_water_mark", "16");
    System.setProperty("webtransport4j.mailbox.low_water_mark", "4");
    WebTransportConfig.reload();
    QuicStreamChannelConfig mockConfig = mock(QuicStreamChannelConfig.class);
    EventLoop mockEventLoop = mock(EventLoop.class);
    when(mockEventLoop.inEventLoop()).thenReturn(true);
    when(mockConfig.isAutoRead()).thenReturn(true);

    QuicStreamChannel mockStream = mock(QuicStreamChannel.class, invocation -> {
      String name = invocation.getMethod().getName();
      if ("config".equals(name)) {
        return mockConfig;
      }
      if ("eventLoop".equals(name)) {
        return mockEventLoop;
      }
      return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
    });

    final List<Runnable> tasks = new ArrayList<>();
    ExecutorService mockExecutor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              tasks.add(invocation.getArgument(0));
              return null;
            })
        .when(mockExecutor)
        .execute(any(Runnable.class));

    StreamMailbox.FrameDispatcher mockDispatcher = mock(StreamMailbox.FrameDispatcher.class);

    StreamMailbox mailbox = new StreamMailbox(mockStream, mockExecutor, mockDispatcher, 1L);

    List<WebTransportStreamFrame> frames = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      ByteBuf data = Unpooled.buffer(0);
      WebTransportStreamFrame f = new WebTransportStreamFrame(1L, 1L, true, data);
      frames.add(f);
      mailbox.enqueue(f);
    }
    verify((io.netty.channel.ChannelConfig) mockConfig, times(0)).setAutoRead(false);

    ByteBuf data = Unpooled.buffer(0);
    WebTransportStreamFrame seventeenthFrame = new WebTransportStreamFrame(1L, 1L, true, data);
    frames.add(seventeenthFrame);
    mailbox.enqueue(seventeenthFrame);
    verify((io.netty.channel.ChannelConfig) mockConfig, times(1)).setAutoRead(false);

    assertEquals(1, tasks.size());
    Runnable mailboxTask = tasks.get(0);
    mailboxTask.run();

    verify((io.netty.channel.ChannelConfig) mockConfig, times(1)).setAutoRead(true);

    // Simulate Netty auto-release that occurs when channelRead0 finishes
    for (WebTransportStreamFrame f : frames) {
      f.release();
    }

    for (WebTransportStreamFrame f : frames) {
      assertEquals(0, f.refCnt());
    }
  }
}
