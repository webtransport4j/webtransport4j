package io.github.webtransport4j.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.BinarySources;
import io.github.webtransport4j.api.SessionFlowControlPolicy;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link SessionFlowControlPolicy} and outbound writability pause/resume mechanics. */
public class SessionFlowControlPolicyAndWritabilityTest {

  @SuppressWarnings("unchecked")
  private <T> Attribute<T> mockAttribute(T value) {
    Attribute<T> attr = mock(Attribute.class);
    when(attr.get()).thenReturn(value);
    return attr;
  }

  @Test
  public void testDefaultPolicyDataAndStreamDecisions() {
    DefaultPropertiesFlowControlPolicy policy = DefaultPropertiesFlowControlPolicy.INSTANCE;

    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            1L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 1000L, true, true);

    // Initial unconsumed credit = 1000 - 0 = 1000 < default extend (2MB = 2097152).
    // Blocked at 1000 >= currentSettingsMaxData (1000).
    long grant = policy.onRequestDataCredit(session, 1000L, 0L);
    assertEquals(2097152L, grant);

    // If unconsumed credit >= extendAmount, policy denies extension (returns 0)
    session.setSettingsMaxData(5000000L);
    long denied = policy.onRequestDataCredit(session, 1000L, 0L);
    assertEquals(0L, denied);

    // Stream credit test: 0 active streams, initial = 10 -> remaining = 10 capped by max.active (5)
    long streamLimit = policy.onRequestStreamCredit(session, true, 10L, 0L);
    assertEquals(15L, streamLimit);
  }

  @Test
  public void testCustomPolicyGrantsAndDeniesDataCreditOnCapsule() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.isOpen()).thenReturn(true);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnect.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            100L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 1000L, true, true);
    mgr.putSession(100L, session);

    // 1. Custom policy that grants 5MB
    session.setFlowControlPolicy(
        new SessionFlowControlPolicy() {
          @Override
          public long onRequestDataCredit(
              @NonNull WebTransportSession s, long blockedAtLimit, long cumulativeReceivedBytes) {
            return 5 * 1024 * 1024L;
          }
        });

    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 1000L);
    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x190B4D41L, payload);

    WebTransportCapsuleHandler handler = new WebTransportCapsuleHandler();
    handler.channelRead(mockCtx, capsule);

    assertEquals(1000L + 5 * 1024 * 1024L, session.getSettingsMaxData());
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockConnect).writeAndFlush(captor.capture());
    ReferenceCountUtil.release(captor.getValue());

    // 2. Custom policy that denies extension (returns 0)
    QuicStreamChannel mockConnect2 = mock(QuicStreamChannel.class);
    when(mockConnect2.isOpen()).thenReturn(true);
    when(mockConnect2.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    DefaultWebTransportSession sessionDeny =
        new DefaultWebTransportSession(
            200L, mockConnect2, "/test", 10L, 10L, 1000L, 10L, 10L, 1000L, true, true);
    sessionDeny.setFlowControlPolicy(SessionFlowControlPolicy.denyAll());
    mgr.putSession(200L, sessionDeny);

    ByteBuf payloadDeny = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payloadDeny, 1000L);
    WebTransportCapsule capsuleDeny = new WebTransportCapsule(200L, 0x190B4D41L, payloadDeny);

    handler.channelRead(mockCtx, capsuleDeny);
    assertEquals(1000L, sessionDeny.getSettingsMaxData());
    verify(mockConnect2, never()).writeAndFlush(any());
  }

  @Test
  public void testCustomPolicyGrantsAndDeniesStreamsCreditOnCapsule() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.isOpen()).thenReturn(true);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnect.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            300L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 1000L, true, true);
    mgr.putSession(300L, session);

    // Custom policy extending streams limit to 75
    session.setFlowControlPolicy(
        new SessionFlowControlPolicy() {
          @Override
          public long onRequestDataCredit(
              @NonNull WebTransportSession s, long blockedAtLimit, long cumulativeReceivedBytes) {
            return 0L;
          }

          @Override
          public long onRequestStreamCredit(
              @NonNull WebTransportSession s,
              boolean bidirectional,
              long blockedAtLimit,
              long currentActiveCount) {
            return 75L;
          }
        });

    ByteBuf payload = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payload, 10L);
    WebTransportCapsule capsule = new WebTransportCapsule(300L, 0x190B4D43L, payload);

    WebTransportCapsuleHandler handler = new WebTransportCapsuleHandler();
    handler.channelRead(mockCtx, capsule);

    assertEquals(75L, session.getSettingsMaxStreamsBidi());
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockConnect).writeAndFlush(captor.capture());
    ReferenceCountUtil.release(captor.getValue());

    // Policy denying stream extension
    QuicStreamChannel mockConnect4 = mock(QuicStreamChannel.class);
    DefaultWebTransportSession sessionDeny =
        new DefaultWebTransportSession(
            400L, mockConnect4, "/test", 10L, 10L, 1000L, 10L, 10L, 1000L, true, true);
    sessionDeny.setFlowControlPolicy(SessionFlowControlPolicy.denyAll());
    mgr.putSession(400L, sessionDeny);

    ByteBuf payloadDeny = Unpooled.buffer();
    WebTransportUtils.writeVarInt(payloadDeny, 10L);
    WebTransportCapsule capsuleDeny = new WebTransportCapsule(400L, 0x190B4D43L, payloadDeny);

    handler.channelRead(mockCtx, capsuleDeny);
    assertEquals(10L, sessionDeny.getSettingsMaxStreamsBidi());
    verify(mockConnect4, never()).writeAndFlush(any());
  }

  @Test
  public void testWritabilityPauseAndResumeLifecycle() {
    QuicStreamChannel mockChannel = mock(QuicStreamChannel.class);
    when(mockChannel.streamId()).thenReturn(4L);
    when(mockChannel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    when(mockChannel.isActive()).thenReturn(true);
    when(mockChannel.isWritable()).thenReturn(true);

    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.isOpen()).thenReturn(true);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            500L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 100L, true, true);

    DefaultNettyWebTransportStream stream =
        new DefaultNettyWebTransportStream(mockChannel, 500L, session);
    Attribute<WebTransportStream> streamAttr = mockAttribute(stream);
    when(mockChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY)).thenReturn(streamAttr);
    session.getActiveClientInitiatedBi().add(mockChannel);

    // 1. Initial state: peer limit = 100, cumulative sent = 0 -> writable
    assertTrue(session.isOutboundFlowControlWritable());
    assertTrue(stream.isWritable());

    AtomicReference<Boolean> notifiedState = new AtomicReference<>(null);
    stream.onWritabilityChanged(notifiedState::set);

    // 2. Exhaust credit
    assertTrue(session.tryReserveOutboundBytes(100L, 100L));
    assertEquals(100L, session.getCumulativeBytesSent());
    assertFalse(session.isOutboundFlowControlWritable());

    // Pause notification
    session.notifyOutboundFlowControlPaused();
    assertFalse(stream.isWritable());
    assertEquals(Boolean.FALSE, notifiedState.get());

    // waitForWritable should queue and return incomplete future
    CompletableFuture<Void> waitFuture = stream.waitForWritable();
    assertFalse(waitFuture.isDone());

    // 3. Replenish credit via setPeerSettingsMaxData
    session.setPeerSettingsMaxData(250L);
    assertTrue(session.isOutboundFlowControlWritable());
    assertTrue(stream.isWritable());
    assertTrue(waitFuture.isDone());
    assertFalse(waitFuture.isCompletedExceptionally());
    assertEquals(Boolean.TRUE, notifiedState.get());
  }

  @Test
  public void testStreamWritabilityRespectsBothChannelAndSessionCredit() {
    QuicStreamChannel mockChannel = mock(QuicStreamChannel.class);
    when(mockChannel.streamId()).thenReturn(8L);
    when(mockChannel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    when(mockChannel.isActive()).thenReturn(true);

    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            600L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 500L, true, true);

    DefaultNettyWebTransportStream stream =
        new DefaultNettyWebTransportStream(mockChannel, 600L, session);

    // Case A: channel not writable, session writable -> stream not writable
    when(mockChannel.isWritable()).thenReturn(false);
    assertFalse(stream.isWritable());

    // Case B: channel writable, session writable -> stream writable
    when(mockChannel.isWritable()).thenReturn(true);
    assertTrue(stream.isWritable());

    // Case C: channel writable, session exhausted -> stream not writable
    session.tryReserveOutboundBytes(500L, 500L);
    assertFalse(session.isOutboundFlowControlWritable());
    assertFalse(stream.isWritable());
  }

  @Test
  public void testRawWebTransportHandlerTriggersWritabilityPauseOnExhaustion() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockChannel = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockCtx.channel()).thenReturn(mockChannel);
    when(mockChannel.parent()).thenReturn(mockQuic);
    when(mockChannel.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockQuic.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.isOpen()).thenReturn(true);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnect.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            700L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 50L, true, true);
    mgr.putSession(700L, session);

    DefaultNettyWebTransportStream stream =
        new DefaultNettyWebTransportStream(mockChannel, 700L, session);
    Attribute<WebTransportStream> streamAttr = mockAttribute(stream);
    when(mockChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY)).thenReturn(streamAttr);

    Attribute<Long> sessIdAttr = mockAttribute(700L);
    when(mockChannel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    when(mockChannel.isWritable()).thenReturn(true);
    session.getActiveClientInitiatedBi().add(mockChannel);

    RawWebTransportHandler handler = new RawWebTransportHandler();

    // 1. Write that consumes all remaining credit (50 bytes)
    ByteBuf data1 = Unpooled.buffer(50);
    data1.writeZero(50);
    ChannelPromise promise1 = mock(ChannelPromise.class);

    handler.write(mockCtx, data1, promise1);

    assertFalse(session.isOutboundFlowControlWritable());
    assertFalse(stream.isWritable());

    // 2. Attempt next write exceeding limit -> queued in session instead of dropped
    ByteBuf data2 = Unpooled.buffer(10);
    data2.writeZero(10);
    ChannelPromise promise2 = mock(ChannelPromise.class);

    handler.write(mockCtx, data2, promise2);

    assertTrue(session.hasPendingOutboundWrites());
    assertEquals(10L, session.getPendingOutboundBytes());
    assertFalse(stream.isWritable());

    // 3. Replenishing credits via WT_MAX_DATA drains queued write and restores writability
    session.setPeerSettingsMaxData(100L);
    assertFalse(session.hasPendingOutboundWrites());
    assertEquals(0L, session.getPendingOutboundBytes());
    assertTrue(session.isOutboundFlowControlWritable());
    assertTrue(stream.isWritable());
    verify(mockCtx).write(data2, promise2);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockConnect).writeAndFlush(captor.capture());
    ReferenceCountUtil.release(captor.getValue());
    data1.release();
    data2.release();
  }

  @Test
  public void testRawWebTransportHandlerFailsImmediatelyWhenQueueDisabled() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockChannel = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockCtx.channel()).thenReturn(mockChannel);
    when(mockChannel.parent()).thenReturn(mockQuic);
    when(mockChannel.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    Attribute<WebTransportSessionManager> mgrAttr = mockAttribute(mgr);
    when(mockQuic.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.isOpen()).thenReturn(true);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnect.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            701L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 50L, true, true);
    session.setFlowControlPolicy(new SessionFlowControlPolicy() {
      @Override
      public long onRequestDataCredit(
          WebTransportSession session, long blockedAtLimit, long cumulativeReceivedBytes) {
        return 0L;
      }

      @Override
      public long getMaxPendingOutboundBytes() {
        return 0L;
      }
    });
    mgr.putSession(701L, session);

    DefaultNettyWebTransportStream stream =
        new DefaultNettyWebTransportStream(mockChannel, 701L, session);
    Attribute<WebTransportStream> streamAttr = mockAttribute(stream);
    when(mockChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY)).thenReturn(streamAttr);

    Attribute<Long> sessIdAttr = mockAttribute(701L);
    when(mockChannel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

    when(mockChannel.isWritable()).thenReturn(true);
    session.getActiveClientInitiatedBi().add(mockChannel);

    RawWebTransportHandler handler = new RawWebTransportHandler();

    ByteBuf data1 = Unpooled.buffer(50);
    data1.writeZero(50);
    ChannelPromise promise1 = mock(ChannelPromise.class);

    handler.write(mockCtx, data1, promise1);

    ByteBuf data2 = Unpooled.buffer(10);
    data2.writeZero(10);
    ChannelPromise promise2 = mock(ChannelPromise.class);

    handler.write(mockCtx, data2, promise2);

    verify(promise2).setFailure(any(java.io.IOException.class));
    assertEquals(0, data2.refCnt());

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(mockConnect).writeAndFlush(captor.capture());
    ReferenceCountUtil.release(captor.getValue());
    data1.release();
  }

  @Test
  public void testBinarySourceChunkedInputRespectsStreamWritabilityDirectly() throws Exception {
    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnect.isOpen()).thenReturn(true);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            800L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 100L, true, true);

    QuicStreamChannel mockStreamChannel = mock(QuicStreamChannel.class);
    when(mockStreamChannel.isWritable()).thenReturn(true);

    DefaultNettyWebTransportStream stream =
        new DefaultNettyWebTransportStream(mockStreamChannel, 800L, session);

    byte[] payload = new byte[150];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) i;
    }
    BinarySourceChunkedInput input =
        new BinarySourceChunkedInput(BinarySources.fromByteArray(payload), 50, stream);

    // 1. First chunk (50 bytes) read while writable
    ByteBuf chunk1 = input.readChunk(UnpooledByteBufAllocator.DEFAULT);
    assertNotNull(chunk1);
    assertEquals(50, chunk1.readableBytes());
    session.incrementCumulativeBytesSent(50);
    chunk1.release();

    // 2. Second chunk (50 bytes) reaches limit 100
    ByteBuf chunk2 = input.readChunk(UnpooledByteBufAllocator.DEFAULT);
    assertNotNull(chunk2);
    assertEquals(50, chunk2.readableBytes());
    session.incrementCumulativeBytesSent(50);
    chunk2.release();

    assertFalse(session.isOutboundFlowControlWritable());
    assertFalse(stream.isWritable());

    // 3. Third chunk: stream is not writable -> chunk extraction is gated (returns null)
    ByteBuf chunk3 = input.readChunk(UnpooledByteBufAllocator.DEFAULT);
    assertNull(chunk3);
    assertFalse(input.isEndOfInput());

    // 4. Replenishing credits restores writability -> chunk extraction resumes
    session.setPeerSettingsMaxData(200L);
    assertTrue(stream.isWritable());

    chunk3 = input.readChunk(UnpooledByteBufAllocator.DEFAULT);
    assertNotNull(chunk3);
    assertEquals(50, chunk3.readableBytes());
    chunk3.release();

    input.close();
  }

  @Test
  public void testWebTransportChunkedWriteHandlerWrapsAndGatesChunkedInput() throws Exception {
    QuicStreamChannel mockConnect = mock(QuicStreamChannel.class);
    when(mockConnect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(mockConnect.isOpen()).thenReturn(true);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            801L, mockConnect, "/test", 10L, 10L, 1000L, 10L, 10L, 50L, true, true);

    QuicStreamChannel mockStreamChannel = mock(QuicStreamChannel.class);
    when(mockStreamChannel.isWritable()).thenReturn(true);

    DefaultNettyWebTransportStream stream =
        new DefaultNettyWebTransportStream(mockStreamChannel, 801L, session);

    Attribute<WebTransportStream> streamAttr = mockAttribute(stream);
    when(mockStreamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY)).thenReturn(streamAttr);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.channel()).thenReturn(mockStreamChannel);
    when(mockCtx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    WebTransportChunkedWriteHandler chunkedHandler = new WebTransportChunkedWriteHandler();

    byte[] data = new byte[100];
    BinarySourceChunkedInput rawInput =
        new BinarySourceChunkedInput(BinarySources.fromByteArray(data), 50);

    ChannelPromise promise = mock(ChannelPromise.class);
    chunkedHandler.write(mockCtx, rawInput, promise);

    // Consume the 50 bytes peer credit
    session.incrementCumulativeBytesSent(50);
    assertFalse(stream.isWritable());

    // Calling channelWritabilityChanged passes stream.isWritable()
    chunkedHandler.channelWritabilityChanged(mockCtx);
    assertFalse(stream.isWritable());

    rawInput.close();
  }

  @Test
  public void testNotifyWritabilityChangedTriggersResumeTransferOnChunkedHandler() {
    QuicStreamChannel mockStreamChannel = mock(QuicStreamChannel.class);
    when(mockStreamChannel.isWritable()).thenReturn(true);

    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    when(mockStreamChannel.pipeline()).thenReturn(mockPipeline);

    WebTransportChunkedWriteHandler mockChunkedHandler = mock(WebTransportChunkedWriteHandler.class);
    when(mockPipeline.get(WebTransportChunkedWriteHandler.class)).thenReturn(mockChunkedHandler);

    DefaultNettyWebTransportStream stream =
        new DefaultNettyWebTransportStream(mockStreamChannel, 900L, null);

    // When stream is writable, notifyWritabilityChanged triggers resumeTransfer()
    stream.notifyWritabilityChanged(true);
    verify(mockChunkedHandler).resumeTransfer();
  }
}
