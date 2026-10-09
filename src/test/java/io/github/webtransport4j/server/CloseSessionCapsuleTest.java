package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Tests for CLOSE_WEBTRANSPORT_SESSION capsule bounds and validation per draft-16 § 6.1. */
public class CloseSessionCapsuleTest {

  @Test
  public void testValidCloseCapsuleClosesSessionNormally() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0); // 32-bit error code
    payload.writeCharSequence("Clean close", StandardCharsets.UTF_8);

    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead0(mockCtx, capsule);

    verify(mockStream, never()).shutdown(eq(0x010e), any());
    verify(mockCtx).close();
    payload.release();
  }

  @Test
  public void testOversizedReasonPhraseResetsStreamWithMessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0);
    // 1025 bytes exceeds the 1024-byte limit in § 6.1
    byte[] oversized = new byte[1025];
    Arrays.fill(oversized, (byte) 'a');
    payload.writeBytes(oversized);

    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead0(mockCtx, capsule);

    verify(mockStream).shutdown(eq(0x010e), any());
    verify(mockCtx, never()).close();
    payload.release();
  }

  @Test
  public void testInvalidUtf8ReasonPhraseResetsStreamWithMessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0);
    // Invalid UTF-8 byte sequence: 0xC3 0x28 (truncated/invalid 2-byte sequence)
    payload.writeBytes(new byte[] {(byte) 0xC3, (byte) 0x28});

    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead0(mockCtx, capsule);

    verify(mockStream).shutdown(eq(0x010e), any());
    verify(mockCtx, never()).close();
    payload.release();
  }

  @Test
  public void testTruncatedPayloadResetsStreamWithMessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    ByteBuf payload = Unpooled.buffer();
    payload.writeBytes(new byte[] {0, 1}); // only 2 bytes (< 4 bytes)

    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead0(mockCtx, capsule);

    verify(mockStream).shutdown(eq(0x010e), any());
    verify(mockCtx, never()).close();
    payload.release();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUnregistersSessionBeforeShutdownOnMessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    io.netty.handler.codec.quic.QuicChannel mockParent =
        mock(io.netty.handler.codec.quic.QuicChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    WebTransportSessionManager mockMgr = mock(WebTransportSessionManager.class);
    io.netty.util.Attribute<WebTransportSessionManager> mgrAttr =
        mock(io.netty.util.Attribute.class);
    when(mgrAttr.get()).thenReturn(mockMgr);
    when(mockParent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

    ByteBuf payload = Unpooled.buffer();
    payload.writeBytes(new byte[] {0, 1}); // truncated < 4 bytes triggers message error

    WebTransportCapsule capsule = new WebTransportCapsule(100L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead0(mockCtx, capsule);

    org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(mockMgr, mockStream);
    inOrder.verify(mockMgr).unregister(mockStream);
    inOrder.verify(mockStream).shutdown(eq(0x010e), any());
    payload.release();
  }

  @Test
  public void testSessionCloseSendsCloseCapsuleAndClosesStream() {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(0L);
    when(mockStream.isActive()).thenReturn(true);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockStream.writeAndFlush(any())).thenReturn(mockFuture);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockStream, "/test", 100L, 100L, 10000L, 100L, 100L, 10000L, true, false);

    session.close(101L, "Finished");

    assertEquals(101, session.getCloseCode());
    assertEquals(101L, session.getCloseCodeAsLong());
    assertEquals("Finished", session.getCloseReason());

    ArgumentCaptor<DefaultHttp3DataFrame> frameCaptor =
        ArgumentCaptor.forClass(DefaultHttp3DataFrame.class);
    verify(mockStream).writeAndFlush(frameCaptor.capture());
    verify(mockFuture).addListener(ChannelFutureListener.CLOSE);

    ByteBuf content = frameCaptor.getValue().content();
    long capsuleType = WebTransportUtils.readVariableLengthInt(content);
    assertEquals(0x2843L, capsuleType);
    long capsuleLen = WebTransportUtils.readVariableLengthInt(content);
    assertEquals(4L + "Finished".length(), capsuleLen);
    long errorCode = content.readUnsignedInt();
    assertEquals(101L, errorCode);
    byte[] reasonBytes = new byte[content.readableBytes()];
    content.readBytes(reasonBytes);
    assertEquals("Finished", new String(reasonBytes, StandardCharsets.UTF_8));
    frameCaptor.getValue().release();
  }

  @Test
  public void testSessionCloseWithCodeZeroSendsCloseCapsule() {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(0L);
    when(mockStream.isActive()).thenReturn(true);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockStream.writeAndFlush(any())).thenReturn(mockFuture);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockStream, "/test", 100L, 100L, 10000L, 100L, 100L, 10000L, true, false);

    session.close();

    assertEquals(0, session.getCloseCode());
    assertEquals(0L, session.getCloseCodeAsLong());

    ArgumentCaptor<DefaultHttp3DataFrame> frameCaptor =
        ArgumentCaptor.forClass(DefaultHttp3DataFrame.class);
    verify(mockStream).writeAndFlush(frameCaptor.capture());
    verify(mockFuture).addListener(ChannelFutureListener.CLOSE);

    ByteBuf content = frameCaptor.getValue().content();
    long capsuleType = WebTransportUtils.readVariableLengthInt(content);
    assertEquals(0x2843L, capsuleType);
    long capsuleLen = WebTransportUtils.readVariableLengthInt(content);
    assertEquals(4L, capsuleLen);
    long errorCode = content.readUnsignedInt();
    assertEquals(0L, errorCode);
    frameCaptor.getValue().release();
  }

  @Test
  public void testSessionCloseWithUnsigned32BitCodePreservesValue() {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(0L);
    when(mockStream.isActive()).thenReturn(true);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockStream.writeAndFlush(any())).thenReturn(mockFuture);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockStream, "/test", 100L, 100L, 10000L, 100L, 100L, 10000L, true, false);

    long highCode = 0xFFFFFFFFL;
    session.close(highCode, "High Code");

    assertEquals(highCode, session.getCloseCodeAsLong());
    assertEquals(highCode, Integer.toUnsignedLong(session.getCloseCode()));

    ArgumentCaptor<DefaultHttp3DataFrame> frameCaptor =
        ArgumentCaptor.forClass(DefaultHttp3DataFrame.class);
    verify(mockStream).writeAndFlush(frameCaptor.capture());

    ByteBuf content = frameCaptor.getValue().content();
    assertEquals(0x2843L, WebTransportUtils.readVariableLengthInt(content));
    WebTransportUtils.readVariableLengthInt(content);
    assertEquals(highCode, content.readUnsignedInt());
    frameCaptor.getValue().release();
  }

  @Test
  public void testSessionCloseRejectsOutOfRangeCodesWithoutModifyingState() {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(0L);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockStream, "/test", 100L, 100L, 10000L, 100L, 100L, 10000L, true, false);

    try {
      session.close(-1L, "Invalid negative");
      fail("Expected IllegalArgumentException for negative code");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    assertTrue(session.isOpen());
    assertEquals(0L, session.getCloseCodeAsLong());

    try {
      session.close(0x1_0000_0000L, "Exceeds 32-bit");
      fail("Expected IllegalArgumentException for code > 0xFFFFFFFFL");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    assertTrue(session.isOpen());
    assertEquals(0L, session.getCloseCodeAsLong());
  }

  @Test
  public void testSessionAbortPerformsAbruptResetWithoutCapsule() {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(0L);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockStream, "/test", 100L, 100L, 10000L, 100L, 100L, 10000L, true, false);

    session.abort(500L);

    verify(mockStream).shutdown(eq(500), any());
    verify(mockStream, never()).writeAndFlush(any());
    assertEquals(500L, session.getCloseCodeAsLong());

    try {
      session.abort(-1L);
      fail("Expected IllegalArgumentException for negative abort code");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testSetCloseCodePreservesUnsigned32BitRangeAndRejectsInvalid() {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(0L);

    DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockStream, "/test", 100L, 100L, 10000L, 100L, 100L, 10000L, true, false);

    NettyWebTransportSession nettySession = session;
    nettySession.setCloseCode(0xFFFFFFFFL);
    assertEquals(0xFFFFFFFFL, session.getCloseCodeAsLong());
    assertEquals(-1, session.getCloseCode());

    // Test int overload widens unsigned int properly without negative loss
    nettySession.setCloseCode(-1);
    assertEquals(0xFFFFFFFFL, session.getCloseCodeAsLong());

    try {
      nettySession.setCloseCode(-1L);
      fail("Expected IllegalArgumentException for negative close code");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      nettySession.setCloseCode(0x1_0000_0000L);
      fail("Expected IllegalArgumentException for code exceeding 32 bits");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testSessionCloseTruncatesReasonPreservingUtf8Boundaries() throws Exception {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(0L);
    when(mockStream.isActive()).thenReturn(true);
    when(mockStream.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    // 1021 'a' characters + 4-byte emoji "\uD83D\uDE80" (🚀) = 1025 bytes in UTF-8
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1021; i++) {
      sb.append('a');
    }
    sb.append("\uD83D\uDE80"); // 4 bytes: 1021 + 4 = 1025 bytes
    sb.append("extra");

    final DefaultWebTransportSession session =
        new DefaultWebTransportSession(
            0L, mockStream, "/test", 100L, 100L, 10000L, 100L, 100L, 10000L, true, false);

    session.close(0L, sb.toString());

    ArgumentCaptor<DefaultHttp3DataFrame> frameCaptor =
        ArgumentCaptor.forClass(DefaultHttp3DataFrame.class);
    verify(mockStream).writeAndFlush(frameCaptor.capture());

    ByteBuf content = frameCaptor.getValue().content();
    long capsuleType = WebTransportUtils.readVariableLengthInt(content);
    assertEquals(0x2843L, capsuleType);
    long capsuleLen = WebTransportUtils.readVariableLengthInt(content);
    // 4 bytes error code + 1021 bytes reason (incomplete 4-byte emoji excluded)
    assertEquals(4L + 1021L, capsuleLen);
    content.readUnsignedInt(); // error code
    byte[] reasonBytes = new byte[content.readableBytes()];
    content.readBytes(reasonBytes);
    assertEquals(1021, reasonBytes.length);

    // Strictly verify UTF-8 validity without malformed input errors
    StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(reasonBytes));
    String decodedReason = new String(reasonBytes, StandardCharsets.UTF_8);
    assertEquals(1021, decodedReason.length());
    frameCaptor.getValue().release();
  }
}
