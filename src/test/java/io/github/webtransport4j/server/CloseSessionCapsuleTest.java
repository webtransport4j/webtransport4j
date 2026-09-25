package io.github.webtransport4j.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;

/**
 * Tests for CLOSE_WEBTRANSPORT_SESSION capsule bounds and validation per draft-16 § 6.1.
 */
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
}
