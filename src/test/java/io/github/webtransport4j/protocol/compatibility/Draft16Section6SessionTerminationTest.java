package io.github.webtransport4j.protocol.compatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportCapsule;
import io.github.webtransport4j.server.WebTransportCapsuleDecoder;
import io.github.webtransport4j.server.WebTransportCapsuleHandler;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Protocol compatibility tests for draft-ietf-webtrans-http3-16 Section 6: Session Termination.
 * Directly exercises application classes: {@link WebTransportCapsuleDecoder} and
 * {@link WebTransportCapsuleHandler}.
 */
public class Draft16Section6SessionTerminationTest {

  /**
   * Section 6.1: Close WebTransport Session Capsule.
   * "WT_CLOSE_SESSION Capsule {
   *   Type (i) = 0x2843,
   *   Length (i),
   *   Application Error Code (32),
   *   Application Error Message (..),
   * }"
   * MANDATORY: Test WebTransportCapsuleDecoder parses wire capsule with type 0x2843.
   */
  @Test
  public void testSection6_1_CloseCapsuleDecodingInApplication() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebTransportCapsuleDecoder());
    channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(4L);

    ByteBuf in = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in, 0x2843L); // Type
    byte[] msgBytes = "Normal Close".getBytes(StandardCharsets.UTF_8);
    WebTransportUtils.writeVarInt(in, 4 + msgBytes.length); // Length
    in.writeInt(0); // Error code
    in.writeBytes(msgBytes);

    channel.writeInbound(in);

    WebTransportCapsule capsule = channel.readInbound();
    assertNotNull("Decoder must produce WebTransportCapsule", capsule);
    assertEquals("Capsule type must be 0x2843", 0x2843L, capsule.capsuleType());
    assertEquals("Session ID must be 4", 4L, capsule.sessionId());
    capsule.content().release();
    channel.finishAndReleaseAll();
  }

  /**
   * Section 6.1: Close WebTransport Session Capsule.
   * "Application Error Message: A UTF-8-encoded [RFC3629] explanation of the session termination.
   * The length of this field MUST NOT exceed 1024 bytes. An endpoint that receives an Application
   * Error Message longer than 1024 bytes MUST reset the CONNECT stream with an H3_MESSAGE_ERROR."
   * MANDATORY: Test WebTransportCapsuleHandler resets with H3_MESSAGE_ERROR when message > 1024 bytes.
   */
  @Test
  public void testSection6_1_ReasonPhraseExceeding1024BytesResetsWithH3MessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0); // Error code 0
    payload.writeZero(1025); // 1025 bytes message (> 1024 bound)

    WebTransportCapsule capsule = new WebTransportCapsule(0L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, capsule);

    verify(mockStream).shutdown(eq(0x010e), any());
  }

  /**
   * Section 6.1: Close WebTransport Session Capsule.
   * "If an endpoint receives a CLOSE_WEBTRANSPORT_SESSION capsule where the error message contains
   * invalid UTF-8, it MUST reset the connect stream with the error code H3_MESSAGE_ERROR."
   * MANDATORY: Test WebTransportCapsuleHandler resets with H3_MESSAGE_ERROR on malformed UTF-8.
   */
  @Test
  public void testSection6_1_InvalidUtf8InCloseMessageResetsWithH3MessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0); // Error code 0
    payload.writeByte(0xFF); // Invalid UTF-8 start byte
    payload.writeByte(0xFE);

    WebTransportCapsule capsule = new WebTransportCapsule(0L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, capsule);

    verify(mockStream).shutdown(eq(0x010e), any());
  }

  /**
   * Section 6.1: Close WebTransport Session Capsule.
   * "If an endpoint receives a CLOSE_WEBTRANSPORT_SESSION capsule whose length is less than four
   * bytes, it MUST reset the connect stream with the error code H3_MESSAGE_ERROR."
   * MANDATORY: Test WebTransportCapsuleHandler resets with H3_MESSAGE_ERROR on truncated payload.
   */
  @Test
  public void testSection6_1_PayloadLessThan4BytesResetsWithH3MessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    ByteBuf payload = Unpooled.buffer();
    payload.writeShort(0); // Only 2 bytes (< 4 bytes)

    WebTransportCapsule capsule = new WebTransportCapsule(0L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, capsule);

    verify(mockStream).shutdown(eq(0x010e), any());
  }

  /**
   * Section 6.1: Close WebTransport Session Capsule.
   * "A clean session closure corresponds to the Application Error Code of 0 and an empty Application
   * Error Message."
   * MANDATORY: Test WebTransportCapsuleHandler cleanly closes session on code 0 and empty message.
   */
  @Test
  public void testSection6_1_CleanCloseCodeZeroEmptyMessage() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0); // Clean close code 0, 0-byte message

    WebTransportCapsule capsule = new WebTransportCapsule(0L, 0x2843L, payload);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, capsule);

    verify(mockCtx).close();
  }

  /**
   * Section 6.1: Close WebTransport Session Capsule.
   * "The endpoint MAY wait for the peer to close the stream in response to receiving the
   * WT_CLOSE_SESSION capsule..."
   * OPTIONAL: Waiting before closing underlying QUIC stream.
   */
  @Test
  public void testSection6_1_WaitBeforeCloseUnderlyingStream_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 6.2: Immediate Closure.
   * "WT_SESSION_GONE (0x170d7b68) is used when the session is closed abruptly."
   * MANDATORY: Assert session gone error code codepoint.
   */
  @Test
  public void testSection6_2_ImmediateClosureSessionGoneCodepoint() {
    assertEquals(
        "WT_SESSION_GONE codepoint must be 0x170d7b68",
        0x170d7b68L,
        WebTransportUtils.WT_SESSION_GONE & 0xFFFFFFFFL);
  }
}
