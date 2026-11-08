package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportCapsule;
import io.github.webtransport4j.server.WebTransportCapsuleDecoder;
import io.github.webtransport4j.server.WebTransportCapsuleHandler;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Mocked unit tests for {@code draft-ietf-webtrans-http3-17} Section 6: Session Termination.
 */
public class Draft17Section6SessionTerminationTest {

  @Test
  public void testSection6_1_CloseCapsuleDecodingInApplication() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebTransportCapsuleDecoder());
    channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(4L);

    ByteBuf in = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in, WebTransportProtocolSpec.CAPSULE_CLOSE_WEBTRANSPORT_SESSION);
    byte[] msgBytes = "Normal Close".getBytes(StandardCharsets.UTF_8);
    WebTransportUtils.writeVarInt(in, 4 + msgBytes.length);
    in.writeInt(0);
    in.writeBytes(msgBytes);

    channel.writeInbound(in);

    WebTransportCapsule capsule = channel.readInbound();
    assertNotNull(capsule);
    assertEquals(WebTransportProtocolSpec.CAPSULE_CLOSE_WEBTRANSPORT_SESSION, capsule.capsuleType());
    assertEquals(4L, capsule.sessionId());
    capsule.content().release();
    channel.finishAndReleaseAll();
  }

  @Test
  public void testSection6_1_ReasonPhraseExceeding1024BytesResetsWithH3MessageError()
      throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0);
    payload.writeZero(1025);

    WebTransportCapsuleHandler.INSTANCE.channelRead(
        mockCtx,
        new WebTransportCapsule(
            0L, WebTransportProtocolSpec.CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload));

    verify(mockStream).shutdown(eq(WebTransportProtocolSpec.H3_MESSAGE_ERROR), any());
  }

  @Test
  public void testSection6_1_InvalidUtf8InCloseMessageResetsWithH3MessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0);
    payload.writeByte(0xFF);
    payload.writeByte(0xFE);

    WebTransportCapsuleHandler.INSTANCE.channelRead(
        mockCtx,
        new WebTransportCapsule(
            0L, WebTransportProtocolSpec.CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload));

    verify(mockStream).shutdown(eq(WebTransportProtocolSpec.H3_MESSAGE_ERROR), any());
  }

  @Test
  public void testSection6_1_PayloadLessThan4BytesResetsWithH3MessageError() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    ByteBuf payload = Unpooled.buffer();
    payload.writeShort(0);

    WebTransportCapsuleHandler.INSTANCE.channelRead(
        mockCtx,
        new WebTransportCapsule(
            0L, WebTransportProtocolSpec.CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload));

    verify(mockStream).shutdown(eq(WebTransportProtocolSpec.H3_MESSAGE_ERROR), any());
  }

  @Test
  public void testSection6_1_CleanCloseCodeZeroEmptyMessage() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    when(mockCtx.channel()).thenReturn(mockStream);

    ByteBuf payload = Unpooled.buffer();
    payload.writeInt(0);

    WebTransportCapsuleHandler.INSTANCE.channelRead(
        mockCtx,
        new WebTransportCapsule(
            0L, WebTransportProtocolSpec.CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload));

    verify(mockCtx).close();
  }

  @Test
  public void testSection6_2_ImmediateClosureSessionGoneCodepoint() {
    assertEquals(
        0x170d7b68L, WebTransportProtocolSpec.WT_SESSION_GONE & 0xFFFFFFFFL);
  }
}

