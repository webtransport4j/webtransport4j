package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests verifying that HTTP capsule size limits (RFC draft-16) are strictly enforced by
 * {@link WebTransportCapsuleDecoder} to prevent memory allocation exhaustion.
 */
public class WebTransportCapsuleDecoderTest {

  @Test
  public void testDefaultMaxCapsuleLength() {
    WebTransportCapsuleDecoder decoder = new WebTransportCapsuleDecoder();
    assertEquals(65536, decoder.maxCapsuleLength());
  }

  @Test
  public void testCustomMaxCapsuleLength() {
    WebTransportCapsuleDecoder decoder = new WebTransportCapsuleDecoder(1024);
    assertEquals(1024, decoder.maxCapsuleLength());
  }

  @Test
  public void testCapsuleWithinLimitDecodesSuccessfully() {
    EmbeddedChannel channel = new EmbeddedChannel(new WebTransportCapsuleDecoder());
    channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(42L);

    ByteBuf input = Unpooled.buffer();
    WebTransportUtils.writeVarInt(input, 0x2844); // WT_DRAIN_SESSION
    WebTransportUtils.writeVarInt(input, 4);
    input.writeBytes("test".getBytes(StandardCharsets.UTF_8));

    assertTrue(channel.writeInbound(input));

    WebTransportCapsule capsule = channel.readInbound();
    assertNotNull("Capsule should be produced", capsule);
    assertEquals(42L, capsule.sessionId());
    assertEquals(0x2844, capsule.capsuleType());
    assertEquals("test", capsule.content().toString(StandardCharsets.UTF_8));
    capsule.content().release();
    channel.finishAndReleaseAll();
  }

  @Test
  public void testCapsuleExceedingLimitClosesEmbeddedChannel() {
    // Limit to 128 bytes
    EmbeddedChannel channel = new EmbeddedChannel(new WebTransportCapsuleDecoder(128));
    channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(100L);

    ByteBuf input = Unpooled.buffer();
    WebTransportUtils.writeVarInt(input, 0x2843); // CLOSE_WEBTRANSPORT_SESSION
    WebTransportUtils.writeVarInt(input, 200); // 200 > 128 limit
    input.writeZero(200);

    // Channel should close upon encountering oversized capsule
    channel.writeInbound(input);

    assertNull("No capsule should be emitted for oversized frame", channel.readInbound());
    assertFalse("Channel must be closed to prevent memory exhaustion", channel.isOpen());
  }

  @Test
  public void testOversizedCapsuleOnQuicStreamShutsDownWithH3MessageError() {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    ChannelPromise promise = mock(ChannelPromise.class);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    DefaultAttributeMap quicAttrMap = new DefaultAttributeMap();

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.newPromise()).thenReturn(promise);
    when(mockStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockQuic.attr(any())).thenAnswer(inv -> quicAttrMap.attr(inv.getArgument(0)));

    WebTransportSessionManager sessionManager = mock(WebTransportSessionManager.class);
    quicAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(sessionManager);

    ByteBuf input = Unpooled.buffer();
    WebTransportUtils.writeVarInt(input, 0x2843);
    WebTransportUtils.writeVarInt(input, 65537); // Exceeds 1024 limit

    List<Object> out = new ArrayList<>();
    final WebTransportCapsuleDecoder decoder = new WebTransportCapsuleDecoder(1024);
    decoder.decode(mockCtx, input, out);

    // Verify unregistration and stream reset with 0x010e (H3_MESSAGE_ERROR)
    verify(sessionManager).unregister(mockStream);
    verify(mockStream).shutdown(eq(0x010e), any());
    assertTrue("Out list should be empty", out.isEmpty());
    input.release();
  }
}
