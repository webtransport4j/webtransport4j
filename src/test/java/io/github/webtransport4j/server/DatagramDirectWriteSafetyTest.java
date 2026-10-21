package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Validates that datagram direct writes do not rewind readerIndex, do not corrupt headers,
 * and correctly prepend the session-specific quarterSessionId.
 */
public class DatagramDirectWriteSafetyTest {

  @Test
  public void testSendDatagramDirectDoesNotRewindReaderIndexAndPrependsCorrectHeader() {
    QuicChannel parentChannel = mock(QuicChannel.class);
    when(parentChannel.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(parentChannel.voidPromise()).thenReturn(mock(ChannelPromise.class));

    List<ByteBuf> capturedPackets = new ArrayList<>();
    when(parentChannel.writeAndFlush(any(ByteBuf.class), any())).thenAnswer(invocation -> {
      capturedPackets.add(invocation.getArgument(0));
      return mock(ChannelFuture.class);
    });

    QuicStreamChannel connectStream = mock(QuicStreamChannel.class);
    when(connectStream.parent()).thenReturn(parentChannel);
    when(connectStream.isActive()).thenReturn(true);
    when(connectStream.isOpen()).thenReturn(true);

    // sessionStreamId == 4 => quarterSessionId == 1
    DefaultWebTransportSession session =
        new DefaultWebTransportSession(4L, connectStream, "/test", 10, 10, 1000, 10, 10, 1000, false, false);

    // Create a payload where readerIndex is already advanced past 0 (e.g. 4 bytes read)
    ByteBuf payload = Unpooled.buffer(16);
    payload.writeBytes(new byte[] {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte) 0x88});
    payload.readerIndex(4); // Only [0x55, 0x66, 0x77, 0x88] are readable

    int initialReaderIndex = payload.readerIndex();
    int initialRefCnt = payload.refCnt();

    session.sendDatagramDirect(payload);

    // Verify caller buffer readerIndex was NOT rewound or mutated
    assertEquals(initialReaderIndex, payload.readerIndex());
    // Verify caller retains ownership (refCnt unchanged)
    assertEquals(initialRefCnt, payload.refCnt());

    assertEquals(1, capturedPackets.size());
    ByteBuf packet = capturedPackets.get(0);

    // Decode quarterSessionId varint from packet
    long quarterSessionId = WebTransportUtils.readVariableLengthInt(packet);
    assertEquals(1L, quarterSessionId);

    // Verify remaining bytes are the actual payload [0x55, 0x66, 0x77, 0x88], NOT [0x11, 0x22, 0x33, 0x44]
    assertEquals(4, packet.readableBytes());
    assertEquals(0x55, packet.readByte());
    assertEquals(0x66, packet.readByte());
    assertEquals(0x77, packet.readByte());
    assertEquals((byte) 0x88, packet.readByte());

    packet.release();
    payload.release();
  }
}
