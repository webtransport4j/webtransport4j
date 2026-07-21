package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Tests for the default Netty-backed stream implementation. */
public class DefaultNettyWebTransportStreamTest {

  @Test
  public void testWriteNettyBufferUsesRetainedSlice() {
    QuicStreamChannel channel = mock(QuicStreamChannel.class);
    ChannelFuture future = mock(ChannelFuture.class);
    when(channel.streamId()).thenReturn(4L);
    when(channel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    when(channel.writeAndFlush(any())).thenReturn(future);
    org.mockito.Mockito.doAnswer(invocation -> {
      io.netty.util.concurrent.GenericFutureListener listener = invocation.getArgument(0);
      listener.operationComplete(future);
      return future;
    }).when(future).addListener(any());
    when(future.isSuccess()).thenReturn(true);

    ByteBuf source = Unpooled.wrappedBuffer("abcdef".getBytes());
    source.skipBytes(1);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(source);
    DefaultNettyWebTransportStream stream = new DefaultNettyWebTransportStream(channel, 0L);

    stream.write(buffer);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(channel).writeAndFlush(captor.capture());
    ByteBuf written = (ByteBuf) captor.getValue();
    assertEquals(5, written.readableBytes());
    assertEquals('b', written.getByte(written.readerIndex()));
    assertEquals(2, source.refCnt());

    source.setByte(1, 'z');
    assertEquals('z', written.getByte(written.readerIndex()));

    written.release();
    assertEquals(1, source.refCnt());
    source.release();
  }
}
