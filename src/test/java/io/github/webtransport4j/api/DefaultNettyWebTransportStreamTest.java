package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamPriority;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests for the default Netty-backed stream implementation. */
public class DefaultNettyWebTransportStreamTest {

  @Test
  public void testWriteNettyBufferUsesRetainedSlice() {
    QuicStreamChannel channel = mock(QuicStreamChannel.class);
    ChannelFuture future = mock(ChannelFuture.class);
    when(channel.streamId()).thenReturn(4L);
    when(channel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    when(channel.writeAndFlush(any())).thenReturn(future);
    Mockito.doAnswer(invocation -> {
      GenericFutureListener listener = invocation.getArgument(0);
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

  @Test
  public void testSetPriorityAndGetPriority() {
    QuicStreamChannel channel = mock(QuicStreamChannel.class);
    ChannelFuture future = mock(ChannelFuture.class);
    when(channel.streamId()).thenReturn(4L);
    when(channel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    when(channel.updatePriority(any())).thenReturn(future);
    when(future.isDone()).thenReturn(true);
    when(future.isSuccess()).thenReturn(true);

    DefaultNettyWebTransportStream stream = new DefaultNettyWebTransportStream(channel, 0L);
    CompletableFuture<Void> priorityFuture = stream.setPriority(StreamPriority.of(2, true));
    assertTrue(priorityFuture.isDone());
    assertFalse(priorityFuture.isCompletedExceptionally());

    ArgumentCaptor<QuicStreamPriority> captor = ArgumentCaptor.forClass(QuicStreamPriority.class);
    verify(channel).updatePriority(captor.capture());
    assertEquals(2, captor.getValue().urgency());
    assertTrue(captor.getValue().isIncremental());

    when(channel.priority()).thenReturn(new QuicStreamPriority(2, true));
    StreamPriority retrieved = stream.getPriority();
    assertEquals(2, retrieved.urgency());
    assertTrue(retrieved.isIncremental());
  }

  @Test
  public void testGetPriorityDefaultWhenNull() {
    QuicStreamChannel channel = mock(QuicStreamChannel.class);
    when(channel.priority()).thenReturn(null);

    DefaultNettyWebTransportStream stream = new DefaultNettyWebTransportStream(channel, 0L);
    assertEquals(StreamPriority.DEFAULT, stream.getPriority());
  }

  @Test
  public void testSetPriorityConvenienceOverload() {
    QuicStreamChannel channel = mock(QuicStreamChannel.class);
    ChannelFuture future = mock(ChannelFuture.class);
    when(channel.updatePriority(any())).thenReturn(future);
    when(future.isDone()).thenReturn(true);
    when(future.isSuccess()).thenReturn(true);

    DefaultNettyWebTransportStream stream = new DefaultNettyWebTransportStream(channel, 0L);
    CompletableFuture<Void> priorityFuture = stream.setPriority(5, false);
    assertTrue(priorityFuture.isDone());

    ArgumentCaptor<QuicStreamPriority> captor = ArgumentCaptor.forClass(QuicStreamPriority.class);
    verify(channel).updatePriority(captor.capture());
    assertEquals(5, captor.getValue().urgency());
    assertFalse(captor.getValue().isIncremental());
  }
}
