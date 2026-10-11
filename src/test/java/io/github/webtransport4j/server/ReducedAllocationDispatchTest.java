package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Unit tests verifying reduced allocation dispatching behavior.
 */
public class ReducedAllocationDispatchTest {
  private EmbeddedChannel channel(WebTransportHandler handler) {
    EmbeddedChannel channel = new EmbeddedChannel(ZeroGcMessageDispatcher.INSTANCE);
    NettyWebTransportSession session = mock(NettyWebTransportSession.class);
    when(session.path()).thenReturn("/test");
    when(session.isOpen()).thenReturn(true);
    WebTransportSessionManager manager = mock(WebTransportSessionManager.class);
    when(manager.get(0L)).thenReturn(session);
    WebTransportServer server = mock(WebTransportServer.class);
    when(server.getHandler("/test")).thenReturn(handler);
    channel.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);
    channel.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);
    return channel;
  }

  private ByteBuf datagram(int value) {
    return Unpooled.buffer().writeByte(0).writeByte(value);
  }

  @Test
  public void repeatedRejectedStreamsRetireAbusiveConnection() {
    QuicChannel quic = mock(QuicChannel.class);
    DefaultAttributeMap attributes = new DefaultAttributeMap();
    when(quic.attr(any())).thenAnswer(call -> attributes.attr(call.getArgument(0)));
    int limit = WebTransportConfig.getInt(
        "webtransport4j.webtransport.max.rejected.streams.per.connection", 64);
    for (int i = 0; i < limit; i++) {
      QuicStreamChannel stream = mock(QuicStreamChannel.class);
      when(stream.parent()).thenReturn(quic);
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      when(ctx.channel()).thenReturn(stream);
      WebTransportUtils.rejectClientStream(ctx, WebTransportUtils.WT_SESSION_GONE);
      verify(stream).close();
      if (i < limit - 1) {
        verify(quic, never()).close(anyBoolean(), anyInt(), any(ByteBuf.class));
      }
    }
    verify(quic).close(eq(true),
        eq((int) io.netty.handler.codec.http3.Http3ErrorCode.H3_EXCESSIVE_LOAD.code()),
        any(ByteBuf.class));
  }

  @Test
  public void rawStreamUsesOwnedCallbackBuffer() throws Exception {
    QuicChannel quic = mock(QuicChannel.class);
    QuicStreamChannel stream = mock(QuicStreamChannel.class);
    DefaultAttributeMap parentAttrs = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();
    when(quic.attr(any())).thenAnswer(call -> parentAttrs.attr(call.getArgument(0)));
    when(stream.attr(any())).thenAnswer(call -> streamAttrs.attr(call.getArgument(0)));
    when(stream.parent()).thenReturn(quic);
    NettyWebTransportSession session = mock(NettyWebTransportSession.class);
    when(session.isOpen()).thenReturn(true);
    WebTransportSessionManager manager = mock(WebTransportSessionManager.class);
    when(manager.get(0L)).thenReturn(session);
    parentAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);
    streamAttrs.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(0L);
    streamAttrs.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).set(true);
    WebTransportStream apiStream = mock(WebTransportStream.class);
    AtomicReference<WebTransportBuffer> retained = new AtomicReference<>();
    when(apiStream.getDataConsumer()).thenReturn(buffer -> {
      assertSame(buffer, buffer.retain());
      retained.set(buffer);
    });
    streamAttrs.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(apiStream);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(stream);
    ByteBuf input = Unpooled.buffer().writeByte(42);
    ZeroGcMessageDispatcher.INSTANCE.channelRead(ctx, input);
    assertEquals(42, retained.get().getByte(0));
    retained.get().release();
    assertEquals(0, input.refCnt());
  }

  @Test
  public void retainedCallbackBufferSurvivesNestedDispatch() {
    AtomicReference<EmbeddedChannel> channel = new AtomicReference<>();
    AtomicReference<WebTransportBuffer> retained = new AtomicReference<>();
    WebTransportHandler handler = new WebTransportHandler() {
      @Override
      public void onDatagramReceived(io.github.webtransport4j.api.WebTransportSession session,
          WebTransportBuffer buffer) {
        if (buffer.getByte(0) == 1) {
          assertSame(buffer, buffer.retain());
          retained.set(buffer);
          channel.get().writeInbound(datagram(2));
          assertEquals(1, buffer.getByte(0));
        } else {
          assertEquals(2, buffer.getByte(0));
        }
      }
    };
    EmbeddedChannel ch = channel(handler);
    channel.set(ch);
    ByteBuf input = datagram(1);
    try {
      ch.writeInbound(input);
      assertEquals(1, retained.get().getByte(0));
      retained.get().release();
      assertEquals(0, input.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test
  public void directEventLoopDispatchRetainsAndReleasesCallbackPayload() {
    AtomicReference<WebTransportBuffer> seen = new AtomicReference<>();
    EmbeddedChannel ch = channel(new WebTransportHandler() {
      @Override
      public void onDatagramReceived(io.github.webtransport4j.api.WebTransportSession session,
          WebTransportBuffer buffer) {
        assertEquals(3, buffer.getByte(0));
        seen.set(buffer);
      }
    });
    ByteBuf input = datagram(3);
    try {
      ch.writeInbound(input);
      assertNotNull(seen.get());
      assertEquals(0, input.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test
  public void applicationOffloadRetainsBufferAcrossAsynchronousExecution() {
    AtomicReference<WebTransportBuffer> asyncSeen = new AtomicReference<>();
    List<Runnable> asyncTasks = new ArrayList<>();
    EmbeddedChannel ch = channel(new WebTransportHandler() {
      @Override
      public void onDatagramReceived(io.github.webtransport4j.api.WebTransportSession session,
          WebTransportBuffer buffer) {
        WebTransportBuffer retainedBuffer = buffer.retain();
        asyncTasks.add(() -> {
          try {
            assertEquals(4, retainedBuffer.getByte(0));
            asyncSeen.set(retainedBuffer);
          } finally {
            retainedBuffer.release();
          }
        });
      }
    });
    ByteBuf input = datagram(4);
    try {
      ch.writeInbound(input);
      assertEquals(1, input.refCnt());
      assertEquals(1, asyncTasks.size());
      asyncTasks.remove(0).run();
      assertNotNull(asyncSeen.get());
      assertEquals(0, input.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test
  public void malformedDatagramAndThrowingCallbackReleasePayload() {
    EmbeddedChannel ch = channel(new WebTransportHandler() {
      @Override
      public void onDatagramReceived(io.github.webtransport4j.api.WebTransportSession session,
          WebTransportBuffer buffer) {
        throw new IllegalStateException("test callback failure");
      }
    });
    ByteBuf malformed = Unpooled.buffer().writeByte(0xc0);
    ByteBuf valid = datagram(5);
    try {
      ch.writeInbound(malformed);
      ch.writeInbound(valid);
      assertEquals(0, malformed.refCnt());
      assertEquals(0, valid.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test
  public void closedSessionDoesNotReceiveQueuedData() {
    WebTransportHandler handler = mock(WebTransportHandler.class);
    EmbeddedChannel ch = channel(handler);
    NettyWebTransportSession session = ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get().get(0L);
    when(session.isOpen()).thenReturn(false);
    ByteBuf input = datagram(6);
    try {
      ch.writeInbound(input);
      verifyNoInteractions(handler);
      assertEquals(0, input.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test
  public void applicationExecutorRejectionHandlesBufferReleaseGracefully() {
    ExecutorService rejectingExecutor = mock(ExecutorService.class);
    doThrow(new java.util.concurrent.RejectedExecutionException("worker pool full"))
        .when(rejectingExecutor).execute(any(Runnable.class));
    EmbeddedChannel ch = channel(new WebTransportHandler() {
      @Override
      public void onDatagramReceived(io.github.webtransport4j.api.WebTransportSession session,
          WebTransportBuffer buffer) {
        WebTransportBuffer retained = buffer.retain();
        try {
          rejectingExecutor.execute(() -> {
            try {
              // async work
            } finally {
              retained.release();
            }
          });
        } catch (java.util.concurrent.RejectedExecutionException e) {
          retained.release();
        }
      }
    });
    ByteBuf input = datagram(8);
    try {
      ch.writeInbound(input);
      assertEquals(0, input.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test
  public void ownedFlyweightBalancesMultipleReferences() {
    ByteBuf source = Unpooled.buffer().writeByte(9);
    FlyweightWebTransportBuffer buffer = FlyweightWebTransportBuffer.wrap(source);
    assertSame(buffer, buffer.retain());
    buffer.release();
    assertEquals(9, buffer.getByte(0));
    buffer.release();
    assertEquals(0, source.refCnt());
  }

  @Test
  public void borrowedFlyweightPromotesSameIdentityAndCannotBeRecycled() {
    ByteBuf source = Unpooled.buffer().writeByte(7);
    FlyweightWebTransportBuffer buffer = FlyweightWebTransportBuffer.createFlyweight().attach(source);
    assertSame(buffer, buffer.retain());
    buffer.detach();
    source.release();
    assertEquals(7, buffer.getByte(0));
    buffer.release();
    assertEquals(0, source.refCnt());
    ByteBuf replacement = Unpooled.buffer();
    try {
      assertThrows(IllegalStateException.class, () -> buffer.attach(replacement));
    } finally {
      replacement.release();
    }
  }

  @Test
  public void rawByteBufStreamDispatchDirectlyInvokesConsumerWithoutWrapper() throws Exception {
    QuicChannel quic = mock(QuicChannel.class);
    QuicStreamChannel streamChannel = mock(QuicStreamChannel.class);
    DefaultAttributeMap parentAttrs = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();
    when(quic.attr(any())).thenAnswer(call -> parentAttrs.attr(call.getArgument(0)));
    when(streamChannel.attr(any())).thenAnswer(call -> streamAttrs.attr(call.getArgument(0)));
    when(streamChannel.parent()).thenReturn(quic);
    NettyWebTransportSession session = mock(NettyWebTransportSession.class);
    when(session.isOpen()).thenReturn(true);
    WebTransportSessionManager manager = mock(WebTransportSessionManager.class);
    when(manager.get(0L)).thenReturn(session);
    parentAttrs.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);
    streamAttrs.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(0L);
    streamAttrs.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).set(true);

    DefaultNettyWebTransportStream apiStream =
        new DefaultNettyWebTransportStream(streamChannel, 0L);
    AtomicReference<ByteBuf> captured = new AtomicReference<>();
    apiStream.onRawByteBuf(buf -> {
      captured.set(buf.retain());
      assertEquals(55, buf.readByte());
    });
    assertThrows(IllegalStateException.class, () -> apiStream.onData(b -> {}));

    streamAttrs.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(apiStream);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(streamChannel);
    ByteBuf input = Unpooled.buffer().writeByte(55);
    ZeroGcMessageDispatcher.INSTANCE.channelRead(ctx, input);

    assertNotNull(captured.get());
    assertEquals(1, captured.get().refCnt());
    captured.get().release();
    assertEquals(0, input.refCnt());
  }

  @Test
  public void rawByteBufDatagramDispatchDirectlyInvokesConsumerWithoutWrapper() {
    DefaultWebTransportSession session = mock(DefaultWebTransportSession.class);
    when(session.isOpen()).thenReturn(true);
    when(session.path()).thenReturn("/test");
    AtomicReference<ByteBuf> captured = new AtomicReference<>();
    when(session.getRawDatagramConsumer()).thenReturn(buf -> {
      captured.set(buf.retain());
      assertEquals(77, buf.readByte());
    });

    EmbeddedChannel ch = new EmbeddedChannel(ZeroGcMessageDispatcher.INSTANCE);
    WebTransportSessionManager manager = mock(WebTransportSessionManager.class);
    when(manager.get(0L)).thenReturn(session);
    ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);

    ByteBuf input = datagram(77);
    try {
      ch.writeInbound(input);
      assertNotNull(captured.get());
      assertEquals(1, captured.get().refCnt());
      captured.get().release();
      assertEquals(0, input.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }
}
