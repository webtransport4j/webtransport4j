package io.github.webtransport4j.concurrency;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.server.*;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.quic.*;
import io.netty.util.*;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.mockito.Mockito;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

/** Actual stream creation check/increment race against a single unit of peer credit. */
@JCStressTest
@State
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "One cumulative index reserved.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Peer stream credit overspent.")
public class StreamCreditStress {
  final QuicChannel parent = Mockito.mock(QuicChannel.class);
  final QuicStreamChannel connect = Mockito.mock(QuicStreamChannel.class);
  final DefaultWebTransportSession session;
  final io.netty.channel.DefaultEventLoop loop = new io.netty.channel.DefaultEventLoop();

  @SuppressWarnings("unchecked")
  public StreamCreditStress() {
    when(connect.parent()).thenReturn(parent);
    when(connect.isOpen()).thenReturn(true);
    when(connect.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(connect.writeAndFlush(any()))
        .thenAnswer(
            call -> {
              ReferenceCountUtil.release(call.getArgument(0));
              return null;
            });
    when(parent.eventLoop()).thenReturn(loop);
    when(parent.createStream(any(QuicStreamType.class), any(ChannelHandler.class)))
        .thenAnswer(
            call -> ImmediateEventExecutor.INSTANCE.newFailedFuture(new Exception("failed open")));
    session = new DefaultWebTransportSession(0, connect, "/test", 1, 1, 100, 1, 1, 100, true, true);
    WebTransportSessionManager manager = Mockito.mock(WebTransportSessionManager.class);
    when(manager.get(0)).thenReturn(session);
    DefaultAttributeMap attributes = new DefaultAttributeMap();
    attributes.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);
    when(parent.attr(any(AttributeKey.class)))
        .thenAnswer(call -> attributes.attr(call.getArgument(0)));
  }

  @Actor
  public void first() {
    WebTransportUtils.createBiStream(
        connect, false, DefaultWebTransportSession.DEFAULT_BI_INITIALIZER);
  }

  @Actor
  public void second() {
    WebTransportUtils.createBiStream(
        connect, false, DefaultWebTransportSession.DEFAULT_BI_INITIALIZER);
  }

  @Arbiter
  public void check(I_Result result) {
    loop.submit(() -> {}).syncUninterruptibly();
    result.r1 = (int) session.getServerInitiatedStreamsBidi();
    loop.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
  }
}
