package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.Random;
import org.junit.Test;

/** Sequential stream creation/grant histories checked against cumulative credit semantics. */
public class FlowControlModelBasedTest {
  @Test
  @SuppressWarnings("unchecked")
  public void failedCreationConsumesCreditWithoutRefundOnClose() {
    Random random = new Random(17041);
    for (int history = 0; history < 100; history++) {
      QuicChannel parent = mock(QuicChannel.class);
      QuicStreamChannel connect = mock(QuicStreamChannel.class);
      when(connect.parent()).thenReturn(parent);
      when(connect.isOpen()).thenReturn(true);
      EventLoop loop = mock(EventLoop.class);
      when(parent.eventLoop()).thenReturn(loop);
      when(loop.inEventLoop()).thenReturn(true);
      when(loop.newPromise()).thenAnswer(call -> ImmediateEventExecutor.INSTANCE.newPromise());
      when(parent.createStream(any(QuicStreamType.class), any(ChannelHandler.class)))
          .thenAnswer(
              call ->
                  ImmediateEventExecutor.INSTANCE.newFailedFuture(new Exception("failed open")));
      DefaultWebTransportSession session =
          new DefaultWebTransportSession(0, connect, "/test", 1, 1, 100, 1, 1, 100, true, true);
      WebTransportSessionManager manager = mock(WebTransportSessionManager.class);
      when(manager.get(0)).thenReturn(session);
      DefaultAttributeMap attributes = new DefaultAttributeMap();
      attributes.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);
      when(parent.attr(any(AttributeKey.class)))
          .thenAnswer(call -> attributes.attr(call.getArgument(0)));
      long limit = 1;
      long used = 0;
      for (int step = 0; step < 20; step++) {
        if (random.nextBoolean()) {
          limit++;
          session.setPeerSettingsMaxStreamsBidi(limit);
        } else {
          WebTransportUtils.createBiStream(
              connect, false, DefaultWebTransportSession.DEFAULT_BI_INITIALIZER);
          if (used < limit) {
            used++;
          }
        }
        assertEquals(
            "history=" + history + ", step=" + step, used, session.getServerInitiatedStreamsBidi());
        org.junit.Assert.assertTrue(used <= session.getPeerSettingsMaxStreamsBidi());
      }
    }
  }
}
