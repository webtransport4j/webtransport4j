package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPipeline;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.Attribute;
import java.lang.reflect.Method;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import io.netty.util.DefaultAttributeMap;
import org.junit.jupiter.api.Test;

/** Tests for {@link WebTransportUniStreamInitializer}. */
public class WebTransportUniStreamInitializerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testInitChannelAttachesTrafficShapersAndOmitsCapsuleHandler() throws Exception {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    GlobalTrafficShapingHandler globalShaper =
        new GlobalTrafficShapingHandler(executor, 1024 * 1024, 1024 * 1024);
    try {
      EmbeddedChannel dummy = new EmbeddedChannel();
      DefaultChannelPipeline pipeline = (DefaultChannelPipeline) dummy.pipeline();

      QuicStreamChannel stream = mock(QuicStreamChannel.class);
      QuicChannel parent = mock(QuicChannel.class);
      Channel grandparent = mock(Channel.class);
      when(stream.parent()).thenReturn(parent);
      when(parent.parent()).thenReturn(grandparent);
      when(stream.pipeline()).thenReturn(pipeline);

      Attribute<GlobalTrafficShapingHandler> globalAttr = mock(Attribute.class);
      when(globalAttr.get()).thenReturn(globalShaper);
      when(grandparent.attr(WebTransportAttributeKeys.GLOBAL_TRAFFIC_SHAPER)).thenReturn(globalAttr);

      Attribute<GlobalTrafficShapingHandler> connAttr = mock(Attribute.class);
      when(parent.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER)).thenReturn(connAttr);

      Attribute<Supplier<MessageDispatcher>> dispAttr = mock(Attribute.class);
      when(parent.attr(WebTransportAttributeKeys.MESSAGE_DISPATCHER_SUPPLIER)).thenReturn(dispAttr);

      WebTransportUniStreamInitializer initializer =
          new WebTransportUniStreamInitializer(WebTransportUtils.UNI_STREAM_TYPE);
      Method initMethod =
          WebTransportUniStreamInitializer.class.getDeclaredMethod(
              "initChannel", QuicStreamChannel.class);
      initMethod.setAccessible(true);
      initMethod.invoke(initializer, stream);

      assertNotNull(pipeline.get(GlobalTrafficShapingHandler.class));
      assertNotNull(pipeline.get(WebTransportUniStreamHeaderDecoder.class));
      assertFalse(pipeline.names().contains("WebTransportCapsuleHandler#0"));
      dummy.finishAndReleaseAll();
    } finally {
      globalShaper.release();
      executor.shutdownNow();
    }
  }
  public void testInitChannelAddsTrafficShapersAndOmitsCapsuleHandler() throws Exception {
    QuicChannel mockQuic = mock(QuicChannel.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);

    DefaultAttributeMap parentAttrs = new DefaultAttributeMap();
    DefaultAttributeMap streamAttrs = new DefaultAttributeMap();

    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrs.attr(inv.getArgument(0)));
    when(mockStream.attr(any())).thenAnswer(inv -> streamAttrs.attr(inv.getArgument(0)));
    when(mockStream.parent()).thenReturn(mockQuic);
    when(mockStream.streamId()).thenReturn(7L);

    ChannelPipeline pipeline = new EmbeddedChannel().pipeline();
    when(mockStream.pipeline()).thenReturn(pipeline);

    GlobalTrafficShapingHandler connShaper = mock(GlobalTrafficShapingHandler.class);
    parentAttrs.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER).set(connShaper);

    WebTransportUniStreamInitializer initializer =
        new WebTransportUniStreamInitializer(WebTransportUtils.UNI_STREAM_TYPE);
    initializer.initChannel(mockStream);

    assertNotNull(pipeline.get("conn-traffic-shaper"), "Connection traffic shaper must be attached");
    assertSame(connShaper, pipeline.get("conn-traffic-shaper"));

    assertNotNull(pipeline.get(WebTransportUniStreamHeaderDecoder.class), "WebTransportUniStreamHeaderDecoder must be attached");

    assertNull(pipeline.get(WebTransportCapsuleHandler.class), "WebTransportCapsuleHandler must NOT be attached to unidirectional stream pipeline");

    assertNotNull(pipeline.get(DefaultMessageDispatcher.class), "MessageDispatcher must be attached");
  }
}
