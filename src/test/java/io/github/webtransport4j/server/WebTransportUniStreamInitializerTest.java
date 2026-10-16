package io.github.webtransport4j.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.DefaultChannelPipeline;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
import org.junit.Test;

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
      when(stream.parent()).thenReturn(parent);
      when(stream.pipeline()).thenReturn(pipeline);

      Attribute<GlobalTrafficShapingHandler> globalAttr = mock(Attribute.class);
      when(globalAttr.get()).thenReturn(globalShaper);
      when(parent.attr(WebTransportAttributeKeys.GLOBAL_TRAFFIC_SHAPER)).thenReturn(globalAttr);

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

    assertNotNull(
        "Connection traffic shaper must be attached",
        pipeline.get("conn-traffic-shaper"));
    assertSame(connShaper, pipeline.get("conn-traffic-shaper"));

    assertNotNull(
        "WebTransportUniStreamHeaderDecoder must be attached",
        pipeline.get(WebTransportUniStreamHeaderDecoder.class));

    assertNull(
        "WebTransportCapsuleHandler must NOT be attached to unidirectional stream pipeline",
        pipeline.get(WebTransportCapsuleHandler.class));

    assertNotNull(
        "MessageDispatcher must be attached",
        pipeline.get(DefaultMessageDispatcher.class));
  }
}
