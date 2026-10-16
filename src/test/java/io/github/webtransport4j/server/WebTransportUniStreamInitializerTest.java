package io.github.webtransport4j.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.DefaultChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.Attribute;
import java.lang.reflect.Method;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
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
}
