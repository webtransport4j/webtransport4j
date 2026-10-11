package io.github.webtransport4j.server;

import io.github.webtransport4j.api.NoOpWebTransportMetricsListener;
import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/** Test for WebTransportServerBuilder functionality. */
public class WebTransportServerBuilderTest {

  @Test
  public void testBuilderConfigurationDefaultsAndOverrides() {
    WebTransportHandler customHandler = new WebTransportHandler() {};
    ReactiveWebTransportHandler customReactiveHandler = new ReactiveWebTransportHandler() {};

    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .host("127.0.0.1")
            .ssl("key.pem", "cert.pem")
            .allowedOrigins("https://example.com", "https://localhost")
            .transportType("nio")
            .idleTimeout(30, TimeUnit.SECONDS)
            .maxStreams(50, 50)
            .maxData(100000)
            .metricsListener(NoOpWebTransportMetricsListener.INSTANCE)
            .quicTokenHandler(InsecureQuicTokenHandler.INSTANCE)
            .defaultHandler(customHandler)
            .reactiveHandler("/chat", customReactiveHandler)
            .build();

    Assert.assertNotNull(server);
    Assert.assertEquals(8443, server.getPort());
    Assert.assertEquals("127.0.0.1", server.getHost());
    Assert.assertFalse(server.isStarted());
    Assert.assertNotNull(server.getHandler("/"));
    Assert.assertNotNull(server.getHandler("/chat"));
  }

  @Test
  public void testBuilderTrafficShapingConfiguration() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .globalTrafficLimits(1024L, 2048L)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    Assert.assertNotNull(server);
    Assert.assertNull(server.getTrafficShaper()); // not started yet
  }

  @Test
  public void testByteBufAllocatorExplicitOverride() {
    io.netty.buffer.ByteBufAllocator customAlloc = io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .byteBufAllocator(customAlloc)
            .build();

    Assert.assertSame(customAlloc, server.getByteBufAllocator());
  }

  @Test
  public void testConfiguredDefaultAllocatorInBothModes() {
    String key = "webtransport4j.server.allocator";
    String previous = System.getProperty(key);
    try {
      System.setProperty(key, "DeFaUlT");
      for (boolean zeroGc : new boolean[] {false, true}) {
        Assert.assertNull(WebTransportServer.builder().enableZeroGc(zeroGc)
            .build().getByteBufAllocator());
        Assert.assertSame(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
            WebTransportServer.builder().enableZeroGc(zeroGc)
                .byteBufAllocator(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT)
                .build().getByteBufAllocator());
      }
    } finally {
      if (previous == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, previous);
      }
    }
  }

  @Test
  public void testByteBufAllocatorZeroGcDefaultsToPooled() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .enableZeroGc(true)
            .build();

    Assert.assertSame(
        io.netty.buffer.PooledByteBufAllocator.DEFAULT, server.getByteBufAllocator());
  }
}
