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
  public void testBuilderSocketCountConfiguration() {
    WebTransportServer server =
        WebTransportServer.builder()
            .socketCount(4)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    Assert.assertNotNull(server);
    Assert.assertEquals(4, server.getSocketCount());
  }

  @Test
  public void testServerMultiSocketStartAndStop() throws Exception {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .socketCount(2)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    Assert.assertFalse(server.isStarted());
    server.start();

    Assert.assertTrue(server.isStarted());
    Assert.assertTrue(server.getPort() > 0);
    Assert.assertTrue(server.getChannels().size() >= 1);
    Assert.assertEquals(server.getChannels().size(), server.getSocketCount());

    server.stop();
    Assert.assertFalse(server.isStarted());
  }

  @Test
  public void testBuilderByteBufAllocatorConfiguration() {
    io.netty.buffer.ByteBufAllocator allocator = io.netty.buffer.PooledByteBufAllocator.DEFAULT;
    WebTransportServerBuilder builder = new WebTransportServerBuilder().byteBufAllocator(allocator);
    Assert.assertSame(allocator, builder.getByteBufAllocator());
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
      Assert.assertNull(WebTransportServer.builder()
          .build().getByteBufAllocator());
      Assert.assertSame(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
          WebTransportServer.builder()
              .byteBufAllocator(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT)
              .build().getByteBufAllocator());
    } finally {
      if (previous == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, previous);
      }
    }
  }

  @Test
  public void testByteBufAllocatorDefaultsToPooled() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .build();

    Assert.assertSame(
        io.netty.buffer.PooledByteBufAllocator.DEFAULT, server.getByteBufAllocator());
  }

  @Test
  public void testAllowedOriginsProductionDefault() {
    String prevProfile = System.getProperty("webtransport4j.profile");
    String prevOrigins = System.getProperty("webtransport4j.allowed.origins");
    try {
      System.setProperty("webtransport4j.profile", "prod");
      System.setProperty("webtransport4j.allowed.origins", "");
      WebTransportConfig.clearProfileCache();

      WebTransportServer server = WebTransportServer.builder().port(0).build();
      java.util.List<String> origins = server.resolveAllowedOrigins();
      Assert.assertNotNull(origins);
      Assert.assertTrue("Production default allowed origins must be empty", origins.isEmpty());
    } finally {
      if (prevProfile != null) {
        System.setProperty("webtransport4j.profile", prevProfile);
      } else {
        System.clearProperty("webtransport4j.profile");
      }
      if (prevOrigins != null) {
        System.setProperty("webtransport4j.allowed.origins", prevOrigins);
      } else {
        System.clearProperty("webtransport4j.allowed.origins");
      }
      WebTransportConfig.clearProfileCache();
    }
  }

  @Test
  public void testPackagedPropertiesDoNotContainInsecureDefaults() throws Exception {
    java.util.Properties props = new java.util.Properties();
    try (java.io.InputStream in =
        getClass().getClassLoader().getResourceAsStream("webtransport.properties")) {
      Assert.assertNotNull("webtransport.properties must exist", in);
      props.load(in);
    }

    String allowedOrigins = props.getProperty("webtransport4j.allowed.origins", "");
    Assert.assertNotEquals(
        "webtransport4j.allowed.origins must not default to * in production config",
        "*",
        allowedOrigins.trim());

    String overrides = props.getProperty("webtransport4j.server.ratelimit.overrides", "");
    Assert.assertFalse(
        "Default overrides must not blackhole 10.0.0.1",
        overrides.contains("10.0.0.1:0"));

    String blocklist = props.getProperty("webtransport4j.server.ratelimit.blocklist", "");
    Assert.assertFalse(
        "Default blocklist must not contain public DNS IP 9.9.9.9",
        blocklist.contains("9.9.9.9"));
  }
}
