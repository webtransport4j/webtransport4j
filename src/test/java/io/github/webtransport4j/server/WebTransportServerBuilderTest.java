package io.github.webtransport4j.server;

import io.github.webtransport4j.api.NoOpWebTransportMetricsListener;
import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.server.ratelimit.LocalMemoryRateLimitBackend;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for WebTransportServerBuilder functionality. */
public class WebTransportServerBuilderTest {

  @Test
  public void testConflictingRateLimitConfigurationRejected() {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> WebTransportServer.builder()
            .ipRateLimitingHandler(new IpRateLimitingHandler())
            .rateLimitBackend(new LocalMemoryRateLimitBackend())
            .build());
  }

  @Test
  public void testCustomRateLimitHandlerRetained() {
    IpRateLimitingHandler handler = new IpRateLimitingHandler(new LocalMemoryRateLimitBackend());
    try (WebTransportServer server = WebTransportServer.builder().ipRateLimitingHandler(handler).build()) {
      Assertions.assertSame(handler, server.getIpRateLimitingHandler());
    }
  }

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

    Assertions.assertNotNull(server);
    Assertions.assertEquals(8443, server.getPort());
    Assertions.assertEquals("127.0.0.1", server.getHost());
    Assertions.assertFalse(server.isStarted());
    Assertions.assertNotNull(server.getHandler("/"));
    Assertions.assertNotNull(server.getHandler("/chat"));
  }

  @Test
  public void testBuilderTrafficShapingConfiguration() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .globalTrafficLimits(1024L, 2048L)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    Assertions.assertNotNull(server);
    Assertions.assertNull(server.getTrafficShaper()); // not started yet
  }

  @Test
  public void testBuilderSocketCountConfiguration() {
    WebTransportServer server =
        WebTransportServer.builder()
            .socketCount(4)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    Assertions.assertNotNull(server);
    Assertions.assertEquals(4, server.getSocketCount());
  }

  @Test
  public void testServerMultiSocketStartAndStop() throws Exception {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .socketCount(2)
            .defaultHandler(new WebTransportHandler() {})
            .build();

    Assertions.assertFalse(server.isStarted());
    server.start();

    Assertions.assertTrue(server.isStarted());
    Assertions.assertTrue(server.getPort() > 0);
    Assertions.assertTrue(server.getChannels().size() >= 1);
    Assertions.assertEquals(server.getChannels().size(), server.getSocketCount());

    server.stop();
    Assertions.assertFalse(server.isStarted());
  }

  @Test
  public void testBuilderByteBufAllocatorConfiguration() {
    io.netty.buffer.ByteBufAllocator allocator = io.netty.buffer.PooledByteBufAllocator.DEFAULT;
    WebTransportServerBuilder builder = new WebTransportServerBuilder().byteBufAllocator(allocator);
    Assertions.assertSame(allocator, builder.getByteBufAllocator());
  }

  @Test
  public void testByteBufAllocatorExplicitOverride() {
    io.netty.buffer.ByteBufAllocator customAlloc = io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .byteBufAllocator(customAlloc)
            .build();

    Assertions.assertSame(customAlloc, server.getByteBufAllocator());
  }

  @Test
  public void testConfiguredDefaultAllocatorInBothModes() {
    String key = "webtransport4j.server.allocator";
    String previous = System.getProperty(key);
    try {
      System.setProperty(key, "DeFaUlT");
      Assertions.assertNull(WebTransportServer.builder()
          .build().getByteBufAllocator());
      Assertions.assertSame(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
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

    Assertions.assertSame(
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
      Assertions.assertNotNull(origins);
      Assertions.assertTrue(origins.isEmpty(), "Production default allowed origins must be empty");
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
      Assertions.assertNotNull(in, "webtransport.properties must exist");
      props.load(in);
    }

    String allowedOrigins = props.getProperty("webtransport4j.allowed.origins", "");
    Assertions.assertNotEquals("*", allowedOrigins.trim(), "webtransport4j.allowed.origins must not default to * in production config");

    String overrides = props.getProperty("webtransport4j.server.ratelimit.overrides", "");
    Assertions.assertFalse(overrides.contains("10.0.0.1:0"), "Default overrides must not blackhole 10.0.0.1");

    String blocklist = props.getProperty("webtransport4j.server.ratelimit.blocklist", "");
    Assertions.assertFalse(blocklist.contains("9.9.9.9"), "Default blocklist must not contain public DNS IP 9.9.9.9");
  }
}
