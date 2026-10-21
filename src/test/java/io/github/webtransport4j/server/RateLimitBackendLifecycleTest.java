package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.webtransport4j.server.ratelimit.LocalMemoryRateLimitBackend;
import io.github.webtransport4j.server.ratelimit.RateLimitBackend;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Rate limit resets follow server ownership across stop and restart. */
public class RateLimitBackendLifecycleTest {
  @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  @Test
  public void stoppingServerResetsOnlyItsBackendAcrossRestart() throws Exception {
    LocalMemoryRateLimitBackend first = new LocalMemoryRateLimitBackend();
    LocalMemoryRateLimitBackend second = new LocalMemoryRateLimitBackend();
    try (WebTransportServer server1 = serverBuilder().rateLimitBackend(first).build();
        WebTransportServer server2 = serverBuilder().rateLimitBackend(second).build()) {
      server1.start();
      server2.start();
      assertEquals(1, first.incrementAndGet("1.1.1.1", 1000L, 10));
      assertEquals(1, second.incrementAndGet("1.1.1.1", 1000L, 10));

      server1.stop();
      assertEquals(0, first.getTrackedIpsCount());
      assertEquals(2, second.incrementAndGet("1.1.1.1", 1000L, 10));
      server1.start();
      assertEquals(1, first.incrementAndGet("1.1.1.1", 1000L, 10));
      assertEquals(3, second.incrementAndGet("1.1.1.1", 1000L, 10));

      server2.stop();
      assertEquals(0, second.getTrackedIpsCount());
      assertEquals(2, first.incrementAndGet("1.1.1.1", 1000L, 10));
    }
  }

  @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  @Test
  public void sharedHandlerRetainsCountsUntilLastServerStops() throws Exception {
    LocalMemoryRateLimitBackend backend = new LocalMemoryRateLimitBackend();
    IpRateLimitingHandler handler = new IpRateLimitingHandler(backend);
    try (WebTransportServer server1 = serverBuilder().ipRateLimitingHandler(handler).build();
        WebTransportServer server2 = serverBuilder().ipRateLimitingHandler(handler).build()) {
      assertSharedBackendLifecycle(server1, server2, backend);
    }
  }

  @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  @Test
  public void defaultServersRetainConfiguredBackendUntilLastServerStops() throws Exception {
    RateLimitBackend original = IpRateLimitingHandler.getBackend();
    LocalMemoryRateLimitBackend backend = new LocalMemoryRateLimitBackend();
    try {
      IpRateLimitingHandler.setBackend(backend);
      try (WebTransportServer server1 = serverBuilder().build();
          WebTransportServer server2 = serverBuilder().build()) {
        assertSharedBackendLifecycle(server1, server2, backend);
      }
    } finally {
      IpRateLimitingHandler.setBackend(original);
    }
  }

  private static void assertSharedBackendLifecycle(
      WebTransportServer server1, WebTransportServer server2, LocalMemoryRateLimitBackend backend)
      throws Exception {
    server1.start();
    server2.start();
    assertEquals(1, backend.incrementAndGet("1.1.1.1", 1000L, 10));
    server1.stop();
    assertEquals(2, backend.incrementAndGet("1.1.1.1", 1000L, 10));
    server1.start();
    assertEquals(3, backend.incrementAndGet("1.1.1.1", 1000L, 10));
    server1.stop();
    assertEquals(4, backend.incrementAndGet("1.1.1.1", 1000L, 10));
    server2.stop();
    assertEquals(0, backend.getTrackedIpsCount());
    server2.start();
    assertEquals(1, backend.incrementAndGet("1.1.1.1", 1000L, 10));
  }

  private static WebTransportServerBuilder serverBuilder() {
    return WebTransportServer.builder().port(0).transportType("nio").socketCount(1);
  }
}
