package io.github.webtransport4j.server.ratelimit;

import static org.junit.Assert.assertEquals;

import io.github.webtransport4j.server.IpRateLimitingHandler;
import org.junit.Before;
import org.junit.Test;

/** Tests for rate limit backend implementations. */
public class RateLimitBackendTest {

  @Before
  public void setUp() {
    IpRateLimitingHandler.clearState();
  }

  @Test
  public void testLocalMemoryRateLimitBackend() {
    LocalMemoryRateLimitBackend backend = new LocalMemoryRateLimitBackend();
    long minute = 1000L;

    assertEquals(1, backend.incrementAndGet("192.168.1.1", minute, 100));
    assertEquals(2, backend.incrementAndGet("192.168.1.1", minute, 100));
    assertEquals(3, backend.incrementAndGet("192.168.1.1", minute, 100));

    // Next minute bucket resets count
    assertEquals(1, backend.incrementAndGet("192.168.1.1", minute + 1, 100));
  }

  @Test
  public void testLocalMemoryRateLimitCapacityBound() {
    LocalMemoryRateLimitBackend backend = new LocalMemoryRateLimitBackend();
    long minute = 1000L;

    assertEquals(1, backend.incrementAndGet("10.0.0.1", minute, 1));
    assertEquals(1, backend.getTrackedIpsCount());
    // Exceeding capacity evicts LRU entry rather than hard-dropping new IPs with MAX_VALUE
    assertEquals(1, backend.incrementAndGet("10.0.0.2", minute, 1));
    assertEquals(1, backend.getTrackedIpsCount());
  }

  @Test
  public void testMinuteRolloverAtomicSwap() {
    LocalMemoryRateLimitBackend backend = new LocalMemoryRateLimitBackend();
    long minute1 = 1000L;
    long minute2 = 1001L;

    assertEquals(1, backend.incrementAndGet("10.0.0.1", minute1, 100));
    assertEquals(2, backend.incrementAndGet("10.0.0.1", minute1, 100));

    // Rollover to minute2
    assertEquals(1, backend.incrementAndGet("10.0.0.1", minute2, 100));
    assertEquals(1, backend.getTrackedIpsCount());
  }

  @Test
  public void testServerInstanceRateLimitIsolation() {
    io.github.webtransport4j.server.WebTransportServer server1 =
        new io.github.webtransport4j.server.WebTransportServerBuilder().port(0).build();
    io.github.webtransport4j.server.WebTransportServer server2 =
        new io.github.webtransport4j.server.WebTransportServerBuilder().port(0).build();

    org.junit.Assert.assertNotSame(
        server1.getIpRateLimitingHandler(), server2.getIpRateLimitingHandler());
    org.junit.Assert.assertNotSame(
        server1.getIpRateLimitingHandler().getEffectiveBackend(),
        server2.getIpRateLimitingHandler().getEffectiveBackend());
  }

  @Test
  public void testRedisRateLimitBackendWithCustomFunction() {
    RedisRateLimitBackend redisBackend =
        new RedisRateLimitBackend(
            (key, min) -> {
              if (key.contains("1.1.1.1")) {
                return 42;
              }
              return null; // trigger fallback
            },
            "test:ratelimit:");

    assertEquals(42, redisBackend.incrementAndGet("1.1.1.1", 2000L, 100));
    assertEquals(1, redisBackend.incrementAndGet("2.2.2.2", 2000L, 100)); // Fell back to local
  }

  @Test
  public void testIpRateLimitingHandlerBackendSetter() {
    LocalMemoryRateLimitBackend customBackend = new LocalMemoryRateLimitBackend();
    IpRateLimitingHandler.setBackend(customBackend);
    assertEquals(customBackend, IpRateLimitingHandler.getBackend());
  }
}
