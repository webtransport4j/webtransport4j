package io.github.webtransport4j.server.ratelimit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.server.IpRateLimitingHandler;
import io.github.webtransport4j.server.WebTransportConfig;
import org.junit.Before;
import org.junit.Test;

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
    // Exceeding capacity returns MAX_VALUE signal
    assertEquals(Integer.MAX_VALUE, backend.incrementAndGet("10.0.0.2", minute, 1));
  }

  @Test
  public void testRedisRateLimitBackendWithCustomFunction() {
    RedisRateLimitBackend redisBackend = new RedisRateLimitBackend((key, min) -> {
      if (key.contains("1.1.1.1")) {
        return 42;
      }
      return null; // trigger fallback
    }, "test:ratelimit:");

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
