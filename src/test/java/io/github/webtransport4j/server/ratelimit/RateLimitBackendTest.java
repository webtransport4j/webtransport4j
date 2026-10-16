package io.github.webtransport4j.server.ratelimit;

import static org.junit.Assert.assertEquals;

import io.github.webtransport4j.server.IpRateLimitingHandler;
import io.github.webtransport4j.server.WebTransportConfig;
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
    // Exceeding capacity returns MAX_VALUE signal
    assertEquals(Integer.MAX_VALUE, backend.incrementAndGet("10.0.0.2", minute, 1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRedisBackendConfigRejection() throws Exception {
    System.setProperty("webtransport4j.server.ratelimit.backend", "redis");
    try {
      WebTransportConfig.reload();
      java.lang.reflect.Method m = IpRateLimitingHandler.class.getDeclaredMethod("createBackend");
      m.setAccessible(true);
      try {
        m.invoke(null);
      } catch (java.lang.reflect.InvocationTargetException ite) {
        if (ite.getCause() instanceof RuntimeException) {
          throw (RuntimeException) ite.getCause();
        }
        throw new RuntimeException(ite.getCause());
      }
    } finally {
      System.clearProperty("webtransport4j.server.ratelimit.backend");
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testIpRateLimitingHandlerBackendSetter() {
    LocalMemoryRateLimitBackend customBackend = new LocalMemoryRateLimitBackend();
    IpRateLimitingHandler.setBackend(customBackend);
    assertEquals(customBackend, IpRateLimitingHandler.getBackend());
  }
}
