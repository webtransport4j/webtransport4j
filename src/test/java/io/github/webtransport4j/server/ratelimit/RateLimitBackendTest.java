package io.github.webtransport4j.server.ratelimit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import io.github.webtransport4j.server.IpRateLimitingHandler;
import io.github.webtransport4j.server.WebTransportConfig;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for rate limit backend implementations. */
public class RateLimitBackendTest {
  private RateLimitBackend originalBackend;

  @Before
  public void setUp() {
    originalBackend = IpRateLimitingHandler.getBackend();
    IpRateLimitingHandler.clearState();
  }

  @After
  public void tearDown() {
    IpRateLimitingHandler.setBackend(originalBackend);
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
    assertEquals(2, backend.incrementAndGet("10.0.0.1", minute, 1));
    // New IPs cannot erase an existing IP's count in the same minute.
    assertEquals(Integer.MAX_VALUE, backend.incrementAndGet("10.0.0.2", minute, 1));
    assertEquals(3, backend.incrementAndGet("10.0.0.1", minute, 1));
    assertEquals(1, backend.getTrackedIpsCount());
    assertEquals(1, backend.incrementAndGet("10.0.0.2", minute + 1, 1));
  }

  @Test(timeout = 10000)
  public void testConcurrentAdmissionPreservesCapacityAndCounts() throws Exception {
    LocalMemoryRateLimitBackend backend = new LocalMemoryRateLimitBackend();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Integer>> results = new ArrayList<>();
    try {
      for (int i = 0; i < 64; i++) {
        String ip = "10.0.0." + i;
        results.add(executor.submit(() -> {
          start.await();
          return backend.incrementAndGet(ip, 1000L, 4);
        }));
      }
      start.countDown();
      int admitted = 0;
      for (int i = 0; i < results.size(); i++) {
        int result = results.get(i).get(5, TimeUnit.SECONDS);
        if (result == 1) {
          admitted++;
          assertEquals(2, backend.incrementAndGet("10.0.0." + i, 1000L, 4));
        } else {
          assertEquals(Integer.MAX_VALUE, result);
        }
      }
      assertEquals(4, admitted);
      assertEquals(4, backend.getTrackedIpsCount());
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
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
    LocalMemoryRateLimitBackend backend1 = new LocalMemoryRateLimitBackend();
    LocalMemoryRateLimitBackend backend2 = new LocalMemoryRateLimitBackend();
    try (WebTransportServer server1 = new WebTransportServerBuilder().rateLimitBackend(backend1).build();
        WebTransportServer server2 = new WebTransportServerBuilder().rateLimitBackend(backend2).build()) {
      assertNotSame(server1.getIpRateLimitingHandler(), server2.getIpRateLimitingHandler());
      assertSame(backend1, server1.getIpRateLimitingHandler().getEffectiveBackend());
      assertSame(backend2, server2.getIpRateLimitingHandler().getEffectiveBackend());
    }
  }

  @Test
  public void testDefaultServersFollowConfiguredBackend() {
    LocalMemoryRateLimitBackend customBackend = new LocalMemoryRateLimitBackend();
    try (WebTransportServer server1 = new WebTransportServer();
        WebTransportServer server2 = new WebTransportServerBuilder().build()) {
      IpRateLimitingHandler.setBackend(customBackend);
      assertSame(customBackend, IpRateLimitingHandler.getBackend());
      assertSame(customBackend, server1.getIpRateLimitingHandler().getEffectiveBackend());
      assertSame(customBackend, server2.getIpRateLimitingHandler().getEffectiveBackend());
    }
  }

  @Test
  public void testClearAndReloadResetExplicitInstanceBackends() {
    LocalMemoryRateLimitBackend backend = new LocalMemoryRateLimitBackend();
    IpRateLimitingHandler handler = new IpRateLimitingHandler(backend);
    assertEquals(1, handler.getEffectiveBackend().incrementAndGet("10.0.0.1", 1000L, 1));
    IpRateLimitingHandler.clearState();
    assertEquals(1, handler.getEffectiveBackend().incrementAndGet("10.0.0.1", 1000L, 1));
    // reloadSharedConfig preserves active rate-limit counters to avoid DoS burst windows
    IpRateLimitingHandler.reloadSharedConfig();
    assertEquals(2, handler.getEffectiveBackend().incrementAndGet("10.0.0.1", 1000L, 1));
    // explicit clearState resets counters
    IpRateLimitingHandler.clearState();
    assertEquals(1, handler.getEffectiveBackend().incrementAndGet("10.0.0.1", 1000L, 1));
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
