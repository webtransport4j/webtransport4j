package io.github.webtransport4j.resilience;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.concurrency.ConcurrencySupport;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Tests for {@link AdaptiveCircuitBreaker} and {@link AdaptiveOverloadProtectionPolicy}.
 */
public class AdaptiveOverloadProtectionTest {

  @Test
  public void testCircuitBreakerTrippingAndReset() {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(3, 50L);
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
    assertTrue(breaker.allowExecution());

    breaker.recordFailure();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
    assertTrue(breaker.allowExecution());

    breaker.recordFailure();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());

    // 3rd failure trips the breaker
    breaker.recordFailure();
    assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());
    assertFalse(breaker.allowExecution());

    // Manual reset
    breaker.reset();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
    assertTrue(breaker.allowExecution());
  }

  @Test
  public void testCircuitBreakerHalfOpenRecovery() throws Exception {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(2, 30L);
    breaker.trip();
    assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());
    assertFalse(breaker.allowExecution());

    expireCoolOff(breaker);

    // After reset timeout, next allowExecution transitions to HALF_OPEN
    assertTrue(breaker.allowExecution());
    assertEquals(AdaptiveCircuitBreaker.State.HALF_OPEN, breaker.getState());
    assertFalse(breaker.allowExecution());

    // Record success closes the circuit
    breaker.recordSuccess();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
    assertTrue(breaker.allowExecution());
  }

  @Test(timeout = 10000)
  public void testOnlyOneConcurrentRecoveryProbeIsAdmitted() throws Exception {
    AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(2, 1000L);
    breaker.trip();
    expireCoolOff(breaker);
    int callers = 16;
    ExecutorService executor = Executors.newFixedThreadPool(callers);
    CountDownLatch ready = new CountDownLatch(callers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Boolean>> results = new ArrayList<>();
    try {
      for (int i = 0; i < callers; i++) {
        results.add(executor.submit(() -> {
          ready.countDown();
          ConcurrencySupport.await(start);
          return breaker.allowExecution();
        }));
      }
      ConcurrencySupport.await(ready);
      start.countDown();
      int admitted = 0;
      for (Future<Boolean> result : results) {
        if (result.get(5, TimeUnit.SECONDS)) {
          admitted++;
        }
      }
      assertEquals("Recovery must admit exactly one probe", 1, admitted);
      assertFalse(breaker.allowExecution());
      breaker.recordFailure();
      assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());
      assertFalse(breaker.allowExecution());
      expireCoolOff(breaker);
      assertTrue(breaker.allowExecution());
      assertFalse(breaker.allowExecution());
      breaker.recordSuccess();
      assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
      assertTrue(breaker.allowExecution());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void expireCoolOff(AdaptiveCircuitBreaker breaker) throws Exception {
    ConcurrencySupport.replace(
        breaker, "lastStateChangeTimestamp", System.currentTimeMillis() - breaker.getResetTimeoutMillis());
  }

  @Test
  public void testAdaptiveOverloadProtectionAdmission() {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(3, 1000L);
    final AdaptiveOverloadProtectionPolicy policy =
        new AdaptiveOverloadProtectionPolicy(0.99, 100, 5, breaker);

    // Within limit
    final OverloadProtectionPolicy.AdmissionResult result = policy.tryAcquire(50);
    assertTrue(result.isAdmitted());
    assertEquals(0, result.getRetryAfterSeconds());
    assertNull(result.getReason());

    // The count excludes the request under evaluation: the 100th session is admitted.
    assertTrue(policy.tryAcquire(99).isAdmitted());

    // Capacity reached before evaluating the next request.
    final OverloadProtectionPolicy.AdmissionResult rejected = policy.tryAcquire(100);
    assertFalse(rejected.isAdmitted());
    assertEquals(5, rejected.getRetryAfterSeconds());
    assertNotNull(rejected.getReason());
    assertTrue(rejected.getReason().contains("Maximum concurrent session capacity"));
  }

  @Test
  public void testCircuitBreakerTripsAfterSheddingFailures() {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(2, 500L);
    final AdaptiveOverloadProtectionPolicy policy =
        new AdaptiveOverloadProtectionPolicy(0.99, 10, 3, breaker);

    // Trigger 2 capacity violations
    policy.tryAcquire(15);
    policy.tryAcquire(15);

    assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());

    // Next acquire rejected due to open breaker
    final OverloadProtectionPolicy.AdmissionResult result = policy.tryAcquire(1);
    assertFalse(result.isAdmitted());
    assertTrue(result.getReason().contains("Circuit breaker is OPEN"));
  }
}
