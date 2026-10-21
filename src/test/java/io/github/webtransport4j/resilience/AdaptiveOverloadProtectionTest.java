package io.github.webtransport4j.resilience;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.concurrency.ConcurrencySupport;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

  @Timeout(value = 10000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
  @Test
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
      assertEquals(1, admitted, "Recovery must admit exactly one probe");
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

  @Test
  public void testTripResetsFailureCount() {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(5, 500L);
    breaker.recordFailure();
    breaker.recordFailure();
    assertEquals(2, breaker.getFailureCount());

    breaker.trip();
    assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());
    assertEquals(0, breaker.getFailureCount());
  }

  @Test
  public void testHeapOverloadSheddingAndCustomSupplier() {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(3, 500L);
    final AdaptiveOverloadProtectionPolicy policy =
        new AdaptiveOverloadProtectionPolicy(0.80, 100, 5, breaker, () -> 0.85);

    OverloadProtectionPolicy.AdmissionResult result = policy.tryAcquire(10);
    assertFalse(result.isAdmitted());
    assertEquals(5, result.getRetryAfterSeconds());
    assertNotNull(result.getReason());
    assertTrue(result.getReason().contains("JVM heap memory pressure"));

    final AdaptiveOverloadProtectionPolicy healthyPolicy =
        new AdaptiveOverloadProtectionPolicy(0.80, 100, 5, breaker, () -> 0.40);
    result = healthyPolicy.tryAcquire(10);
    assertTrue(result.isAdmitted());
  }

  @Test
  public void testTryAcquireDoesNotPrematurelyCloseHalfOpenCircuitBreaker() throws Exception {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(2, 20L);
    breaker.trip();
    assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());

    expireCoolOff(breaker);

    final AdaptiveOverloadProtectionPolicy policy =
        new AdaptiveOverloadProtectionPolicy(0.99, 100, 5, breaker, () -> 0.10);

    // Admission check probe transitions OPEN to HALF_OPEN and is admitted,
    // but MUST NOT prematurely close the circuit breaker to CLOSED
    OverloadProtectionPolicy.AdmissionResult result = policy.tryAcquire(5);
    assertTrue(result.isAdmitted());
    assertEquals(AdaptiveCircuitBreaker.State.HALF_OPEN, breaker.getState());

    // Subsequent acquire before probe completes must be rejected
    OverloadProtectionPolicy.AdmissionResult secondResult = policy.tryAcquire(5);
    assertFalse(secondResult.isAdmitted());

    // Successful handshake completion closes the circuit
    policy.recordSuccess();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
    assertTrue(policy.tryAcquire(5).isAdmitted());
  }

  /**
   * Verifies that heap utilization stays within the supported admission threshold range.
   */
  @Test
  public void testCalculateHeapUsageRatioValidRange() {
    double ratio = AdaptiveOverloadProtectionPolicy.calculateHeapUsageRatio();
    assertTrue(ratio >= 0.0, "Heap ratio must be >= 0.0");
    assertTrue(ratio <= 1.0, "Heap ratio must be <= 1.0");
  }

  /**
   * Verifies that a failed handshake reopens a half-open circuit and a later probe can recover.
   *
   * @throws Exception if the test cannot expire the circuit breaker's cool-off timestamp
   */
  @Test
  public void testHalfOpenProbeFailureReturnsToOpenAndRecovers() throws Exception {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(2, 20L);
    breaker.trip();
    expireCoolOff(breaker);

    final AdaptiveOverloadProtectionPolicy policy =
        new AdaptiveOverloadProtectionPolicy(0.99, 100, 5, breaker, () -> 0.10);

    // Probe transitions OPEN -> HALF_OPEN
    assertTrue(policy.tryAcquire(5).isAdmitted());
    assertEquals(AdaptiveCircuitBreaker.State.HALF_OPEN, breaker.getState());

    // Handshake failure or rejection notifies recordFailure() -> transitions back to OPEN
    policy.recordFailure();
    assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());
    assertFalse(policy.tryAcquire(5).isAdmitted());

    // After cool-off expires again, a new probe can succeed and close the circuit
    expireCoolOff(breaker);
    assertTrue(policy.tryAcquire(5).isAdmitted());
    policy.recordSuccess();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
  }

  /**
   * Verifies that an incomplete probe permits a replacement after the reset timeout, then recovers.
   *
   * @throws Exception if the test cannot expire the circuit breaker's cool-off timestamp
   */
  @Test
  public void testHalfOpenStalledProbeTimeoutAllowsNewProbe() throws Exception {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(2, 50L);
    breaker.trip();
    expireCoolOff(breaker);

    assertTrue(breaker.allowExecution());
    assertEquals(AdaptiveCircuitBreaker.State.HALF_OPEN, breaker.getState());
    assertFalse(breaker.allowExecution());

    // If the probe stalls beyond resetTimeoutMillis without recording completion,
    // allowExecution permits a single replacement probe instead of deadlocking forever
    expireCoolOff(breaker);
    assertTrue(breaker.allowExecution());
    assertFalse(breaker.allowExecution());

    breaker.recordSuccess();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
  }
}
