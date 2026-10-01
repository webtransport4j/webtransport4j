package io.github.webtransport4j.resilience;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
  public void testCircuitBreakerHalfOpenRecovery() throws InterruptedException {
    final AdaptiveCircuitBreaker breaker = new AdaptiveCircuitBreaker(2, 30L);
    breaker.trip();
    assertEquals(AdaptiveCircuitBreaker.State.OPEN, breaker.getState());
    assertFalse(breaker.allowExecution());

    Thread.sleep(45L);

    // After reset timeout, next allowExecution transitions to HALF_OPEN
    assertTrue(breaker.allowExecution());
    assertEquals(AdaptiveCircuitBreaker.State.HALF_OPEN, breaker.getState());

    // Record success closes the circuit
    breaker.recordSuccess();
    assertEquals(AdaptiveCircuitBreaker.State.CLOSED, breaker.getState());
    assertTrue(breaker.allowExecution());
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

    // Capacity reached
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
