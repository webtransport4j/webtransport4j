package io.github.webtransport4j.resilience;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;

/**
 * Thread-safe adaptive circuit breaker for protecting WebTransport server resources under load.
 */
public class AdaptiveCircuitBreaker {

  /**
   * Operational state of the circuit breaker.
   */
  public enum State {
    /** Normal operation; requests are permitted. */
    CLOSED,
    /** Tripped open; requests are shed immediately. */
    OPEN,
    /** Testing recovery; only the caller that transitions from OPEN runs a probe. */
    HALF_OPEN
  }

  private final int failureThreshold;
  private final long resetTimeoutMillis;
  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicLong lastStateChangeTimestamp = new AtomicLong(System.currentTimeMillis());

  /**
   * Constructs a circuit breaker with specified failure threshold and cool-off timeout.
   *
   * @param failureThreshold consecutive failures before tripping open
   * @param resetTimeoutMillis milliseconds to wait in OPEN state before transitioning to HALF_OPEN
   */
  public AdaptiveCircuitBreaker(int failureThreshold, long resetTimeoutMillis) {
    if (failureThreshold <= 0) {
      throw new IllegalArgumentException("failureThreshold must be positive: " + failureThreshold);
    }
    if (resetTimeoutMillis <= 0) {
      throw new IllegalArgumentException("resetTimeoutMillis must be positive: " + resetTimeoutMillis);
    }
    this.failureThreshold = failureThreshold;
    this.resetTimeoutMillis = resetTimeoutMillis;
  }

  /**
   * Evaluates if a request is permitted to proceed according to the breaker state.
   *
   * <p>After the cool-off period, only the caller that transitions OPEN to HALF_OPEN is
   * permitted. That caller must record success or failure before another probe can run.
   *
   * @return true if execution is permitted
   */
  public boolean allowExecution() {
    final State current = state.get();
    if (current == State.CLOSED) {
      return true;
    }
    if (current == State.OPEN) {
      final long now = System.currentTimeMillis();
      if (now - lastStateChangeTimestamp.get() >= resetTimeoutMillis) {
        if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
          lastStateChangeTimestamp.set(now);
          return true;
        }
      }
      return false;
    }
    return false; // The OPEN-to-HALF_OPEN CAS winner owns the outstanding probe.
  }

  /**
   * Records a successful operation, resetting failures and closing half-open circuits.
   */
  public void recordSuccess() {
    failureCount.set(0);
    if (state.get() == State.HALF_OPEN) {
      state.set(State.CLOSED);
      lastStateChangeTimestamp.set(System.currentTimeMillis());
    }
  }

  /**
   * Records an operation failure or overload condition, potentially tripping the circuit open.
   */
  public void recordFailure() {
    final int failures = failureCount.incrementAndGet();
    if (failures >= failureThreshold || state.get() == State.HALF_OPEN) {
      trip();
    }
  }

  /**
   * Explicitly forces the circuit breaker into the OPEN state.
   */
  public void trip() {
    state.set(State.OPEN);
    lastStateChangeTimestamp.set(System.currentTimeMillis());
  }

  /**
   * Explicitly resets the circuit breaker to the CLOSED state.
   */
  public void reset() {
    failureCount.set(0);
    state.set(State.CLOSED);
    lastStateChangeTimestamp.set(System.currentTimeMillis());
  }

  /**
   * Returns the current operational state of the circuit breaker.
   *
   * @return current state
   */
  public @NonNull State getState() {
    return state.get();
  }

  /**
   * Returns the configured failure threshold.
   *
   * @return failure threshold
   */
  public int getFailureThreshold() {
    return failureThreshold;
  }

  /**
   * Returns the reset timeout in milliseconds.
   *
   * @return reset timeout in milliseconds
   */
  public long getResetTimeoutMillis() {
    return resetTimeoutMillis;
  }
}
