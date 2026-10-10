package io.github.webtransport4j.resilience;

import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import io.github.webtransport4j.internal.handles.LongHandle;
import io.github.webtransport4j.internal.handles.RefHandle;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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

  private static final RefHandle<AdaptiveCircuitBreaker, State> STATE_HANDLE =
      Handles.newRefHandle(
          AdaptiveCircuitBreaker.class,
          State.class,
          "state",
          MethodHandles.lookup(),
          () -> AtomicReferenceFieldUpdater.newUpdater(AdaptiveCircuitBreaker.class, State.class, "state"));

  private static final IntHandle<AdaptiveCircuitBreaker> FAILURE_COUNT_HANDLE =
      Handles.newIntHandle(
          AdaptiveCircuitBreaker.class,
          "failureCount",
          MethodHandles.lookup(),
          () -> AtomicIntegerFieldUpdater.newUpdater(AdaptiveCircuitBreaker.class, "failureCount"));

  private static final LongHandle<AdaptiveCircuitBreaker> LAST_STATE_CHANGE_HANDLE =
      Handles.newLongHandle(
          AdaptiveCircuitBreaker.class,
          "lastStateChangeTimestamp",
          MethodHandles.lookup(),
          () ->
              AtomicLongFieldUpdater.newUpdater(
                  AdaptiveCircuitBreaker.class, "lastStateChangeTimestamp"));

  private final int failureThreshold;
  private final long resetTimeoutMillis;
  private volatile State state = State.CLOSED;
  private volatile int failureCount = 0;
  private volatile long lastStateChangeTimestamp = System.currentTimeMillis();

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
    final State current = this.state;
    if (current == State.CLOSED) {
      return true;
    }
    if (current == State.OPEN) {
      final long now = System.currentTimeMillis();
      if (now - this.lastStateChangeTimestamp >= resetTimeoutMillis) {
        if (STATE_HANDLE.compareAndSet(this, State.OPEN, State.HALF_OPEN)) {
          this.lastStateChangeTimestamp = now;
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
    this.failureCount = 0;
    if (this.state == State.HALF_OPEN) {
      this.state = State.CLOSED;
      this.lastStateChangeTimestamp = System.currentTimeMillis();
    }
  }

  /**
   * Records an operation failure or overload condition, potentially tripping the circuit open.
   */
  public void recordFailure() {
    final int failures = FAILURE_COUNT_HANDLE.incrementAndGet(this);
    if (failures >= failureThreshold || this.state == State.HALF_OPEN) {
      trip();
    }
  }

  /**
   * Explicitly forces the circuit breaker into the OPEN state.
   */
  public void trip() {
    this.state = State.OPEN;
    this.lastStateChangeTimestamp = System.currentTimeMillis();
  }

  /**
   * Explicitly resets the circuit breaker to the CLOSED state.
   */
  public void reset() {
    this.failureCount = 0;
    this.state = State.CLOSED;
    this.lastStateChangeTimestamp = System.currentTimeMillis();
  }

  /**
   * Returns the current operational state of the circuit breaker.
   *
   * @return current state
   */
  public @NonNull State getState() {
    return this.state;
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
