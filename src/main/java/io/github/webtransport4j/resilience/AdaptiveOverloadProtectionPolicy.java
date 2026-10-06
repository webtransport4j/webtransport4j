package io.github.webtransport4j.resilience;

import java.util.Objects;
import org.jspecify.annotations.NonNull;

/**
 * Adaptive overload protection policy that monitors JVM heap utilization, active session limits,
 * and circuit breaker health to safeguard against cascade server failures.
 */
public class AdaptiveOverloadProtectionPolicy implements OverloadProtectionPolicy {

  /** Default maximum heap ratio before load shedding (85%). */
  public static final double DEFAULT_MAX_HEAP_RATIO = 0.85;

  /** Default maximum concurrent sessions. */
  public static final int DEFAULT_MAX_ACTIVE_SESSIONS = 50_000;

  /** Default recommended retry delay in seconds. */
  public static final int DEFAULT_RETRY_AFTER_SECONDS = 5;

  private final double maxHeapUsageRatio;
  private final int maxActiveSessions;
  private final int retryAfterSeconds;
  private final AdaptiveCircuitBreaker circuitBreaker;

  /**
   * Constructs an overload protection policy with default thresholds.
   */
  public AdaptiveOverloadProtectionPolicy() {
    this(DEFAULT_MAX_HEAP_RATIO, DEFAULT_MAX_ACTIVE_SESSIONS, DEFAULT_RETRY_AFTER_SECONDS);
  }

  /**
   * Constructs an overload protection policy with specified thresholds.
   *
   * @param maxHeapUsageRatio maximum fraction of JVM maxMemory before shedding load (0.0 to 1.0)
   * @param maxActiveSessions maximum allowed concurrent sessions
   * @param retryAfterSeconds recommended retry delay sent to clients in HTTP 503 Retry-After header
   */
  public AdaptiveOverloadProtectionPolicy(
      double maxHeapUsageRatio, int maxActiveSessions, int retryAfterSeconds) {
    this(maxHeapUsageRatio, maxActiveSessions, retryAfterSeconds,
        new AdaptiveCircuitBreaker(5, 10_000L));
  }

  /**
   * Constructs an overload protection policy with a custom circuit breaker.
   *
   * @param maxHeapUsageRatio maximum fraction of JVM maxMemory before shedding load (0.0 to 1.0)
   * @param maxActiveSessions maximum allowed concurrent sessions
   * @param retryAfterSeconds recommended retry delay sent to clients in HTTP 503 Retry-After header
   * @param circuitBreaker custom circuit breaker instance
   */
  public AdaptiveOverloadProtectionPolicy(
      double maxHeapUsageRatio,
      int maxActiveSessions,
      int retryAfterSeconds,
      @NonNull AdaptiveCircuitBreaker circuitBreaker) {
    if (maxHeapUsageRatio <= 0.0 || maxHeapUsageRatio > 1.0) {
      throw new IllegalArgumentException(
          "maxHeapUsageRatio must be between 0.0 and 1.0: " + maxHeapUsageRatio);
    }
    if (maxActiveSessions <= 0) {
      throw new IllegalArgumentException("maxActiveSessions must be positive: " + maxActiveSessions);
    }
    this.maxHeapUsageRatio = maxHeapUsageRatio;
    this.maxActiveSessions = maxActiveSessions;
    this.retryAfterSeconds = Math.max(1, retryAfterSeconds);
    this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
  }

  @Override
  public @NonNull AdmissionResult tryAcquire(int currentActiveSessions) {
    // 1. Check circuit breaker state
    if (!circuitBreaker.allowExecution()) {
      return AdmissionResult.rejected(retryAfterSeconds, "Circuit breaker is OPEN");
    }

    // 2. Check maximum active sessions
    if (currentActiveSessions >= maxActiveSessions) {
      circuitBreaker.recordFailure();
      return AdmissionResult.rejected(
          retryAfterSeconds,
          "Maximum concurrent session capacity reached (" + maxActiveSessions + ")");
    }

    // 3. Check JVM heap memory utilization
    final Runtime runtime = Runtime.getRuntime();
    final long maxMemory = runtime.maxMemory();
    final long usedMemory = runtime.totalMemory() - runtime.freeMemory();
    if (maxMemory > 0) {
      final double heapRatio = (double) usedMemory / maxMemory;
      if (heapRatio >= maxHeapUsageRatio) {
        circuitBreaker.recordFailure();
        return AdmissionResult.rejected(
            retryAfterSeconds,
            String.format(
                java.util.Locale.ROOT,
                "JVM heap memory pressure (%.1f%% >= %.1f%%)",
                heapRatio * 100.0,
                maxHeapUsageRatio * 100.0));
      }
    }

    // 4. Session admitted
    circuitBreaker.recordSuccess();
    return AdmissionResult.allowed();
  }

  @Override
  public void release() {
    // No-op for stateless admission, but hook available for token bucket or lease tracking
  }

  /**
   * Returns the underlying circuit breaker.
   *
   * @return circuit breaker instance
   */
  public @NonNull AdaptiveCircuitBreaker getCircuitBreaker() {
    return circuitBreaker;
  }

  /**
   * Returns the configured maximum heap usage ratio threshold.
   *
   * @return max heap usage ratio
   */
  public double getMaxHeapUsageRatio() {
    return maxHeapUsageRatio;
  }

  /**
   * Returns the configured maximum active sessions threshold.
   *
   * @return max active sessions
   */
  public int getMaxActiveSessions() {
    return maxActiveSessions;
  }
}
