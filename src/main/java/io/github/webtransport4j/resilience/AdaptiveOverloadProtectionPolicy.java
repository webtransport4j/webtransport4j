package io.github.webtransport4j.resilience;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Objects;
import java.util.function.DoubleSupplier;
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
  private final DoubleSupplier heapUsageSupplier;

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
    this(
        maxHeapUsageRatio,
        maxActiveSessions,
        retryAfterSeconds,
        circuitBreaker,
        AdaptiveOverloadProtectionPolicy::calculateHeapUsageRatio);
  }

  /**
   * Constructs an overload protection policy with a custom circuit breaker and heap supplier.
   *
   * @param maxHeapUsageRatio maximum fraction of JVM maxMemory before shedding load (0.0 to 1.0)
   * @param maxActiveSessions maximum allowed concurrent sessions
   * @param retryAfterSeconds recommended retry delay sent to clients in HTTP 503 Retry-After header
   * @param circuitBreaker custom circuit breaker instance
   * @param heapUsageSupplier supplier of current heap usage ratio (0.0 to 1.0)
   */
  public AdaptiveOverloadProtectionPolicy(
      double maxHeapUsageRatio,
      int maxActiveSessions,
      int retryAfterSeconds,
      @NonNull AdaptiveCircuitBreaker circuitBreaker,
      @NonNull DoubleSupplier heapUsageSupplier) {
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
    this.heapUsageSupplier =
        Objects.requireNonNull(heapUsageSupplier, "heapUsageSupplier must not be null");
  }

  /**
   * Computes the current post-GC retained heap utilization ratio across all JVM heap pools.
   *
   * <p>Using collection usage ({@link MemoryPoolMXBean#getCollectionUsage()}) avoids
   * false-positive overload rejections triggered by normal transient young-generation
   * allocations between GC cycles.
   *
   * @return heap utilization ratio between 0.0 and 1.0
   */
  public static double calculateHeapUsageRatio() {
    long collectionUsed = 0;
    boolean hasCollectionUsage = false;
    long fallbackUsed = 0;

    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP) {
        MemoryUsage collectionUsage = pool.getCollectionUsage();
        if (collectionUsage != null && collectionUsage.getUsed() >= 0) {
          collectionUsed += collectionUsage.getUsed();
          hasCollectionUsage = true;
        }
        MemoryUsage currentUsage = pool.getUsage();
        if (currentUsage != null && currentUsage.getUsed() >= 0) {
          fallbackUsed += currentUsage.getUsed();
        }
      }
    }

    final long maxMemory = Runtime.getRuntime().maxMemory();
    if (maxMemory <= 0) {
      return 0.0;
    }

    final long effectiveUsed;
    if (hasCollectionUsage) {
      effectiveUsed = collectionUsed;
    } else if (fallbackUsed > 0) {
      effectiveUsed = fallbackUsed;
    } else {
      effectiveUsed =
          Math.max(0L, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    return Math.min(1.0, Math.max(0.0, (double) effectiveUsed / (double) maxMemory));
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
    final double heapRatio = heapUsageSupplier.getAsDouble();
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

    // 4. Session admitted (circuit breaker success is recorded upon handshake completion)
    return AdmissionResult.allowed();
  }

  @Override
  public void recordSuccess() {
    circuitBreaker.recordSuccess();
  }

  @Override
  public void recordFailure() {
    circuitBreaker.recordFailure();
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
