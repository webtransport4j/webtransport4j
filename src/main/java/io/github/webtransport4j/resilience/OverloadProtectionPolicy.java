package io.github.webtransport4j.resilience;

import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Strategy interface for adaptive overload protection, admission control, and circuit breaking.
 */
public interface OverloadProtectionPolicy {

  /**
   * Encapsulates the admission decision for an incoming WebTransport session.
   */
  final class AdmissionResult {
    private static final AdmissionResult ADMITTED = new AdmissionResult(true, 0, null);

    private final boolean admitted;
    private final int retryAfterSeconds;
    private final String reason;

    private AdmissionResult(boolean admitted, int retryAfterSeconds, @Nullable String reason) {
      this.admitted = admitted;
      this.retryAfterSeconds = retryAfterSeconds;
      this.reason = reason;
    }

    /**
     * Returns a successful admission result.
     *
     * @return admitted result
     */
    public static @NonNull AdmissionResult allowed() {
      return ADMITTED;
    }

    /**
     * Creates a rejection result with retry-after hint and diagnostic reason.
     *
     * @param retryAfterSeconds recommended seconds before client retries
     * @param reason diagnostic explanation for load shedding
     * @return rejection result
     */
    public static @NonNull AdmissionResult rejected(int retryAfterSeconds, @NonNull String reason) {
      Objects.requireNonNull(reason, "reason must not be null");
      return new AdmissionResult(false, Math.max(0, retryAfterSeconds), reason);
    }

    /**
     * Returns true if the session is permitted.
     *
     * @return true if admitted
     */
    public boolean isAdmitted() {
      return admitted;
    }

    /**
     * Returns the recommended retry delay in seconds.
     *
     * @return retry delay in seconds
     */
    public int getRetryAfterSeconds() {
      return retryAfterSeconds;
    }

    /**
     * Returns the diagnostic reason if rejected, or null if admitted.
     *
     * @return rejection reason or null
     */
    public @Nullable String getReason() {
      return reason;
    }
  }

  /**
   * Evaluates whether a new session request may proceed or should be shed.
   *
   * @param currentActiveSessions other active sessions and pending reservations across the server,
   *     excluding the request being evaluated; zero when global slot tracking is unavailable
   * @return the admission result
   */
  @NonNull AdmissionResult tryAcquire(int currentActiveSessions);

  /**
   * Notifies the policy that an active session has terminated.
   */
  void release();
}
