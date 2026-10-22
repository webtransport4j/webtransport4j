package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;

/**
 * Policy governing WebTransport session-level flow control credit extension and denial.
 *
 * <p>Implementations determine how many additional bytes or streams to grant when a peer indicates
 * it is blocked via {@code WT_DATA_BLOCKED} or {@code WT_STREAMS_BLOCKED} capsules.
 */
public interface SessionFlowControlPolicy {

  /**
   * Evaluates a {@code WT_DATA_BLOCKED} capsule from the peer and determines how many additional
   * data credit bytes to grant, or {@code 0} to deny or stall the extension.
   *
   * @param session the WebTransport session
   * @param blockedAtLimit the session-level limit at which blocking occurred
   * @param cumulativeReceivedBytes total bytes received from the peer so far
   * @return additional bytes to extend (must be positive), or {@code 0} (or negative) to deny
   */
  long onRequestDataCredit(
      @NonNull WebTransportSession session, long blockedAtLimit, long cumulativeReceivedBytes);

  /**
   * Evaluates a {@code WT_STREAMS_BLOCKED} capsule from the peer and determines how many additional
   * streams to permit, or {@code 0} to deny or cap the extension.
   *
   * @param session the WebTransport session
   * @param bidirectional {@code true} for bidirectional streams, {@code false} for unidirectional
   * @param blockedAtLimit the stream limit at which blocking occurred
   * @param currentActiveCount the number of active streams currently open in this direction
   * @return additional stream slots to grant (must be positive), or {@code 0} (or negative) to deny
   */
  default long onRequestStreamCredit(
      @NonNull WebTransportSession session,
      boolean bidirectional,
      long blockedAtLimit,
      long currentActiveCount) {
    return 0L;
  }

  /**
   * Returns a policy that denies all flow control extension requests (returns 0 for all credits).
   *
   * @return a denying policy
   */
  static @NonNull SessionFlowControlPolicy denyAll() {
    return new SessionFlowControlPolicy() {
      @Override
      public long onRequestDataCredit(
          @NonNull WebTransportSession session, long blockedAtLimit, long cumulativeReceivedBytes) {
        return 0L;
      }

      @Override
      public long onRequestStreamCredit(
          @NonNull WebTransportSession session,
          boolean bidirectional,
          long blockedAtLimit,
          long currentActiveCount) {
        return 0L;
      }
    };
  }
}
