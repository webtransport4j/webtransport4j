package io.github.webtransport4j.server.ratelimit;

import org.jspecify.annotations.NonNull;

/**
 * Pluggable backend interface for rate-limiting incoming connection counts per IP.
 */
public interface RateLimitBackend {

  /**
   * Increments and returns the active connection count for the specified IP within the current minute window.
   *
   * @param ip the remote IP address
   * @param currentMinute current minute timestamp (System.currentTimeMillis() / 60000)
   * @param maxTrackedIps maximum number of tracked IP entries allowed in memory/backend
   * @return the updated connection count for the IP in the current window
   */
  int incrementAndGet(@NonNull String ip, long currentMinute, int maxTrackedIps);

  /**
   * Clears all tracking state in the rate limiting backend.
   */
  void clear();
}
