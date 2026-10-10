package io.github.webtransport4j.example.lichess.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.NonNull;

/**
 * Replicates the lag tracking and exponential smoothing from Lichess {@code Lag.scala}.
 *
 * <p>Tracks per-user trusted frame lag, applies a 10% exponential moving average (EMA)
 * decay factor, and detects latency spikes.
 */
public final class LilaLagTracker {

  private static final float TRUSTED_REFRESH_FACTOR = 0.1f;
  private static final int MAX_TRUSTED_LAG_MS = 5000;

  private final Map<String, Integer> userLag = new ConcurrentHashMap<>();

  /**
   * Records a measured round frame lag for a user, updating their smoothed EMA lag.
   *
   * @param user the user identifier
   * @param rawMillis the measured round trip lag in milliseconds
   * @return the newly smoothed EMA lag
   */
  public int recordLag(@NonNull String user, int rawMillis) {
    int capped = Math.min(Math.max(rawMillis, 0), MAX_TRUSTED_LAG_MS);
    return userLag.compute(user, (k, prev) -> {
      if (prev == null) {
        return capped;
      }
      return (int) (prev * (1.0f - TRUSTED_REFRESH_FACTOR) + capped * TRUSTED_REFRESH_FACTOR);
    });
  }

  /**
   * Returns the current smoothed lag for a user.
   *
   * @param user the user identifier
   * @return smoothed lag in milliseconds, or 0 if unrecorded
   */
  public int getLag(@NonNull String user) {
    Integer lag = userLag.get(user);
    return lag != null ? lag : 0;
  }

  /**
   * Clears tracked stats.
   */
  public void clear() {
    userLag.clear();
  }
}
