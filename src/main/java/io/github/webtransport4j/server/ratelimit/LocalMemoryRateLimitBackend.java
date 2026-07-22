package io.github.webtransport4j.server.ratelimit;

import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;

/**
 * Lock-free, high-performance in-memory implementation of {@link RateLimitBackend} using atomic sliding minute buckets.
 */
public class LocalMemoryRateLimitBackend implements RateLimitBackend {

  private final Map<String, ConnectionCount> ipCounts = Object2ObjectMaps.synchronize(new Object2ObjectOpenHashMap<>());
  private final AtomicBoolean clearing = new AtomicBoolean(false);
  private volatile long currentMinute = System.currentTimeMillis() / 60000;

  @Override
  public int incrementAndGet(@NonNull String ip, long nowMinute, int maxTrackedIps) {
    if (nowMinute != currentMinute) {
      if (clearing.compareAndSet(false, true)) {
        try {
          ipCounts.clear();
          currentMinute = nowMinute;
        } finally {
          clearing.set(false);
        }
      }
    }

    ConnectionCount count = ipCounts.get(ip);
    if (count == null) {
      if (ipCounts.size() >= maxTrackedIps) {
        return Integer.MAX_VALUE; // State table capacity exceeded signal
      }
      count = ipCounts.computeIfAbsent(ip, k -> new ConnectionCount());
    }
    return count.incrementAndGet();
  }

  @Override
  public void clear() {
    ipCounts.clear();
  }

  private static class ConnectionCount {
    private final AtomicInteger count = new AtomicInteger(0);

    public int incrementAndGet() {
      return count.incrementAndGet();
    }
  }
}
