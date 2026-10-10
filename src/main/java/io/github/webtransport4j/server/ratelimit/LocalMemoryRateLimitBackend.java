package io.github.webtransport4j.server.ratelimit;

import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.jspecify.annotations.NonNull;

/**
 * Lock-free, high-performance in-memory implementation of {@link RateLimitBackend} using atomic
 * sliding minute buckets.
 */
public class LocalMemoryRateLimitBackend implements RateLimitBackend {

  private static final IntHandle<LocalMemoryRateLimitBackend> CLEARING_HANDLE =
      Handles.newIntHandle(
          LocalMemoryRateLimitBackend.class,
          "clearing",
          MethodHandles.lookup(),
          () -> AtomicIntegerFieldUpdater.newUpdater(LocalMemoryRateLimitBackend.class, "clearing"));

  private final Map<String, ConnectionCount> ipCounts = new ConcurrentHashMap<>();
  private volatile int clearing;
  private volatile long currentMinute = System.currentTimeMillis() / 60000;

  @Override
  public int incrementAndGet(@NonNull String ip, long nowMinute, int maxTrackedIps) {
    if (nowMinute != currentMinute) {
      if (CLEARING_HANDLE.compareAndSet(this, 0, 1)) {
        try {
          if (nowMinute != currentMinute) {
            ipCounts.clear();
            currentMinute = nowMinute;
          }
        } finally {
          clearing = 0;
        }
      } else {
        while (clearing != 0 && nowMinute != currentMinute) {
          Thread.yield();
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

  private static final class ConnectionCount {
    private static final IntHandle<ConnectionCount> COUNT_HANDLE =
        Handles.newIntHandle(
            ConnectionCount.class,
            "count",
            MethodHandles.lookup(),
            () -> AtomicIntegerFieldUpdater.newUpdater(ConnectionCount.class, "count"));
    private volatile int count;

    public int incrementAndGet() {
      return COUNT_HANDLE.incrementAndGet(this);
    }
  }
}
