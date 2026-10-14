package io.github.webtransport4j.server.ratelimit;

import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;

/**
 * Lock-free, high-performance in-memory implementation of {@link RateLimitBackend} using atomic
 * sliding minute buckets and bounded LRU eviction.
 */
public class LocalMemoryRateLimitBackend implements RateLimitBackend {

  private static final class MinuteBucket {
    final long minute;
    final ConcurrentHashMap<String, ConnectionCount> counts;

    MinuteBucket(long minute) {
      this.minute = minute;
      this.counts = new ConcurrentHashMap<>();
    }
  }

  private final AtomicReference<MinuteBucket> currentBucket =
      new AtomicReference<>(new MinuteBucket(-1L));

  @Override
  public int incrementAndGet(@NonNull String ip, long nowMinute, int maxTrackedIps) {
    MinuteBucket bucket;
    while (true) {
      bucket = currentBucket.get();
      if (nowMinute == bucket.minute) {
        break;
      }
      if (nowMinute < bucket.minute) {
        break;
      }
      MinuteBucket newBucket = new MinuteBucket(nowMinute);
      if (currentBucket.compareAndSet(bucket, newBucket)) {
        bucket = newBucket;
        break;
      }
    }

    ConcurrentHashMap<String, ConnectionCount> counts = bucket.counts;
    ConnectionCount count = counts.get(ip);
    long nowNanos = System.nanoTime();
    if (count == null) {
      if (maxTrackedIps > 0 && counts.size() >= maxTrackedIps) {
        evictLru(counts, maxTrackedIps);
      }
      count = counts.computeIfAbsent(ip, k -> new ConnectionCount(nowNanos));
    }
    return count.incrementAndGet(nowNanos);
  }

  private void evictLru(ConcurrentHashMap<String, ConnectionCount> counts, int maxTrackedIps) {
    int excess = counts.size() - maxTrackedIps + 1;
    if (excess <= 0) {
      return;
    }
    int toEvict = Math.min(excess, 8);
    for (int i = 0; i < toEvict; i++) {
      int samples = Math.min(16, counts.size());
      if (samples == 0) {
        break;
      }
      String oldestKey = null;
      ConnectionCount oldestCount = null;
      long oldestTime = Long.MAX_VALUE;

      int examined = 0;
      for (Map.Entry<String, ConnectionCount> entry : counts.entrySet()) {
        ConnectionCount cc = entry.getValue();
        if (cc != null) {
          long access = cc.getLastAccessTime();
          if (access < oldestTime) {
            oldestTime = access;
            oldestKey = entry.getKey();
            oldestCount = cc;
          }
        }
        examined++;
        if (examined >= samples) {
          break;
        }
      }

      if (oldestKey != null && oldestCount != null) {
        counts.remove(oldestKey, oldestCount);
      } else {
        break;
      }
    }
  }

  @Override
  public void clear() {
    MinuteBucket old = currentBucket.get();
    currentBucket.set(new MinuteBucket(old.minute));
  }

  /** Returns the current number of tracked IP entries. */
  public int getTrackedIpsCount() {
    return currentBucket.get().counts.size();
  }

  private static final class ConnectionCount {
    private static final IntHandle<ConnectionCount> COUNT_HANDLE =
        Handles.newIntHandle(
            ConnectionCount.class,
            "count",
            MethodHandles.lookup(),
            () -> AtomicIntegerFieldUpdater.newUpdater(ConnectionCount.class, "count"));
    private volatile int count;
    private volatile long lastAccessTime;

    ConnectionCount(long initialAccessTime) {
      this.lastAccessTime = initialAccessTime;
    }

    public int incrementAndGet(long nowNanos) {
      this.lastAccessTime = nowNanos;
      return COUNT_HANDLE.incrementAndGet(this);
    }

    public long getLastAccessTime() {
      return lastAccessTime;
    }
  }
}
