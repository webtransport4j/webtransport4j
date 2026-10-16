package io.github.webtransport4j.server.ratelimit;

import io.github.webtransport4j.internal.handles.Handles;
import io.github.webtransport4j.internal.handles.IntHandle;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;

/**
 * Lock-free, high-performance in-memory implementation of {@link RateLimitBackend} using atomic
 * minute buckets and bounded admission without evicting active counts.
 */
public class LocalMemoryRateLimitBackend implements RateLimitBackend {

  private static final class MinuteBucket {
    final long minute;
    final ConcurrentHashMap<String, ConnectionCount> counts;
    final AtomicInteger trackedIps = new AtomicInteger();

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
    if (count == null) {
      MinuteBucket activeBucket = bucket;
      count = counts.computeIfAbsent(ip, k -> {
        while (true) {
          int tracked = activeBucket.trackedIps.get();
          if (maxTrackedIps > 0 && tracked >= maxTrackedIps) {
            return null;
          }
          if (activeBucket.trackedIps.compareAndSet(tracked, tracked + 1)) {
            return new ConnectionCount();
          }
        }
      });
    }
    return count == null ? Integer.MAX_VALUE : count.incrementAndGet();
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

    public int incrementAndGet() {
      return COUNT_HANDLE.incrementAndGet(this);
    }
  }
}
