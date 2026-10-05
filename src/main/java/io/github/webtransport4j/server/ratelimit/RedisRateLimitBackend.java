package io.github.webtransport4j.server.ratelimit;

import io.github.webtransport4j.server.WebTransportConfig;
import java.util.function.BiFunction;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed Redis implementation of {@link RateLimitBackend} supporting multi-node cluster rate
 * limiting. Accommodates custom Redis client functions or reflection-based client adapters while
 * maintaining fallback capabilities.
 */
public class RedisRateLimitBackend implements RateLimitBackend {
  private static final Logger logger = LoggerFactory.getLogger(RedisRateLimitBackend.class);

  private final LocalMemoryRateLimitBackend fallbackBackend = new LocalMemoryRateLimitBackend();
  private final BiFunction<String, Long, Integer> redisIncrFunction;
  private final String keyPrefix;

  /** Constructs a new Redis rate limit backend with default key prefix. */
  public RedisRateLimitBackend() {
    this(
        null,
        WebTransportConfig.getNonNull(
            "webtransport4j.server.ratelimit.redis.key_prefix", "webtransport4j:ratelimit:"));
  }

  /**
   * Constructs a new Redis rate limit backend.
   *
   * @param customIncrFunction the Redis increment function
   * @param keyPrefix the key prefix to use in Redis
   */
  public RedisRateLimitBackend(
      @Nullable BiFunction<String, Long, Integer> customIncrFunction, @NonNull String keyPrefix) {
    this.keyPrefix = keyPrefix;
    this.redisIncrFunction = customIncrFunction;
  }

  @Override
  public int incrementAndGet(@NonNull String ip, long currentMinute, int maxTrackedIps) {
    if (redisIncrFunction != null) {
      try {
        String key = keyPrefix + ip + ":" + currentMinute;
        Integer count = redisIncrFunction.apply(key, currentMinute);
        if (count != null) {
          return count;
        }
      } catch (Exception e) {
        logger.warn(
            "⚠️ Redis rate limit backend error for IP {}, falling back to local memory: {}",
            ip,
            e.getMessage());
      }
    }
    return fallbackBackend.incrementAndGet(ip, currentMinute, maxTrackedIps);
  }

  @Override
  public void clear() {
    fallbackBackend.clear();
  }
}
