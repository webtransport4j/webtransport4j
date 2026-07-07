package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportSession;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global registry for managing WebTransport session resumption tokens.
 * Caches disconnected sessions and evicts them if the resumption window expires.
 */
public class SessionResumptionManager {
  private static final Logger logger = LoggerFactory.getLogger(SessionResumptionManager.class);
  private static final SessionResumptionManager INSTANCE = new SessionResumptionManager();

  private final Map<String, ResumableSession> cache = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "wt-session-resumption-cleaner");
    t.setDaemon(true);
    return t;
  });

  private SessionResumptionManager() {
    long cleanupInterval = WebTransportConfig.getLong("webtransport4j.session.resumption.cleanup.seconds", 10L);
    scheduler.scheduleAtFixedRate(this::cleanExpiredSessions, cleanupInterval, cleanupInterval, TimeUnit.SECONDS);
  }

  public static SessionResumptionManager getInstance() {
    return INSTANCE;
  }

  public void registerOrphanedSession(@NonNull String token, @NonNull WebTransportSession session) {
    long timeoutMs = WebTransportConfig.getLong("webtransport4j.session.resumption.timeout.seconds", 60L) * 1000L;
    cache.put(token, new ResumableSession(session, System.currentTimeMillis() + timeoutMs));
    logger.info("🔑 Registered orphaned session for resumption. Token: {}", token);
  }

  public @Nullable WebTransportSession retrieveAndRemove(@NonNull String token) {
    ResumableSession resumable = cache.remove(token);
    if (resumable != null && System.currentTimeMillis() <= resumable.expireTime) {
      return resumable.session;
    }
    return null;
  }

  private void cleanExpiredSessions() {
    long now = System.currentTimeMillis();
    cache.entrySet().removeIf(entry -> {
      boolean expired = now > entry.getValue().expireTime;
      if (expired) {
        logger.info("⏳ Resumption token expired and removed: {}", entry.getKey());
        try {
          entry.getValue().session.close();
        } catch (Exception e) {
          logger.error("Error closing expired session", e);
        }
      }
      return expired;
    });
  }

  private static class ResumableSession {
    final WebTransportSession session;
    final long expireTime;

    ResumableSession(WebTransportSession session, long expireTime) {
      this.session = session;
      this.expireTime = expireTime;
    }
  }
}
