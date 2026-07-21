package io.github.webtransport4j.spring;

import io.github.webtransport4j.server.WebTransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Spring {@link SmartLifecycle} wrapper for {@link WebTransportServer}.
 * Automatically starts the WebTransport server on Spring application startup
 * and gracefully stops it during application context shutdown.
 */
public class SpringWebTransportServerLifecycle implements SmartLifecycle {

  private static final Logger logger = LoggerFactory.getLogger(SpringWebTransportServerLifecycle.class);

  private final WebTransportServer server;
  private volatile boolean running = false;

  public SpringWebTransportServerLifecycle(WebTransportServer server) {
    if (server == null) {
      throw new IllegalArgumentException("WebTransportServer cannot be null");
    }
    this.server = server;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    try {
      logger.info("🌱 Starting WebTransport server via Spring SmartLifecycle...");
      server.start();
      running = true;
      logger.info("✅ WebTransport server started successfully on port {}", server.getPort());
    } catch (Exception e) {
      logger.error("❌ Failed to start WebTransport server via Spring SmartLifecycle", e);
      throw new IllegalStateException("Failed to start WebTransport server", e);
    }
  }

  @Override
  public void stop() {
    if (!running) {
      return;
    }
    logger.info("🌱 Stopping WebTransport server via Spring SmartLifecycle...");
    try {
      server.stop();
    } finally {
      running = false;
    }
  }

  @Override
  public void stop(Runnable callback) {
    try {
      stop();
    } finally {
      callback.run();
    }
  }

  @Override
  public boolean isRunning() {
    return running && server.isRunning();
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE - 1000;
  }

  public WebTransportServer getServer() {
    return server;
  }
}
