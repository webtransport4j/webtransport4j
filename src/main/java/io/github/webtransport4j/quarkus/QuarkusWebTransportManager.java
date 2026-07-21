package io.github.webtransport4j.quarkus;

import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lifecycle manager and endpoint discovery helper for Quarkus and CDI microservices.
 * Supports non-blocking startup during Quarkus {@code StartupEvent} and graceful shutdown on {@code ShutdownEvent}.
 */
public class QuarkusWebTransportManager {

  private static final Logger logger = LoggerFactory.getLogger(QuarkusWebTransportManager.class);

  private final WebTransportServer server;

  public QuarkusWebTransportManager(WebTransportServer server) {
    if (server == null) {
      throw new IllegalArgumentException("WebTransportServer cannot be null");
    }
    this.server = server;
  }

  /**
   * Constructs a WebTransportServer by discovering {@link WebTransportEndpoint} annotated CDI beans
   * and building the server with a fluent builder.
   *
   * @param builder pre-configured builder (or {@link WebTransportServer#builder()})
   * @param cdiBeans collection of CDI bean instances
   * @return initialized manager instance
   */
  public static QuarkusWebTransportManager create(
      WebTransportServerBuilder builder, Collection<Object> cdiBeans) {

    if (builder == null) {
      builder = WebTransportServer.builder();
    }

    if (cdiBeans != null) {
      for (Object bean : cdiBeans) {
        WebTransportEndpoint ann = bean.getClass().getAnnotation(WebTransportEndpoint.class);
        if (ann != null) {
          String path = ann.path();
          boolean isDefault = ann.isDefault();
          logger.info("⚡ Discovered Quarkus/CDI WebTransport Endpoint: path='{}', default={}, bean='{}'",
              path, isDefault, bean.getClass().getName());

          if (bean instanceof WebTransportHandler) {
            WebTransportHandler handler = (WebTransportHandler) bean;
            if (isDefault) {
              builder.defaultHandler(handler);
            } else {
              builder.handler(path, handler);
            }
          } else if (bean instanceof ReactiveWebTransportHandler) {
            ReactiveWebTransportHandler reactiveHandler = (ReactiveWebTransportHandler) bean;
            if (isDefault) {
              builder.defaultReactiveHandler(reactiveHandler);
            } else {
              builder.reactiveHandler(path, reactiveHandler);
            }
          } else {
            logger.warn("⚠️ Bean '{}' has @WebTransportEndpoint but does not implement WebTransportHandler or ReactiveWebTransportHandler",
                bean.getClass().getName());
          }
        }
      }
    }

    return new QuarkusWebTransportManager(builder.build());
  }

  /**
   * Starts the WebTransport server non-blockingly (suitable for calling inside Quarkus {@code StartupEvent}).
   */
  public void onStartup() {
    try {
      logger.info("⚡ Starting Quarkus WebTransport Server...");
      server.start();
      logger.info("✅ Quarkus WebTransport Server started on port {}", server.getPort());
    } catch (Exception e) {
      logger.error("❌ Failed to start Quarkus WebTransport Server", e);
      throw new RuntimeException("Failed to start WebTransport server", e);
    }
  }

  /**
   * Stops the WebTransport server (suitable for calling inside Quarkus {@code ShutdownEvent}).
   */
  public void onShutdown() {
    logger.info("⚡ Stopping Quarkus WebTransport Server...");
    try {
      server.stop();
    } catch (Exception e) {
      logger.error("❌ Error shutting down Quarkus WebTransport Server", e);
    }
  }

  public WebTransportServer getServer() {
    return server;
  }
}
