package io.github.webtransport4j.health;

import io.github.webtransport4j.server.WebTransportServer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.NonNull;

/**
 * Kubernetes-compatible Liveness and Readiness health probe checker for {@link WebTransportServer}.
 *
 * <p>Can be directly mapped to Spring Boot Actuator health indicators, MicroProfile Health,
 * or lightweight HTTP health endpoints ({@code /healthz} and {@code /readyz}).
 */
public final class WebTransportHealthCheck {

  /**
   * Health status classification.
   */
  public enum Status {
    UP,
    DOWN,
    DRAINING
  }

  private WebTransportHealthCheck() {}

  /**
   * Evaluates the Kubernetes liveness probe of the server.
   *
   * @param server the WebTransport server instance
   * @return {@code true} if the server process is alive and running
   */
  public static boolean isAlive(@NonNull WebTransportServer server) {
    Objects.requireNonNull(server, "server");
    return server.isRunning();
  }

  /**
   * Evaluates the Kubernetes readiness probe of the server.
   *
   * <p>A server is ready to accept incoming traffic only if it is running and not in
   * a draining phase.
   *
   * @param server the WebTransport server instance
   * @return {@code true} if the server is ready to receive new connections
   */
  public static boolean isReady(@NonNull WebTransportServer server) {
    Objects.requireNonNull(server, "server");
    return server.isRunning() && !server.isDraining();
  }

  /**
   * Inspects detailed health telemetry of the server.
   *
   * @param server the WebTransport server instance
   * @return immutable map containing health metrics and state details
   */
  public static @NonNull Map<String, Object> getHealthDetails(@NonNull WebTransportServer server) {
    Objects.requireNonNull(server, "server");
    Map<String, Object> details = new LinkedHashMap<>();

    boolean running = server.isRunning();
    boolean draining = server.isDraining();

    Status status;
    if (!running) {
      status = Status.DOWN;
    } else if (draining) {
      status = Status.DRAINING;
    } else {
      status = Status.UP;
    }

    details.put("status", status.name());
    details.put("running", running);
    details.put("draining", draining);
    details.put("boundPort", server.getPort());
    details.put("activeSessions", server.getActiveSessionsCount());

    return Collections.unmodifiableMap(details);
  }
}
