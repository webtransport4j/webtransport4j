package io.github.webtransport4j.observability;

import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.MDC;

/**
 * Utility for propagating WebTransport session context into SLF4J Mapped Diagnostic Context (MDC).
 *
 * <p>Enables structured log aggregators (Datadog, Splunk, Elastic/Kibana, Grafana Loki) to index
 * and correlate logs across sessions, connection IDs, and endpoints.
 */
public final class WebTransportMdcContext {

  public static final String KEY_SESSION_ID = "webtransport.session_id";
  public static final String KEY_PATH = "webtransport.path";
  public static final String KEY_REMOTE_ADDRESS = "webtransport.remote_address";
  public static final String KEY_CONNECTION_ID = "webtransport.connection_id";

  private WebTransportMdcContext() {}

  /**
   * Opens an MDC scope populated with the specified WebTransport session metadata.
   *
   * @param sessionId the unique WebTransport session identifier
   * @param path the request path
   * @param remoteAddress the remote client address
   * @param connectionId the underlying QUIC connection identifier
   * @return an {@link AutoCloseable} scope that restores previous MDC state upon {@link AutoCloseable#close()}
   */
  public static Scope open(
      long sessionId,
      @Nullable String path,
      @Nullable String remoteAddress,
      @Nullable String connectionId) {
    final String prevSessionId = MDC.get(KEY_SESSION_ID);
    final String prevPath = MDC.get(KEY_PATH);
    final String prevRemoteAddress = MDC.get(KEY_REMOTE_ADDRESS);
    final String prevConnectionId = MDC.get(KEY_CONNECTION_ID);

    MDC.put(KEY_SESSION_ID, String.valueOf(sessionId));
    if (path != null) {
      MDC.put(KEY_PATH, path);
    }
    if (remoteAddress != null) {
      MDC.put(KEY_REMOTE_ADDRESS, remoteAddress);
    }
    if (connectionId != null) {
      MDC.put(KEY_CONNECTION_ID, connectionId);
    }

    return new Scope(prevSessionId, prevPath, prevRemoteAddress, prevConnectionId);
  }

  /**
   * Scoped resource that restores previous MDC values when closed.
   */
  public static final class Scope implements AutoCloseable {
    private final String prevSessionId;
    private final String prevPath;
    private final String prevRemoteAddress;
    private final String prevConnectionId;

    private Scope(
        @Nullable String prevSessionId,
        @Nullable String prevPath,
        @Nullable String prevRemoteAddress,
        @Nullable String prevConnectionId) {
      this.prevSessionId = prevSessionId;
      this.prevPath = prevPath;
      this.prevRemoteAddress = prevRemoteAddress;
      this.prevConnectionId = prevConnectionId;
    }

    @Override
    public void close() {
      restoreOrRemove(KEY_SESSION_ID, prevSessionId);
      restoreOrRemove(KEY_PATH, prevPath);
      restoreOrRemove(KEY_REMOTE_ADDRESS, prevRemoteAddress);
      restoreOrRemove(KEY_CONNECTION_ID, prevConnectionId);
    }

    private static void restoreOrRemove(String key, @Nullable String val) {
      if (val != null) {
        MDC.put(key, val);
      } else {
        MDC.remove(key);
      }
    }
  }
}
