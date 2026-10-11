package io.github.webtransport4j.observability;

import org.jspecify.annotations.Nullable;
import org.slf4j.MDC;

/**
 * Utility for propagating WebTransport session context into SLF4J Mapped Diagnostic Context (MDC).
 *
 * <p>Enables structured log aggregators (Datadog, Splunk, Elastic/Kibana, Grafana Loki) to index
 * and correlate logs across sessions, connection IDs, and endpoints.
 */
public final class WebTransportMdcContext {

  /** MDC key for WebTransport session ID. */
  public static final String KEY_SESSION_ID = "webtransport.session_id";

  /** MDC key for WebTransport request path. */
  public static final String KEY_PATH = "webtransport.path";

  /** MDC key for remote client socket address. */
  public static final String KEY_REMOTE_ADDRESS = "webtransport.remote_address";

  /** MDC key for underlying QUIC connection ID. */
  public static final String KEY_CONNECTION_ID = "webtransport.connection_id";

  /** Private constructor to prevent instantiation. */
  private WebTransportMdcContext() {}

  /**
   * Opens an MDC scope populated with the specified WebTransport session metadata.
   *
   * @param sessionId the unique WebTransport session identifier
   * @param path the request path
   * @param remoteAddress the remote client address
   * @param connectionId the underlying QUIC connection identifier
   * @return an {@link AutoCloseable} scope that restores previous MDC state upon {@link
   *     AutoCloseable#close()}
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
    } else {
      MDC.remove(KEY_PATH);
    }
    if (remoteAddress != null) {
      MDC.put(KEY_REMOTE_ADDRESS, remoteAddress);
    } else {
      MDC.remove(KEY_REMOTE_ADDRESS);
    }
    if (connectionId != null) {
      MDC.put(KEY_CONNECTION_ID, connectionId);
    } else {
      MDC.remove(KEY_CONNECTION_ID);
    }

    return new Scope(prevSessionId, prevPath, prevRemoteAddress, prevConnectionId);
  }

  /** Scoped resource that restores previous MDC values when closed. */
  public static final class Scope implements AutoCloseable {

    private static final io.github.webtransport4j.internal.handles.IntHandle<Scope> CLOSED_HANDLE =
        io.github.webtransport4j.internal.handles.Handles.newIntHandle(
            Scope.class,
            "closed",
            java.lang.invoke.MethodHandles.lookup(),
            () -> java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater(Scope.class, "closed"));

    private final String prevSessionId;
    private final String prevPath;
    private final String prevRemoteAddress;
    private final String prevConnectionId;
    private volatile int closed;

    /**
     * Constructs a new scope capturing previous MDC values.
     *
     * @param prevSessionId previous session ID
     * @param prevPath previous path
     * @param prevRemoteAddress previous remote address
     * @param prevConnectionId previous connection ID
     */
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

    /** Closes the scope and restores previous MDC values idempotently. */
    @Override
    public void close() {
      if (CLOSED_HANDLE.compareAndSet(this, 0, 1)) {
        restoreOrRemove(KEY_SESSION_ID, prevSessionId);
        restoreOrRemove(KEY_PATH, prevPath);
        restoreOrRemove(KEY_REMOTE_ADDRESS, prevRemoteAddress);
        restoreOrRemove(KEY_CONNECTION_ID, prevConnectionId);
      }
    }

    /**
     * Restores previous value or removes the MDC key if null.
     *
     * @param key MDC key
     * @param val previous value or null
     */
    private static void restoreOrRemove(String key, @Nullable String val) {
      if (val != null) {
        MDC.put(key, val);
      } else {
        MDC.remove(key);
      }
    }
  }
}
