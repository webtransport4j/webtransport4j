package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;

/**
 * Service Provider Interface (SPI) for exporting observability metrics from the WebTransport
 * server. Implementations can map these callbacks to any telemetry backend:
 * OpenTelemetry (OTLP), Micrometer/Prometheus, Datadog, or simple logging.
 *
 * <p>The default no-op implementation is used unless the user registers a custom listener via
 * {@code WebTransportServer#setMetricsListener(WebTransportMetricsListener)}.
 *
 * @author https://github.com/sanjomo
 */
public interface WebTransportMetricsListener {

    /**
     * Called when a new WebTransport session is successfully negotiated and opened.
     *
     * @param sessionId The unique CONNECT stream ID for this session.
     * @param path      The request path (e.g., "/chat").
     */
    void onSessionOpened(long sessionId, @NonNull String path);

    /**
     * Called when a WebTransport session is closed or reaped.
     *
     * @param sessionId The unique CONNECT stream ID for this session.
     * @param closeCode The HTTP/3 error code used to close the session (0 = graceful close).
     */
    void onSessionClosed(long sessionId, int closeCode);

    /**
     * Called when a new stream (unidirectional or bidirectional) is opened under a session.
     *
     * @param sessionId     The session this stream belongs to.
     * @param streamId      The QUIC stream channel ID.
     * @param bidirectional {@code true} if the stream is bidirectional.
     */
    void onStreamOpened(long sessionId, long streamId, boolean bidirectional);

    /**
     * Called when a stream is closed.
     *
     * @param sessionId The session this stream belongs to.
     * @param streamId  The QUIC stream channel ID.
     */
    void onStreamClosed(long sessionId, long streamId);

    /**
     * Called when a datagram is successfully written and flushed to the network.
     *
     * @param sessionId The session sending the datagram.
     * @param bytes     The size of the datagram payload in bytes.
     */
    void onDatagramSent(long sessionId, int bytes);

    /**
     * Called when a datagram is received from the client.
     *
     * @param sessionId The session that received the datagram.
     * @param bytes     The size of the datagram payload in bytes.
     */
    void onDatagramReceived(long sessionId, int bytes);

    /**
     * Called when an incoming datagram is discarded (e.g., invalid session, parsing error).
     *
     * @param sessionId The session ID extracted from the datagram header, or {@code -1} if unknown.
     * @param reason    A short description of the discard reason.
     */
    void onDatagramDiscarded(long sessionId, @NonNull String reason);

    /**
     * Called when a client performs connection migration (IP/Port rebinding).
     *
     * @param sessionId  The session that migrated.
     * @param oldAddress The previous client address (e.g., "192.168.1.1:5000").
     * @param newAddress The new client address (e.g., "10.0.0.1:6000").
     */
    void onConnectionMigration(long sessionId, @NonNull String oldAddress, @NonNull String newAddress);
}
