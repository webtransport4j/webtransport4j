package io.github.webtransport4j.api;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Context for an incoming WebTransport extended CONNECT session request prior to admission.
 */
public interface SessionRequestContext {

  /**
   * Returns the URI request path (including query string if present).
   *
   * @return the requested path
   */
  @NonNull String path();

  /**
   * Returns the client origin header value, or {@code null} if omitted.
   *
   * @return the client origin, or null
   */
  @Nullable String origin();

  /**
   * Returns the target authority (the {@code :authority} pseudo-header), or {@code null} if omitted.
   *
   * @return the target authority, or null
   */
  @Nullable String authority();

  /**
   * Returns the remote network address of the connecting peer, or {@code null} if unknown.
   *
   * @return the remote socket address, or null
   */
  @Nullable SocketAddress remoteAddress();

  /**
   * Returns an unmodifiable multi-map of all incoming HTTP/3 request headers.
   * Header names are normalized to lowercase.
   *
   * @return all request headers
   */
  @NonNull Map<String, List<String>> headers();

  /**
   * Returns the first header value for the given header name, or {@code null} if absent.
   *
   * @param name header name (case-insensitive)
   * @return the header value, or null
   */
  default @Nullable String header(@NonNull String name) {
    List<String> values = headers().get(name.toLowerCase(java.util.Locale.ROOT));
    return (values != null && !values.isEmpty()) ? values.get(0) : null;
  }
}
