package io.github.webtransport4j.api;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Context for an incoming WebTransport extended CONNECT session request prior to admission.
 */
public interface SessionRequestContext {

  /**
   * Returns the full URI request path (including query string if present).
   *
   * @return the requested path
   */
  @NonNull String path();

  /**
   * Returns the URI request path component without any query string.
   *
   * @return the base request path
   */
  default @NonNull String basePath() {
    String p = path();
    int idx = p.indexOf('?');
    return idx != -1 ? p.substring(0, idx) : p;
  }

  /**
   * Returns the raw query string, or {@code null} if no query string is present.
   *
   * @return the query string, or null
   */
  default @Nullable String query() {
    String p = path();
    int idx = p.indexOf('?');
    return (idx != -1 && idx < p.length() - 1) ? p.substring(idx + 1) : null;
  }

  /**
   * Returns an unmodifiable multi-map of decoded query parameters.
   *
   * @return the query parameters
   */
  default @NonNull Map<String, List<String>> queryParams() {
    return Collections.emptyMap();
  }

  /**
   * Returns the first query parameter value for the given name, or {@code null} if absent.
   *
   * @param name parameter name
   * @return parameter value, or null
   */
  default @Nullable String queryParam(@NonNull String name) {
    List<String> values = queryParams().get(name);
    return (values != null && !values.isEmpty()) ? values.get(0) : null;
  }

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
    List<String> values = headers().get(name.toLowerCase(Locale.ROOT));
    return (values != null && !values.isEmpty()) ? values.get(0) : null;
  }

  /**
   * Returns the value of a specific cookie parsed from the {@code Cookie} header, or {@code null} if absent.
   *
   * @param name cookie name
   * @return cookie value, or null
   */
  default @Nullable String cookie(@NonNull String name) {
    List<String> cookieHeaders = headers().get("cookie");
    if (cookieHeaders == null || cookieHeaders.isEmpty()) {
      return null;
    }
    for (String headerVal : cookieHeaders) {
      String[] pairs = headerVal.split(";");
      for (String pair : pairs) {
        String trimmed = pair.trim();
        int eq = trimmed.indexOf('=');
        if (eq > 0) {
          String cookieName = trimmed.substring(0, eq).trim();
          if (cookieName.equalsIgnoreCase(name)) {
            String val = trimmed.substring(eq + 1).trim();
            if (val.length() >= 2 && val.startsWith("\"") && val.endsWith("\"")) {
              val = val.substring(1, val.length() - 1);
            }
            return val;
          }
        }
      }
    }
    return null;
  }
}
