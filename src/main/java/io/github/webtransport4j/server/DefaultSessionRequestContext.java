package io.github.webtransport4j.server;

import io.github.webtransport4j.api.SessionRequestContext;
import io.netty.handler.codec.http3.Http3Headers;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Default immutable implementation of {@link SessionRequestContext}. */
public final class DefaultSessionRequestContext implements SessionRequestContext {
  private final String path;
  private final String origin;
  private final String authority;
  private final SocketAddress remoteAddress;
  private final Map<String, List<String>> headers;

  /**
   * Constructs a new {@link DefaultSessionRequestContext}.
   *
   * @param http3Headers incoming HTTP/3 headers
   * @param path the request path
   * @param remoteAddress the remote socket address
   */
  public DefaultSessionRequestContext(
      @NonNull Http3Headers http3Headers,
      @NonNull String path,
      @Nullable SocketAddress remoteAddress) {
    this.path = Objects.requireNonNull(path, "path must not be null");
    this.remoteAddress = remoteAddress;
    CharSequence origSeq = http3Headers.get("origin");
    this.origin = origSeq != null ? origSeq.toString() : null;
    CharSequence authSeq = http3Headers.authority();
    this.authority = authSeq != null ? authSeq.toString() : null;

    Map<String, List<String>> headerMap = new HashMap<>();
    for (Map.Entry<CharSequence, CharSequence> entry : http3Headers) {
      String key = entry.getKey().toString().toLowerCase(Locale.ROOT);
      String value = entry.getValue().toString();
      headerMap.computeIfAbsent(key, k -> new ArrayList<>(1)).add(value);
    }
    Map<String, List<String>> unmod = new HashMap<>();
    for (Map.Entry<String, List<String>> e : headerMap.entrySet()) {
      unmod.put(e.getKey(), Collections.unmodifiableList(e.getValue()));
    }
    this.headers = Collections.unmodifiableMap(unmod);
  }

  @Override
  public @NonNull String path() {
    return path;
  }

  @Override
  public @Nullable String origin() {
    return origin;
  }

  @Override
  public @Nullable String authority() {
    return authority;
  }

  @Override
  public @Nullable SocketAddress remoteAddress() {
    return remoteAddress;
  }

  @Override
  public @NonNull Map<String, List<String>> headers() {
    return headers;
  }
}
