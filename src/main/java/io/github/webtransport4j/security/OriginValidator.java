package io.github.webtransport4j.security;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Functional interface for validating WebTransport session origins and authorities. */
@FunctionalInterface
public interface OriginValidator {

  /**
   * Evaluates whether an incoming WebTransport session request is permitted from the given origin
   * and authority.
   *
   * @param origin the client-provided {@code Origin} header value, or {@code null} if omitted
   * @param authority the HTTP/3 {@code :authority} header value, or {@code null} if omitted
   * @return {@code true} if allowed; {@code false} to reject with HTTP 403 Forbidden
   */
  boolean validate(@Nullable String origin, @Nullable String authority);

  /**
   * Returns an origin validator that allows all connections regardless of origin header.
   *
   * @return permissive origin validator
   */
  static @NonNull OriginValidator allowAll() {
    return (origin, authority) -> true;
  }

  /**
   * Returns an origin validator that strictly requires an {@code Origin} header matching one of the
   * provided exact origins or host patterns.
   *
   * @param allowed the allowed origins or hostnames
   * @return strict origin validator
   */
  static @NonNull OriginValidator exact(@NonNull String... allowed) {
    Objects.requireNonNull(allowed, "allowed origins must not be null");
    return fromCollection(Arrays.asList(allowed), true);
  }

  /**
   * Returns an origin validator that supports wildcard domain patterns (e.g. {@code
   * *.example.com}).
   *
   * @param patterns the allowed origin patterns or hostnames
   * @return wildcard origin validator
   */
  static @NonNull OriginValidator wildcard(@NonNull String... patterns) {
    Objects.requireNonNull(patterns, "patterns must not be null");
    return fromCollection(Arrays.asList(patterns), false);
  }

  /**
   * Creates an origin validator from a collection of allowed patterns. An empty collection, or one
   * containing only null/blank entries, denies all requests. An explicit {@code *} allows all
   * origins but still respects {@code requireOriginHeader}.
   *
   * @param allowedPatterns collection of allowed origins or hostnames
   * @param requireOriginHeader if {@code true}, connections without an {@code Origin} header are
   *     rejected
   * @return configured origin validator
   */
  static @NonNull OriginValidator fromCollection(
      @NonNull Collection<String> allowedPatterns, boolean requireOriginHeader) {
    Objects.requireNonNull(allowedPatterns, "allowedPatterns must not be null");
    final Set<String> patterns = new HashSet<>();
    for (String p : allowedPatterns) {
      if (p != null && !p.trim().isEmpty()) {
        patterns.add(p.trim().toLowerCase(Locale.ROOT));
      }
    }

    return (origin, authority) -> {
      if (patterns.isEmpty()
          || (requireOriginHeader && (origin == null || origin.trim().isEmpty()))) {
        return false;
      }
      if (patterns.contains("*")) {
        return true;
      }
      if (origin == null || origin.trim().isEmpty()) {
        if (requireOriginHeader) {
          return false;
        }
        if (authority == null) {
          return false;
        }
        return matchesHostOrPattern(authority, patterns);
      }
      return matchesHostOrPattern(origin, patterns);
    };
  }

  /**
   * Checks whether the given input (URL, origin, or host) matches any of the patterns.
   *
   * @param input the input string
   * @param patterns the normalized lowercase patterns
   * @return {@code true} if matching
   */
  static boolean matchesHostOrPattern(@NonNull String input, @NonNull Set<String> patterns) {
    String trimmed = input.trim().toLowerCase(Locale.ROOT);
    if (patterns.contains(trimmed)) {
      return true;
    }
    String host = extractHost(trimmed);
    if (host == null) {
      return false;
    }
    if (patterns.contains(host)) {
      return true;
    }
    for (String pattern : patterns) {
      if (pattern.startsWith("*.")) {
        String baseDomain = pattern.substring(2);
        if (host.equals(baseDomain) || host.endsWith("." + baseDomain)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Extracts the hostname component from an origin or URL string.
   *
   * @param value the origin or URL string
   * @return the extracted hostname, or {@code null} if parsing fails
   */
  static @Nullable String extractHost(@Nullable String value) {
    if (value == null) {
      return null;
    }
    try {
      String uriStr = value.trim();
      if (!uriStr.contains("://")) {
        uriStr = "https://" + uriStr;
      }
      URI uri = new URI(uriStr);
      return uri.getHost();
    } catch (Exception ignored) {
      return null;
    }
  }
}
