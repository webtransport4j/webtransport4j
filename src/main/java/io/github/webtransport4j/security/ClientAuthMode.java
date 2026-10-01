package io.github.webtransport4j.security;

/**
 * TLS Client Authentication (mTLS) enforcement mode for WebTransport connections.
 */
public enum ClientAuthMode {
  /**
   * Client authentication is disabled. No client certificate is requested.
   */
  NONE,

  /**
   * Client certificate is requested during TLS handshake, but connection is permitted
   * if the client does not present one. If presented, it must be valid.
   */
  OPTIONAL,

  /**
   * Client certificate is strictly required. Handshake is terminated if client does not present
   * a trusted certificate.
   */
  REQUIRE
}
