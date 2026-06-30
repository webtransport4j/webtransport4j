package io.github.webtransport4j.server;

import java.net.InetSocketAddress;

/**
 * An engine for matching IP addresses and CIDR prefixes against a set of rules.
 *
 * @param <V> The type of the value associated with each rule.
 */
public interface IpFilterEngine<V> {

  /**
   * Adds a new rule to the engine.
   *
   * @param cidrOrIp The IP address (e.g. "127.0.0.1") or CIDR prefix (e.g. "10.0.0.0/8").
   * @param value The value associated with this rule.
   */
  void addRule(String cidrOrIp, V value);

  /**
   * Matches the given IP address against the rules.
   *
   * @param address The remote address to match.
   * @return The value of the matching rule, or null if no rule matches.
   */
  V match(InetSocketAddress address);

  /**
   * Matches the given IP address string against the rules.
   *
   * @param ip The remote IP string.
   * @return The value of the matching rule, or null if no rule matches.
   */
  V match(String ip);
}
