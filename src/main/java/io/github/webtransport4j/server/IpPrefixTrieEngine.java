package io.github.webtransport4j.server;

import io.netty.util.NetUtil;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** IP filter engine using a binary trie. */
public class IpPrefixTrieEngine<V> implements IpFilterEngine<V> {
  private static final Logger logger = LoggerFactory.getLogger(IpPrefixTrieEngine.class);

  private static class Node<V> {
    Node<V> zero;
    Node<V> one;
    V value;
  }

  private final Node<V> ipv4Root = new Node<>();
  private final Node<V> ipv6Root = new Node<>();

  @Override
  public void addRule(String cidrOrIp, V value) {
    try {
      String ipPart = cidrOrIp;
      int prefixLength = -1;
      if (cidrOrIp.contains("/")) {
        String[] parts = cidrOrIp.split("/");
        ipPart = parts[0];
        prefixLength = Integer.parseInt(parts[1]);
      }

      byte[] bytes = NetUtil.createByteArrayFromIpAddressString(ipPart);
      if (bytes == null) {
        logger.error("Invalid IP or CIDR in IpPrefixTrieEngine: {}", cidrOrIp);
        return;
      }

      if (prefixLength == -1) {
        prefixLength = bytes.length * 8;
      }

      Node<V> current = (bytes.length == 4) ? ipv4Root : ipv6Root;

      for (int i = 0; i < prefixLength; i++) {
        int byteIndex = i / 8;
        int bitIndex = 7 - (i % 8);
        boolean isSet = ((bytes[byteIndex] >> bitIndex) & 1) == 1;

        if (isSet) {
          if (current.one == null) {
            current.one = new Node<>();
          }
          current = current.one;
        } else {
          if (current.zero == null) {
            current.zero = new Node<>();
          }
          current = current.zero;
        }
      }
      current.value = value;
    } catch (Exception e) {
      logger.error("Failed to parse IP rule in IpPrefixTrieEngine: {}", cidrOrIp, e);
    }
  }

  @Override
  public V match(InetSocketAddress address) {
    if (address == null || address.getAddress() == null) {
      return null;
    }
    return matchBytes(address.getAddress().getAddress());
  }

  @Override
  public V match(String ip) {
    byte[] bytes = NetUtil.createByteArrayFromIpAddressString(ip);
    if (bytes == null) {
      return null;
    }
    return matchBytes(bytes);
  }

  private V matchBytes(byte[] bytes) {
    Node<V> current = (bytes.length == 4) ? ipv4Root : ipv6Root;
    V bestMatch = current.value;

    for (int i = 0; i < bytes.length * 8; i++) {
      int byteIndex = i / 8;
      int bitIndex = 7 - (i % 8);
      boolean isSet = ((bytes[byteIndex] >> bitIndex) & 1) == 1;

      if (isSet) {
        if (current.one == null) {
          break;
        }
        current = current.one;
      } else {
        if (current.zero == null) {
          break;
        }
        current = current.zero;
      }

      if (current.value != null) {
        bestMatch = current.value;
      }
    }
    return bestMatch;
  }
}
