package io.github.webtransport4j.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler for rate-limiting connections per IP. */
public class IpRateLimitingHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(IpRateLimitingHandler.class);

  // Simplified token bucket / sliding window per minute
  private static final Map<String, ConnectionCount> ipCounts = new ConcurrentHashMap<>();
  private static final AtomicBoolean clearing = new AtomicBoolean(false);
  private static volatile long currentMinute = System.currentTimeMillis() / 60000;

  private static class SharedRateLimitRules {
    final int maxConnectionsPerMinute;
    final int maxTrackedIps;
    final IpFilterEngine<Boolean> whitelistEngine;
    final IpFilterEngine<Integer> overridesEngine;
    final IpBloomFilter blocklistFilter;
    final java.util.Set<String> exactBlocklist;

    // Fields to detect changes
    final String rawBlocklistConfig;
    final String rawWhitelistConfig;
    final String rawOverridesConfig;
    final int bloomCapacity;
    final double bloomFpp;
    final String engineType;

    SharedRateLimitRules() {
      this(null);
    }

    SharedRateLimitRules(SharedRateLimitRules previous) {
      this.maxConnectionsPerMinute =
          WebTransportConfig.getInt(
              "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100);
      this.maxTrackedIps =
          WebTransportConfig.getInt(
              "webtransport4j.server.ratelimit.max_tracked_ips", 100000);

      this.engineType =
          Objects.requireNonNull(WebTransportConfig.get("webtransport4j.server.ratelimit.filter_engine", "trie"))
              .toLowerCase();

      this.rawWhitelistConfig =
          WebTransportConfig.getNonNull("webtransport4j.server.ratelimit.whitelist", "");
      if (previous != null && this.engineType.equals(previous.engineType) && this.rawWhitelistConfig.equals(previous.rawWhitelistConfig)) {
        this.whitelistEngine = previous.whitelistEngine;
      } else {
        if ("netty".equals(this.engineType)) {
          this.whitelistEngine = new NettyLinearIpFilterEngine<>();
        } else {
          this.whitelistEngine = new IpPrefixTrieEngine<>();
        }
        for (String allowed : this.rawWhitelistConfig.split(",")) {
          allowed = allowed.trim();
          if (!allowed.isEmpty()) {
            this.whitelistEngine.addRule(allowed, true);
          }
        }
      }

      this.rawOverridesConfig =
          WebTransportConfig.getNonNull("webtransport4j.server.ratelimit.overrides", "");
      if (previous != null && this.engineType.equals(previous.engineType) && this.rawOverridesConfig.equals(previous.rawOverridesConfig)) {
        this.overridesEngine = previous.overridesEngine;
      } else {
        if ("netty".equals(this.engineType)) {
          this.overridesEngine = new NettyLinearIpFilterEngine<>();
        } else {
          this.overridesEngine = new IpPrefixTrieEngine<>();
        }
        if (!this.rawOverridesConfig.isEmpty()) {
          for (String override : this.rawOverridesConfig.split(",")) {
            int lastColon = override.lastIndexOf(":");
            if (lastColon > 0) {
              try {
                String ipOrCidr = override.substring(0, lastColon).trim();
                int limit = Integer.parseInt(override.substring(lastColon + 1).trim());
                this.overridesEngine.addRule(ipOrCidr, limit);
              } catch (NumberFormatException e) {
                logger.error("Invalid ratelimit override format: {}", override);
              }
            }
          }
        }
      }

      this.bloomCapacity =
          WebTransportConfig.getInt(
              "webtransport4j.server.ratelimit.blocklist.bloom_capacity", 1_000_000);
      double fpp = 0.000000001;
      String fppStr =
          WebTransportConfig.getNonNull(
              "webtransport4j.server.ratelimit.blocklist.bloom_fpp", "0.000000001");
      try {
        fpp = Double.parseDouble(fppStr);
      } catch (NumberFormatException e) {
        logger.warn("Invalid bloom_fpp config: {}, using default 0.000000001", fppStr);
      }
      this.bloomFpp = fpp;

      this.rawBlocklistConfig =
          WebTransportConfig.getNonNull("webtransport4j.server.ratelimit.blocklist", "");

      if (previous != null
          && this.bloomCapacity == previous.bloomCapacity
          && Double.compare(this.bloomFpp, previous.bloomFpp) == 0
          && this.rawBlocklistConfig.equals(previous.rawBlocklistConfig)) {
        this.blocklistFilter = previous.blocklistFilter;
        this.exactBlocklist = previous.exactBlocklist;
      } else {
        this.blocklistFilter = new IpBloomFilter(this.bloomCapacity, this.bloomFpp);
        this.exactBlocklist = new java.util.HashSet<>();
        if (!this.rawBlocklistConfig.isEmpty()) {
          for (String blocked : this.rawBlocklistConfig.split(",")) {
            blocked = blocked.trim();
            if (!blocked.isEmpty()) {
              this.blocklistFilter.add(blocked);
              this.exactBlocklist.add(blocked);
            }
          }
        }
      }
    }
  }

  private static volatile SharedRateLimitRules sharedRules = new SharedRateLimitRules();

  public static void reloadSharedConfig() {
    sharedRules = new SharedRateLimitRules(sharedRules);
  }

  static {
    boolean reloadEnabled =
        WebTransportConfig.getBoolean("webtransport4j.server.ratelimit.dynamic_reload.enabled", true);
    if (reloadEnabled) {
      int reloadInterval =
          WebTransportConfig.getInt("webtransport4j.server.ratelimit.dynamic_reload.interval_secs", 10);
      if (reloadInterval > 0) {
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
          Thread t = new Thread(r, "wt-rate-limit-reloader");
          t.setDaemon(true);
          return t;
        }).scheduleAtFixedRate(() -> {
          try {
            if (WebTransportConfig.reload()) {
              reloadSharedConfig();
            }
          } catch (Exception e) {
            logger.error("Error reloading configuration in background", e);
          }
        }, reloadInterval, reloadInterval, java.util.concurrent.TimeUnit.SECONDS);
      }
    }
  }

  private final SharedRateLimitRules rules;
  private final int maxConnectionsPerMinute;
  private final int maxTrackedIps;
  private final IpFilterEngine<Boolean> whitelistEngine;
  private final IpFilterEngine<Integer> overridesEngine;
  private final IpBloomFilter blocklistFilter;
  private final java.util.Set<String> exactBlocklist;

  /** Ip Rate Limiting Handler. */
  public IpRateLimitingHandler() {
    this.rules = sharedRules;
    this.maxConnectionsPerMinute = rules.maxConnectionsPerMinute;
    this.maxTrackedIps = rules.maxTrackedIps;
    this.whitelistEngine = rules.whitelistEngine;
    this.overridesEngine = rules.overridesEngine;
    this.blocklistFilter = rules.blocklistFilter;
    this.exactBlocklist = rules.exactBlocklist;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    if (maxConnectionsPerMinute <= 0) {
      super.channelActive(ctx);
      return;
    }

    if (ctx.channel() instanceof QuicChannel) {
      QuicChannel quicChannel = (QuicChannel) ctx.channel();
      SocketAddress remoteSocketAddress = quicChannel.remoteSocketAddress();

      if (remoteSocketAddress instanceof InetSocketAddress) {
        String ip = ((InetSocketAddress) remoteSocketAddress).getAddress().getHostAddress();
        int percentIdx = ip.indexOf('%');
        if (percentIdx >= 0) {
          ip = ip.substring(0, percentIdx);
        }
        if (ip.startsWith("::ffff:") || ip.startsWith("::FFFF:")) {
          ip = ip.substring(7);
        }

        if (Boolean.TRUE.equals(whitelistEngine.match((InetSocketAddress) remoteSocketAddress))) {
          if (logger.isDebugEnabled()) {
            logger.debug("✅ IP {} is whitelisted. Bypassing rate limit and blocklist.", ip);
          }
          super.channelActive(ctx);
          return;
        }

        if (blocklistFilter.isEnabled() && blocklistFilter.mightContain(ip)) {
          if (exactBlocklist.contains(ip)) {
            logger.warn(
                "❌ IP {} is in the BloomFilter Blocklist. Dropping connection immediately.", ip);
            ctx.close();
            return;
          }
        }

        long nowMinute = System.currentTimeMillis() / 60000;
        if (nowMinute != currentMinute) {
          if (clearing.compareAndSet(false, true)) {
            try {
              ipCounts.clear();
              currentMinute = nowMinute;
            } finally {
              clearing.set(false);
            }
          }
        }

        int effectiveMax = maxConnectionsPerMinute;
        Integer overrideMax = overridesEngine.match((InetSocketAddress) remoteSocketAddress);
        if (overrideMax != null) {
          effectiveMax = overrideMax;
        }

        ConnectionCount count = ipCounts.get(ip);
        if (count == null) {
          if (ipCounts.size() >= maxTrackedIps) {
            logger.warn(
                "❌ Rate Limiter State Table Full ({} entries). Dropping connection from IP: {}.",
                maxTrackedIps,
                ip);
            ctx.close();
            return;
          }
          count = ipCounts.computeIfAbsent(ip, k -> new ConnectionCount());
        }
        int current = count.incrementAndGet();

        if (current > effectiveMax) {
          logger.warn(
              "❌ Rate Limit Exceeded for IP: {} ({} connections > {} allowed). Closing QUIC"
                  + " connection immediately.",
              ip,
              current,
              effectiveMax);
          ctx.close();
          return;
        }
      }
    }
    super.channelActive(ctx);
  }

  private static class ConnectionCount {
    private final AtomicInteger count = new AtomicInteger(0);

    public int incrementAndGet() {
      return count.incrementAndGet();
    }
  }
}
