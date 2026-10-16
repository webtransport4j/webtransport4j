package io.github.webtransport4j.server;

import io.github.webtransport4j.server.ratelimit.LocalMemoryRateLimitBackend;
import io.github.webtransport4j.server.ratelimit.RateLimitBackend;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicChannel;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler for rate-limiting connections per IP. */
@ChannelHandler.Sharable
public class IpRateLimitingHandler extends ChannelInboundHandlerAdapter {
  private static volatile SharedRateLimitRules sharedRules = new SharedRateLimitRules();
  private static final Logger logger = LoggerFactory.getLogger(IpRateLimitingHandler.class);
  private static volatile RateLimitBackend backend = createBackend();

  public static final IpRateLimitingHandler INSTANCE =
      new IpRateLimitingHandler((RateLimitBackend) null);

  private final RateLimitBackend instanceBackend;
  private volatile SharedRateLimitRules instanceRules;

  public IpRateLimitingHandler() {
    this(createBackend());
  }

  public IpRateLimitingHandler(RateLimitBackend backend) {
    this.instanceBackend = backend;
    this.instanceRules = null;
  }

  public IpRateLimitingHandler(RateLimitBackend backend, SharedRateLimitRules rules) {
    this.instanceBackend = backend;
    this.instanceRules = rules;
  }

  public RateLimitBackend getEffectiveBackend() {
    return this.instanceBackend != null ? this.instanceBackend : backend;
  }

  public SharedRateLimitRules getEffectiveRules() {
    return this.instanceRules != null ? this.instanceRules : sharedRules;
  }

  private static RateLimitBackend createBackend() {
    String backendType =
        WebTransportConfig.get("webtransport4j.server.ratelimit.backend", "local").toLowerCase();
    if ("redis".equals(backendType)) {
      throw new IllegalArgumentException(
          "The built-in 'redis' rate limit backend stub has been removed. Distributed rate"
              + " limiting requires a non-blocking RateLimitBackend implementation; configure a"
              + " custom RateLimitBackend class name or pass an instance to WebTransportServerBuilder.");
    } else if (!"local".equals(backendType)) {
      try {
        Class<?> clazz = Class.forName(backendType);
        if (RateLimitBackend.class.isAssignableFrom(clazz)) {
          logger.info("⚡ Custom RateLimitBackend loaded: {}", backendType);
          return (RateLimitBackend) clazz.getDeclaredConstructor().newInstance();
        }
      } catch (Exception e) {
        logger.error(
            "❌ Failed to load custom RateLimitBackend class: {}. Falling back to local.",
            backendType,
            e);
      }
    }
    return new LocalMemoryRateLimitBackend();
  }

  public static void setBackend(@NonNull RateLimitBackend customBackend) {
    backend = customBackend;
  }

  public static RateLimitBackend getBackend() {
    return backend;
  }

  private static class SharedRateLimitRules {
    final int maxConnectionsPerMinute;
    final int maxTrackedIps;
    final IpFilterEngine<Boolean> whitelistEngine;
    final IpFilterEngine<Integer> overridesEngine;
    final IpBloomFilter blocklistFilter;
    final Set<String> exactBlocklist;

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
      WebTransportConfig.Snapshot config = WebTransportConfig.snapshot();
      this.maxConnectionsPerMinute =
          config.getInt("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100);
      this.maxTrackedIps = config.getInt("webtransport4j.server.ratelimit.max_tracked_ips", 100000);

      this.engineType =
          Objects.requireNonNull(
                  config.get("webtransport4j.server.ratelimit.filter_engine", "trie"))
              .toLowerCase();

      this.rawWhitelistConfig = config.getNonNull("webtransport4j.server.ratelimit.whitelist", "");
      if (previous != null
          && this.engineType.equals(previous.engineType)
          && this.rawWhitelistConfig.equals(previous.rawWhitelistConfig)) {
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

      this.rawOverridesConfig = config.getNonNull("webtransport4j.server.ratelimit.overrides", "");
      if (previous != null
          && this.engineType.equals(previous.engineType)
          && this.rawOverridesConfig.equals(previous.rawOverridesConfig)) {
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
          config.getInt("webtransport4j.server.ratelimit.blocklist.bloom_capacity", 1_000_000);
      double fpp = 0.000000001;
      String fppStr =
          config.getNonNull("webtransport4j.server.ratelimit.blocklist.bloom_fpp", "0.000000001");
      try {
        fpp = Double.parseDouble(fppStr);
      } catch (NumberFormatException e) {
        logger.warn("Invalid bloom_fpp config: {}, using default 0.000000001", fppStr);
      }
      this.bloomFpp = fpp;

      this.rawBlocklistConfig = config.getNonNull("webtransport4j.server.ratelimit.blocklist", "");

      if (previous != null
          && this.bloomCapacity == previous.bloomCapacity
          && Double.compare(this.bloomFpp, previous.bloomFpp) == 0
          && this.rawBlocklistConfig.equals(previous.rawBlocklistConfig)) {
        this.blocklistFilter = previous.blocklistFilter;
        this.exactBlocklist = previous.exactBlocklist;
      } else {
        this.blocklistFilter = new IpBloomFilter(this.bloomCapacity, this.bloomFpp);
        this.exactBlocklist = new ObjectOpenHashSet<>();
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

  /** Reloads shared rate limiting rules from configuration. */
  public static void reloadSharedConfig() {
    sharedRules = new SharedRateLimitRules(sharedRules);
    clearState();
    updateReloaderState();
  }

  /** Resets rate limiting state and rules for testing. */
  public static void resetForTest() {
    sharedRules = new SharedRateLimitRules(null);
    clearState();
  }

  private static final AtomicReference<ScheduledExecutorService> reloaderExecutor =
      new AtomicReference<>();
  private static volatile int currentIntervalSecs = -1;

  /** Clears backend rate limiting state. */
  public static void clearState() {
    backend.clear();
  }

  /** Stops the background dynamic configuration reloader. */
  public static void stopReloader() {
    ScheduledExecutorService executor = reloaderExecutor.getAndSet(null);
    if (executor != null) {
      executor.shutdownNow();
    }
    currentIntervalSecs = -1;
    clearState();
  }

  /** Updates the dynamic configuration reloader state based on configuration settings. */
  public static void updateReloaderState() {
    WebTransportConfig.Snapshot config = WebTransportConfig.snapshot();
    boolean reloadEnabled =
        config.getBoolean("webtransport4j.server.ratelimit.dynamic_reload.enabled", true);
    int reloadInterval =
        config.getInt("webtransport4j.server.ratelimit.dynamic_reload.interval_secs", 10);

    if (!reloadEnabled || reloadInterval <= 0) {
      stopReloader();
      return;
    }

    ScheduledExecutorService current = reloaderExecutor.get();
    if (current == null || current.isShutdown() || currentIntervalSecs != reloadInterval) {
      stopReloader();
      startReloader(reloadInterval);
    }
  }

  public static void ensureReloaderStarted() {
    updateReloaderState();
  }

  private static void startReloader(int reloadInterval) {
    if (reloaderExecutor.get() != null) {
      return;
    }
    ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "wt-rate-limit-reloader");
              t.setDaemon(true);
              return t;
            });
    if (reloaderExecutor.compareAndSet(null, executor)) {
      currentIntervalSecs = reloadInterval;
      executor.scheduleAtFixedRate(
          () -> {
            try {
              if (WebTransportConfig.reload()) {
                reloadSharedConfig();
              }
            } catch (Exception e) {
              logger.error("Error reloading configuration in background", e);
            }
          },
          reloadInterval,
          reloadInterval,
          TimeUnit.SECONDS);
    } else {
      executor.shutdownNow();
    }
  }

  static {
    ensureReloaderStarted();
  }

  public Set<String> getExactBlocklist() {
    return getEffectiveRules().exactBlocklist;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    SharedRateLimitRules currentRules = getEffectiveRules();
    if (currentRules.maxConnectionsPerMinute <= 0) {
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

        if (Boolean.TRUE.equals(
            currentRules.whitelistEngine.match((InetSocketAddress) remoteSocketAddress))) {
          if (logger.isDebugEnabled()) {
            logger.debug("✅ IP {} is whitelisted. Bypassing rate limit and blocklist.", ip);
          }
          super.channelActive(ctx);
          return;
        }

        if (currentRules.blocklistFilter.isEnabled()
            && currentRules.blocklistFilter.mightContain(ip)) {
          if (currentRules.exactBlocklist.contains(ip)) {
            logger.warn(
                "❌ IP {} is in the BloomFilter Blocklist. Dropping connection immediately.", ip);
            ctx.close();
            return;
          }
        }

        long nowMinute = System.currentTimeMillis() / 60000;
        int effectiveMax = currentRules.maxConnectionsPerMinute;
        Integer overrideMax =
            currentRules.overridesEngine.match((InetSocketAddress) remoteSocketAddress);
        if (overrideMax != null) {
          effectiveMax = overrideMax;
        }

        int current =
            getEffectiveBackend().incrementAndGet(ip, nowMinute, currentRules.maxTrackedIps);
        if (current == Integer.MAX_VALUE) {
          logger.warn(
              "❌ Rate Limiter State Table Full ({} entries). Dropping connection from IP: {}.",
              currentRules.maxTrackedIps,
              ip);
          ctx.close();
          return;
        }

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
}
