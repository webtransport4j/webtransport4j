package io.github.webtransport4j.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IpRateLimitingHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(IpRateLimitingHandler.class);

    // Simplified token bucket / sliding window per minute
    private static final Map<String, ConnectionCount> ipCounts = new ConcurrentHashMap<>();
    private static volatile long currentMinute = System.currentTimeMillis() / 60000;

    private final int maxConnectionsPerMinute;
    private final IpFilterEngine<Boolean> whitelistEngine;
    private final IpFilterEngine<Integer> overridesEngine;
    private final IpBloomFilter blocklistFilter;

    public IpRateLimitingHandler() {
        this.maxConnectionsPerMinute = WebTransportConfig.getInt("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", 100);

        String engineType = WebTransportConfig.get("webtransport4j.server.ratelimit.filter_engine", "trie").toLowerCase();
        if ("netty".equals(engineType)) {
            this.whitelistEngine = new NettyLinearIpFilterEngine<>();
            this.overridesEngine = new NettyLinearIpFilterEngine<>();
        } else {
            this.whitelistEngine = new IpPrefixTrieEngine<>();
            this.overridesEngine = new IpPrefixTrieEngine<>();
        }

        String whitelistConfig = WebTransportConfig.get("webtransport4j.server.ratelimit.whitelist", "");
        for (String allowed : whitelistConfig.split(",")) {
            allowed = allowed.trim();
            if (!allowed.isEmpty()) {
                this.whitelistEngine.addRule(allowed, true);
            }
        }
                
        String overridesConfig = WebTransportConfig.get("webtransport4j.server.ratelimit.overrides", "");
        if (!overridesConfig.isEmpty()) {
            for (String override : overridesConfig.split(",")) {
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
        
        int bloomCapacity = WebTransportConfig.getInt("webtransport4j.server.ratelimit.blocklist.bloom_capacity", 1_000_000);
        double bloomFpp = 0.000000001;
        String fppStr = WebTransportConfig.get("webtransport4j.server.ratelimit.blocklist.bloom_fpp", "0.000000001");
        try {
            bloomFpp = Double.parseDouble(fppStr);
        } catch (NumberFormatException e) {
            logger.warn("Invalid bloom_fpp config: {}, using default 0.000000001", fppStr);
        }
        
        // Initialize blocklist bloom filter
        this.blocklistFilter = new IpBloomFilter(bloomCapacity, bloomFpp);
        String blocklistConfig = WebTransportConfig.get("webtransport4j.server.ratelimit.blocklist", "");
        if (!blocklistConfig.isEmpty()) {
            for (String blockedIp : blocklistConfig.split(",")) {
                this.blocklistFilter.add(blockedIp.trim());
            }
        }
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

                if (Boolean.TRUE.equals(whitelistEngine.match((InetSocketAddress) remoteSocketAddress))) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("✅ IP {} is whitelisted. Bypassing rate limit and blocklist.", ip);
                    }
                    super.channelActive(ctx);
                    return;
                }

                if (blocklistFilter.isEnabled() && blocklistFilter.mightContain(ip)) {
                    logger.warn("❌ IP {} is in the BloomFilter Blocklist. Dropping connection immediately.", ip);
                    ctx.close();
                    return;
                }

                long nowMinute = System.currentTimeMillis() / 60000;
                if (nowMinute != currentMinute) {
                    synchronized (IpRateLimitingHandler.class) {
                        if (nowMinute != currentMinute) {
                            ipCounts.clear();
                            currentMinute = nowMinute;
                        }
                    }
                }

                int effectiveMax = maxConnectionsPerMinute;
                Integer overrideMax = overridesEngine.match((InetSocketAddress) remoteSocketAddress);
                if (overrideMax != null) {
                    effectiveMax = overrideMax;
                }

                ConnectionCount count = ipCounts.computeIfAbsent(ip, k -> new ConnectionCount());
                int current = count.incrementAndGet();

                if (current > effectiveMax) {
                    logger.warn("❌ Rate Limit Exceeded for IP: {} ({} connections > {} allowed). Closing QUIC connection immediately.", ip, current, effectiveMax);
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
