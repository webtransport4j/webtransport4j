package io.github.webtransport4j.server;

import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.handler.ipfilter.IpSubnetFilterRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class NettyLinearIpFilterEngine<V> implements IpFilterEngine<V> {
    private static final Logger logger = LoggerFactory.getLogger(NettyLinearIpFilterEngine.class);

    private static class RuleEntry<V> {
        final IpSubnetFilterRule rule;
        final V value;

        RuleEntry(IpSubnetFilterRule rule, V value) {
            this.rule = rule;
            this.value = value;
        }
    }

    private final List<RuleEntry<V>> rules = new ArrayList<>();

    @Override
    public void addRule(String cidrOrIp, V value) {
        try {
            IpSubnetFilterRule rule;
            if (cidrOrIp.contains("/")) {
                String[] parts = cidrOrIp.split("/");
                rule = new IpSubnetFilterRule(parts[0], Integer.parseInt(parts[1]), IpFilterRuleType.ACCEPT);
            } else {
                rule = new IpSubnetFilterRule(cidrOrIp, cidrOrIp.contains(":") ? 128 : 32, IpFilterRuleType.ACCEPT);
            }
            rules.add(new RuleEntry<>(rule, value));
        } catch (Exception e) {
            logger.error("Failed to parse IP rule in NettyLinearIpFilterEngine: {}", cidrOrIp, e);
        }
    }

    @Override
    public V match(InetSocketAddress address) {
        for (RuleEntry<V> entry : rules) {
            if (entry.rule.matches(address)) {
                return entry.value;
            }
        }
        return null;
    }

    @Override
    public V match(String ip) {
        return match(new InetSocketAddress(ip, 0));
    }
}
