package io.github.webtransport4j.server;

import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IpFilterEnginesTest {

    @Test
    public void testIpv4EdgeCases() {
        IpFilterEngine<Integer> nettyEngine = new NettyLinearIpFilterEngine<>();
        IpFilterEngine<Integer> trieEngine = new IpPrefixTrieEngine<>();

        // /32 matches exact
        nettyEngine.addRule("192.168.1.100/32", 2);
        trieEngine.addRule("192.168.1.100/32", 2);

        // /8 matches subnet
        nettyEngine.addRule("10.0.0.0/8", 3);
        trieEngine.addRule("10.0.0.0/8", 3);

        // /0 matches everything
        nettyEngine.addRule("0.0.0.0/0", 1);
        trieEngine.addRule("0.0.0.0/0", 1);

        assertMatch(nettyEngine, trieEngine, "8.8.8.8", 1);
        assertMatch(nettyEngine, trieEngine, "192.168.1.100", 2);
        assertMatch(nettyEngine, trieEngine, "10.255.255.255", 3);
        assertMatch(nettyEngine, trieEngine, "10.0.0.1", 3);
    }

    @Test
    public void testIpv6EdgeCases() {
        IpFilterEngine<Integer> nettyEngine = new NettyLinearIpFilterEngine<>();
        IpFilterEngine<Integer> trieEngine = new IpPrefixTrieEngine<>();

        // localhost (this is /128)
        nettyEngine.addRule("::1", 40);
        trieEngine.addRule("::1", 40);

        // /128 matches exact
        nettyEngine.addRule("2001:db8::1/128", 20);
        trieEngine.addRule("2001:db8::1/128", 20);

        // Standard /64
        nettyEngine.addRule("2001:db8:abcd:0012::/64", 30);
        trieEngine.addRule("2001:db8:abcd:0012::/64", 30);

        // /0 matches everything (IPv6 wildcard)
        nettyEngine.addRule("::/0", 10);
        trieEngine.addRule("::/0", 10);

        assertMatch(nettyEngine, trieEngine, "2001:4860:4860::8888", 10);
        assertMatch(nettyEngine, trieEngine, "2001:db8:0:0:0:0:0:1", 20);
        assertMatch(nettyEngine, trieEngine, "2001:db8:abcd:0012:ffff:ffff:ffff:ffff", 30);
        assertMatch(nettyEngine, trieEngine, "0:0:0:0:0:0:0:1", 40);
    }

    @Test
    public void testLongestPrefixMatch() {
        IpFilterEngine<Integer> nettyEngine = new NettyLinearIpFilterEngine<>();
        IpFilterEngine<Integer> trieEngine = new IpPrefixTrieEngine<>();

        // Add overlapping rules
        // Note: NettyLinearIpFilterEngine evaluates linearly in order of addition. 
        // A real longest-prefix match (like the Trie) will pick the most specific regardless of order.
        // To make both yield the same result, we MUST add the most specific rule first for Netty's linear scan,
        // because Netty returns the first match it finds.
        nettyEngine.addRule("192.168.1.50/32", 100); 
        nettyEngine.addRule("192.168.1.0/24", 200);
        nettyEngine.addRule("192.168.0.0/16", 300);

        trieEngine.addRule("192.168.0.0/16", 300); // Insert order doesn't matter for trie
        trieEngine.addRule("192.168.1.50/32", 100);
        trieEngine.addRule("192.168.1.0/24", 200);

        assertMatch(nettyEngine, trieEngine, "192.168.1.50", 100);
        assertMatch(nettyEngine, trieEngine, "192.168.1.51", 200);
        assertMatch(nettyEngine, trieEngine, "192.168.2.1", 300);
        assertMatch(nettyEngine, trieEngine, "10.0.0.1", null);
    }

    @Test
    public void testMalformedInputsIgnoredGracefully() {
        IpFilterEngine<Integer> nettyEngine = new NettyLinearIpFilterEngine<>();
        IpFilterEngine<Integer> trieEngine = new IpPrefixTrieEngine<>();

        nettyEngine.addRule("invalid-ip", 1);
        trieEngine.addRule("invalid-ip", 1);
        
        nettyEngine.addRule("999.999.999.999/32", 2);
        trieEngine.addRule("999.999.999.999/32", 2);

        nettyEngine.addRule("fe80:::1/128", 3);
        trieEngine.addRule("fe80:::1/128", 3);

        assertMatch(nettyEngine, trieEngine, "1.1.1.1", null);
    }

    private void assertMatch(IpFilterEngine<Integer> netty, IpFilterEngine<Integer> trie, String ip, Integer expected) {
        InetSocketAddress address = new InetSocketAddress(ip, 0);
        assertEquals("Netty engine failed for " + ip, expected, netty.match(address));
        assertEquals("Trie engine failed for " + ip, expected, trie.match(address));
    }
}
