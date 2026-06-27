package io.github.webtransport4j.server;

import java.util.BitSet;

/**
 * A zero-dependency Bloom Filter implementation specifically for IP blocklists.
 * 
 * NOTE: Bloom filters are probabilistic. They can produce false positives
 * (e.g., claiming an IP is in the blocklist when it isn't), but they never
 * produce false negatives (if an IP is in the list, it will definitely be blocked).
 * 
 * To mitigate false positives blocking legitimate users, the default false positive
 * rate is tuned to 1 in 1,000,000,000 (0.000000001).
 */
public class IpBloomFilter {
    private final BitSet bitSet;
    private final int size;
    private final int numHashes;
    private boolean enabled = false;

    public IpBloomFilter(int expectedElements, double falsePositiveRate) {
        if (expectedElements <= 0) {
            this.size = 1;
            this.numHashes = 1;
            this.bitSet = new BitSet(1);
            return;
        }
        
        // Optimal size (m) and number of hash functions (k) for a Bloom filter
        this.size = (int) Math.ceil(-(expectedElements * Math.log(falsePositiveRate)) / (Math.log(2) * Math.log(2)));
        this.numHashes = (int) Math.ceil((this.size / (double) expectedElements) * Math.log(2));
        this.bitSet = new BitSet(this.size);
    }

    public void add(String ip) {
        if (ip == null || ip.trim().isEmpty()) return;
        this.enabled = true;
        for (int i = 0; i < numHashes; i++) {
            int hash = getHash(ip, i);
            bitSet.set(Math.abs(hash % size));
        }
    }

    public boolean mightContain(String ip) {
        if (!enabled) return false;
        
        for (int i = 0; i < numHashes; i++) {
            int hash = getHash(ip, i);
            if (!bitSet.get(Math.abs(hash % size))) {
                return false; // Definitely not in the blocklist
            }
        }
        return true; // Probably in the blocklist
    }
    
    public boolean isEnabled() {
        return enabled;
    }

    private int getHash(String ip, int i) {
        // Double-hashing technique to simulate k hash functions
        // Uses basic string hashing for zero external dependencies
        int hash1 = ip.hashCode();
        
        // Custom secondary hash (similar to DJB2) to avoid collision patterns of standard hashCode
        int hash2 = 5381;
        for (int j = 0; j < ip.length(); j++) {
            hash2 = ((hash2 << 5) + hash2) + ip.charAt(j);
        }
        
        return hash1 + (i * hash2);
    }
}
