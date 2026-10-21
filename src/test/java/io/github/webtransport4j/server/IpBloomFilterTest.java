package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests for {@link IpBloomFilter}. */
public class IpBloomFilterTest {

  @Test
  public void testLargeCapacityDoesNotOverflow() {
    // 50,000,000 expected elements with low false positive rate exceeds Integer.MAX_VALUE in naive math
    IpBloomFilter filter = new IpBloomFilter(50_000_000, 0.000000001);
    assertFalse(filter.isEnabled());
    filter.add("192.168.1.1");
    assertTrue(filter.isEnabled());
    assertTrue(filter.mightContain("192.168.1.1"));
    assertFalse(filter.mightContain("10.0.0.1"));
  }

  @Test
  public void testBasicBloomFilterBehavior() {
    IpBloomFilter filter = new IpBloomFilter(1000, 0.01);
    assertFalse(filter.isEnabled());
    assertFalse(filter.mightContain("127.0.0.1"));

    filter.add("127.0.0.1");
    filter.add("10.0.0.5");
    assertTrue(filter.isEnabled());
    assertTrue(filter.mightContain("127.0.0.1"));
    assertTrue(filter.mightContain("10.0.0.5"));
    assertFalse(filter.mightContain("8.8.8.8"));

    // Null and empty strings do not enable or break the filter
    filter.add(null);
    filter.add("   ");
    assertTrue(filter.mightContain("127.0.0.1"));
  }

  @Test
  public void testInvalidParametersFallbackGracefully() {
    IpBloomFilter zeroElements = new IpBloomFilter(0, 0.01);
    assertFalse(zeroElements.isEnabled());

    IpBloomFilter invalidFpp = new IpBloomFilter(100, 0.0);
    assertFalse(invalidFpp.isEnabled());
  }
}
