package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.BufferedReader;
import java.io.StringReader;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link UdpErrorMetricsPoller} {@code /proc/net/snmp} parsing. */
public class UdpErrorMetricsPollerTest {

  @Test
  public void testParseUdpErrorCounters() throws Exception {
    String snmp =
        "Ip: Forwarding DefaultTTL InReceives InHdrErrors InAddrErrors ForwDatagrams\n"
            + "Ip: 1 64 12345 0 0 0\n"
            + "Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors"
            + " InCsumErrors IgnoredMulti MemErrors\n"
            + "Udp: 100 4 7 96 12 0 3 0 0\n"
            + "UdpLite: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors"
            + " InCsumErrors IgnoredMulti MemErrors\n"
            + "UdpLite: 1 0 0 1 0 0 0 0 0\n";
    long[] counters =
        UdpErrorMetricsPoller.parseUdpErrorCounters(new BufferedReader(new StringReader(snmp)));
    assertArrayEquals(new long[] {12, 7}, counters);
  }

  @Test
  public void testParseUdpErrorCountersWithoutUdpSection() throws Exception {
    String snmp = "Tcp: RtoAlgorithm RtoMin RtoMax MaxConn\nTcp: 1 200 120000 -1\n";
    assertNull(
        UdpErrorMetricsPoller.parseUdpErrorCounters(new BufferedReader(new StringReader(snmp))));
  }

  @Test
  public void testParseUdpErrorCountersMissingColumns() throws Exception {
    String snmp = "Udp: InDatagrams OutDatagrams\nUdp: 10 10\n";
    assertNull(
        UdpErrorMetricsPoller.parseUdpErrorCounters(new BufferedReader(new StringReader(snmp))));
  }
}
