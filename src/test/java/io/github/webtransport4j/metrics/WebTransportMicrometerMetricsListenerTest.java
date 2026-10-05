package io.github.webtransport4j.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test verifying {@link WebTransportMicrometerMetricsListener} meter registration and
 * callbacks.
 */
public class WebTransportMicrometerMetricsListenerTest {

  @Test
  public void testAsyncAdapterPreservesSessionIdentityAcrossConnections() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    WebTransportMicrometerMetricsListener metrics =
        new WebTransportMicrometerMetricsListener("async");
    metrics.bindTo(registry);
    try {
      try (io.github.webtransport4j.api.AsyncWebTransportMetricsListener adapter =
          new io.github.webtransport4j.api.AsyncWebTransportMetricsListener(metrics)) {
        adapter.onSessionOpened(0, 101, "/first");
        adapter.onSessionOpened(0, 102, "/second");
        adapter.onSessionClosed(0, 101, 0);
        adapter.onSessionClosed(0, 102, 0);
      }
      assertEquals(2, registry.get("async.session.duration").timer().count());
      assertEquals(0.0, registry.get("async.sessions.active").gauge().value(), 0.0);
    } finally {
      registry.close();
    }
  }

  @Test
  public void testMultipleRegistriesReceiveAllCallbacksAndBindingIsIdempotent() {
    WebTransportMicrometerMetricsListener metrics =
        new WebTransportMicrometerMetricsListener("multi");
    SimpleMeterRegistry first = new SimpleMeterRegistry();
    SimpleMeterRegistry second = new SimpleMeterRegistry();
    try {
      metrics.bindTo(first);
      metrics.bindTo(second);
      metrics.bindTo(first);
      metrics.onSessionOpened(0, 101, "/test");
      metrics.onSessionOpened(0, 102, "/test");
      metrics.onDatagramSent(0, 16);
      metrics.onStreamOpened(0, 4, true);
      metrics.onStreamClosed(0, 4);
      metrics.onSessionClosed(0, 101, 0);
      metrics.onSessionClosed(0, 102, 0);
      for (SimpleMeterRegistry registry : new SimpleMeterRegistry[] {first, second}) {
        assertEquals(0.0, registry.get("multi.sessions.active").gauge().value(), 0.0);
        assertEquals(2.0, registry.get("multi.sessions.opened").counter().count(), 0.0);
        assertEquals(2, registry.get("multi.session.duration").timer().count());
        assertEquals(1.0, registry.get("multi.datagrams.sent").counter().count(), 0.0);
        assertEquals(16.0, registry.get("multi.datagram.sent.bytes").summary().totalAmount(), 0.0);
        assertEquals(1.0, registry.get("multi.streams.closed").counter().count(), 0.0);
      }
    } finally {
      first.close();
      second.close();
    }
  }

  private MeterRegistry registry;
  private WebTransportMicrometerMetricsListener listener;

  /** Initializes meter registry and listener. */
  @Before
  public void setUp() {
    registry = new SimpleMeterRegistry();
    listener = new WebTransportMicrometerMetricsListener("test.wt");
    listener.bindTo(registry);
  }

  @Test
  public void testSessionLifecycleMetrics() {
    Gauge activeGauge = registry.find("test.wt.sessions.active").gauge();
    assertNotNull(activeGauge);
    assertEquals(0.0, activeGauge.value(), 0.001);

    listener.onSessionOpened(1L, "/chat");
    assertEquals(1.0, activeGauge.value(), 0.001);
    assertEquals(1, listener.getActiveSessions());

    Counter openedCounter = registry.find("test.wt.sessions.opened").tag("path", "/chat").counter();
    assertNotNull(openedCounter);
    assertEquals(1.0, openedCounter.count(), 0.001);

    listener.onSessionClosed(1L, 0);
    assertEquals(0.0, activeGauge.value(), 0.001);
    assertEquals(0, listener.getActiveSessions());

    Counter closedCounter = registry.find("test.wt.sessions.closed").tag("status", "0").counter();
    assertNotNull(closedCounter);
    assertEquals(1.0, closedCounter.count(), 0.001);
  }

  @Test
  public void testMeterCardinalityUnderDistinctTraffic() {
    for (int i = 0; i < 1000; i++) {
      listener.onSessionOpened(i, "/session/" + i);
      listener.onSessionClosed(i, i);
    }
    assertEquals(101, registry.find("test.wt.sessions.opened").counters().size());
    assertEquals(2, registry.find("test.wt.sessions.closed").counters().size());
    assertEquals(0, listener.getActiveSessions());
  }

  @Test
  public void testStreamMetrics() {
    Gauge streamGauge = registry.find("test.wt.streams.active").gauge();
    assertNotNull(streamGauge);
    assertEquals(0.0, streamGauge.value(), 0.001);

    listener.onStreamOpened(1L, 4L, true);
    assertEquals(1.0, streamGauge.value(), 0.001);
    Counter bidiOpened = registry.find("test.wt.streams.opened").tag("type", "bidi").counter();
    assertNotNull(bidiOpened);
    assertEquals(1.0, bidiOpened.count(), 0.001);

    listener.onStreamOpened(1L, 8L, false);
    assertEquals(2.0, streamGauge.value(), 0.001);
    Counter uniOpened = registry.find("test.wt.streams.opened").tag("type", "uni").counter();
    assertNotNull(uniOpened);
    assertEquals(1.0, uniOpened.count(), 0.001);

    listener.onStreamClosed(1L, 4L);
    assertEquals(1.0, streamGauge.value(), 0.001);
    Counter closed = registry.find("test.wt.streams.closed").counter();
    assertNotNull(closed);
    assertEquals(1.0, closed.count(), 0.001);
  }

  @Test
  public void testDatagramMetrics() {
    listener.onDatagramSent(1L, 256);
    listener.onDatagramSent(1L, 512);
    Counter sentCounter = registry.find("test.wt.datagrams.sent").counter();
    assertNotNull(sentCounter);
    assertEquals(2.0, sentCounter.count(), 0.001);

    DistributionSummary sentBytes = registry.find("test.wt.datagram.sent.bytes").summary();
    assertNotNull(sentBytes);
    assertEquals(2L, sentBytes.count());
    assertEquals(768.0, sentBytes.totalAmount(), 0.001);

    listener.onDatagramReceived(1L, 128);
    Counter recvCounter = registry.find("test.wt.datagrams.received").counter();
    assertNotNull(recvCounter);
    assertEquals(1.0, recvCounter.count(), 0.001);

    listener.onDatagramDiscarded(1L, "queue_full");
    Counter dropCounter = registry.find("test.wt.datagrams.dropped").counter();
    assertNotNull(dropCounter);
    assertEquals(1.0, dropCounter.count(), 0.001);

    listener.onConnectionMigration(1L, "192.168.1.1:5000", "10.0.0.1:6000");
    Counter migratedCounter = registry.find("test.wt.connections.migrated").counter();
    assertNotNull(migratedCounter);
    assertEquals(1.0, migratedCounter.count(), 0.001);
  }

  @Test
  public void testSanitizePathBoundedCardinality() {
    StringBuilder sb = new StringBuilder("/");
    for (int i = 0; i < 100; i++) {
      sb.append('a');
    }
    String longPath = sb.toString();
    listener.onSessionOpened(2L, longPath);

    // Bounded to 64 chars
    String expectedBounded = longPath.substring(0, 64);
    Counter longCounter =
        registry.find("test.wt.sessions.opened").tag("path", expectedBounded).counter();
    assertNotNull(longCounter);
    assertEquals(1.0, longCounter.count(), 0.001);

    // Invalid path without leading slash gets normalized to "/"
    listener.onSessionOpened(3L, "invalid-no-slash");
    Counter fallbackCounter = registry.find("test.wt.sessions.opened").tag("path", "/").counter();
    assertNotNull(fallbackCounter);
    assertEquals(1.0, fallbackCounter.count(), 0.001);
  }
}
