package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/** Real file reload histories verify whole-object publication and old snapshot immutability. */
public class ConfigReloadConcurrencyTest {
  @Test
  public void rateLimitRuleConstructionUsesOneGenerationDuringAnUpdate() throws Exception {
    IpRateLimitingHandler.resetForTest();
    Field dynamic = WebTransportConfig.class.getDeclaredField("dynamicProperties");
    dynamic.setAccessible(true);
    Properties original = (Properties) dynamic.get(null);
    String prefix = "webtransport4j.server.ratelimit.";
    AtomicBoolean firstRead = new AtomicBoolean(true);
    Properties controlled =
        new Properties() {
          @Override
          public String getProperty(String key) {
            if (key.equals(prefix + "max_connections_per_ip_per_minute")
                && firstRead.compareAndSet(true, false)) {
              WebTransportConfig.setProperty(prefix + "max_tracked_ips", "20");
            }
            return super.getProperty(key);
          }
        };
    controlled.setProperty(prefix + "max_connections_per_ip_per_minute", "1");
    controlled.setProperty(prefix + "max_tracked_ips", "2");
    try {
      dynamic.set(null, controlled);
      IpRateLimitingHandler.resetForTest();
      Field rulesField = IpRateLimitingHandler.class.getDeclaredField("sharedRules");
      rulesField.setAccessible(true);
      Object rules = rulesField.get(null);
      Field capacity = rules.getClass().getDeclaredField("maxTrackedIps");
      capacity.setAccessible(true);
      assertEquals(2, capacity.getInt(rules));
      assertEquals("20", WebTransportConfig.get(prefix + "max_tracked_ips", null));
    } finally {
      dynamic.set(null, original);
      IpRateLimitingHandler.resetForTest();
    }
  }

  @Test
  public void snapshotSurvivesProgrammaticUpdatesAndRemoval() {
    String key = "webtransport4j.server.ratelimit.snapshot.programmatic";
    try {
      WebTransportConfig.setProperty(key, "1");
      WebTransportConfig.Snapshot snapshot = WebTransportConfig.snapshot();
      WebTransportConfig.setProperty(key, "2");
      assertEquals("1", snapshot.get(key, null));
      assertEquals("2", WebTransportConfig.get(key, null));
      WebTransportConfig.removeProperty(key);
      assertEquals("1", snapshot.get(key, null));
      assertEquals("missing", WebTransportConfig.get(key, "missing"));
    } finally {
      WebTransportConfig.removeProperty(key);
    }
  }

  @Test
  public void replacementPublishesCompleteSnapshotsAndLeavesPriorSnapshotIntact() throws Exception {
    Path config = Paths.get("webtransport-dynamic.properties");
    byte[] backup = Files.exists(config) ? Files.readAllBytes(config) : null;
    Field field = WebTransportConfig.class.getDeclaredField("dynamicProperties");
    field.setAccessible(true);
    Properties original = (Properties) field.get(null);
    String prefix = "webtransport4j.server.ratelimit.snapshot.";
    try {
      for (int generation = 1; generation <= 20; generation++) {
        Properties previous = (Properties) field.get(null);
        final String oldValue = previous.getProperty(prefix + "a");
        String value = String.valueOf(generation);
        Files.write(
            config,
            (prefix + "a=" + value + "\n" + prefix + "b=" + value + "\n")
                .getBytes(StandardCharsets.UTF_8));
        WebTransportConfig.reload();
        Properties snapshot = (Properties) field.get(null);
        assertNotSame(previous, snapshot);
        assertEquals(value, snapshot.getProperty(prefix + "a"));
        assertEquals(value, snapshot.getProperty(prefix + "b"));
        assertEquals(oldValue, previous.getProperty(prefix + "a"));
      }
    } finally {
      field.set(null, original);
      if (backup == null) {
        Files.deleteIfExists(config);
      } else {
        Files.write(config, backup);
      }
    }
  }
}
