package io.github.webtransport4j.cluster;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Tests for {@link LocalClusterBroadcastBridge}.
 */
public class ClusterBroadcastBridgeTest {

  @Test
  public void testPublishAndSubscribe() throws Exception {
    final LocalClusterBroadcastBridge bridge = new LocalClusterBroadcastBridge();
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    try (AutoCloseable sub =
        bridge.subscribe(
            "chat-room-1", payload -> received.add(new String(payload, StandardCharsets.UTF_8)))) {

      bridge.publish("chat-room-1", "hello cluster".getBytes(StandardCharsets.UTF_8));
      bridge.publish("other-topic", "ignore me".getBytes(StandardCharsets.UTF_8));

      assertEquals(1, received.size());
      assertEquals("hello cluster", received.get(0));
    }

    // After close, no further messages received
    bridge.publish("chat-room-1", "second message".getBytes(StandardCharsets.UTF_8));
    assertEquals(1, received.size());
  }

  @Test
  public void testMultipleSubscribers() throws Exception {
    final LocalClusterBroadcastBridge bridge = new LocalClusterBroadcastBridge();
    final List<String> sub1 = new ArrayList<>();
    final List<String> sub2 = new ArrayList<>();

    try (AutoCloseable s1 =
            bridge.subscribe("updates", b -> sub1.add(new String(b, StandardCharsets.UTF_8)));
        AutoCloseable s2 =
            bridge.subscribe("updates", b -> sub2.add(new String(b, StandardCharsets.UTF_8)))) {

      bridge.publish("updates", "v1.0".getBytes(StandardCharsets.UTF_8));
      assertEquals(1, sub1.size());
      assertEquals(1, sub2.size());
      assertEquals("v1.0", sub1.get(0));
      assertEquals("v1.0", sub2.get(0));
    }
  }
}
