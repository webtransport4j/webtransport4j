package io.github.webtransport4j.cluster;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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

  @Test
  public void testSubscribersReceiveIndependentPayloadCopies() throws Exception {
    final LocalClusterBroadcastBridge bridge = new LocalClusterBroadcastBridge();
    final List<byte[]> received = new ArrayList<>();
    byte[] published = new byte[] {1, 2, 3};
    try (AutoCloseable ignored1 = bridge.subscribe("updates", payload -> {
      payload[0] = 9;
      received.add(payload);
    });
        AutoCloseable ignored2 = bridge.subscribe("updates", received::add)) {
      bridge.publish("updates", published);
    }
    assertArrayEquals(new byte[] {1, 2, 3}, published);
    assertArrayEquals(new byte[] {9, 2, 3}, received.get(0));
    assertArrayEquals(new byte[] {1, 2, 3}, received.get(1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testClosingLastSubscriberRemovesTopic() throws Exception {
    final LocalClusterBroadcastBridge bridge = new LocalClusterBroadcastBridge();
    AutoCloseable subscription = bridge.subscribe("temporary", payload -> {});
    subscription.close();

    Field field = LocalClusterBroadcastBridge.class.getDeclaredField("listeners");
    field.setAccessible(true);
    Map<String, ?> listeners = (Map<String, ?>) field.get(bridge);
    assertEquals(0, listeners.size());
  }

  @Test
  public void testRepeatedListenerSubscriptionsHaveIndependentHandles() throws Exception {
    final LocalClusterBroadcastBridge bridge = new LocalClusterBroadcastBridge();
    final List<String> received = new ArrayList<>();
    Consumer<byte[]> listener = payload -> received.add("received");
    AutoCloseable first = bridge.subscribe("duplicate", listener);
    final AutoCloseable second = bridge.subscribe("duplicate", listener);

    first.close();
    first.close();
    bridge.publish("duplicate", new byte[] {1});
    assertEquals(1, received.size());

    second.close();
    bridge.publish("duplicate", new byte[] {2});
    assertEquals(1, received.size());
  }
}
