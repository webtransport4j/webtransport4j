package io.github.webtransport4j.cluster;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;

/**
 * In-memory local implementation of {@link ClusterBroadcastBridge} for single-node deployments
 * and testing.
 */
public class LocalClusterBroadcastBridge implements ClusterBroadcastBridge {

  private final Map<String, List<Consumer<byte[]>>> listeners = new ConcurrentHashMap<>();

  @Override
  public void publish(@NonNull String topic, byte @NonNull [] payload) {
    Objects.requireNonNull(topic, "topic must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
    final List<Consumer<byte[]>> topicListeners = listeners.get(topic);
    if (topicListeners != null) {
      for (Consumer<byte[]> listener : topicListeners) {
        try {
          listener.accept(payload);
        } catch (Exception ignored) {
          // Prevent listener exceptions from breaking publisher
        }
      }
    }
  }

  @Override
  public @NonNull AutoCloseable subscribe(
      @NonNull String topic, @NonNull Consumer<byte[]> listener) {
    Objects.requireNonNull(topic, "topic must not be null");
    Objects.requireNonNull(listener, "listener must not be null");
    final List<Consumer<byte[]>> topicListeners =
        listeners.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>());
    topicListeners.add(listener);
    return () -> topicListeners.remove(listener);
  }
}
