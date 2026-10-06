package io.github.webtransport4j.cluster;

import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;

/**
 * Strategy interface for inter-node message broadcasting across a distributed cluster.
 *
 * <p>Enables WebTransport server instances connected in a cluster (e.g. via Redis Pub/Sub,
 * Apache Kafka, NATS, or RabbitMQ) to fan out datagrams, session signaling, or control capsules
 * to sessions connected to other physical servers in the fleet.
 */
public interface ClusterBroadcastBridge {

  /**
   * Publishes a message payload to a distributed topic.
   *
   * @param topic cluster routing topic or channel name
   * @param payload message payload bytes
   */
  void publish(@NonNull String topic, byte @NonNull [] payload);

  /**
   * Subscribes a listener to receive messages published on a distributed topic.
   *
   * @param topic cluster routing topic or channel name
   * @param listener callback invoked when a broadcast message arrives
   * @return auto-closeable subscription handle to cancel the subscription
   */
  @NonNull AutoCloseable subscribe(@NonNull String topic, @NonNull Consumer<byte[]> listener);
}
