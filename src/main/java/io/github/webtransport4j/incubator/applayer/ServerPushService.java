package io.github.webtransport4j.incubator.applayer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

/**
 * @author https://github.com/sanjomo
 * @date 20/01/26 11:11 pm
 */
public class ServerPushService {
  private static final Logger logger = Logger.getLogger(ServerPushService.class.getName());

  // Singleton Instance
  public static final ServerPushService INSTANCE = new ServerPushService();

  // The Common Registry: Key -> Sender
  private final Map<String, StreamSender> registry = new ConcurrentHashMap<>();

  public void register(String key, StreamSender sender) {
    registry.put(key, sender);
    logger.debug("✅ REGISTERED: " + key + " (Total: " + registry.size() + ")");

    // Safety: Auto-remove from map if the underlying channel closes
    sender.getStreamChannel().closeFuture().addListener(f -> unregister(key));
  }

  public void unregister(String key) {
    if (registry.remove(key) != null) {
      logger.debug("❌ UNREGISTERED: " + key);
    }
  }

  // SEND TO ONE (e.g., "user-123")
  public void sendTo(String key, String message) {
    StreamSender sender = registry.get(key);
    if (sender != null) {
      sender.send(message);
    } else {
      System.err.println("⚠️ Send failed: Key '" + key + "' not found.");
    }
  }

  // BROADCAST (Send to Everyone)
  public void broadcast(String message) {
    registry.values().forEach(sender -> sender.send(message));
  }
}
