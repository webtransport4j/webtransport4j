package io.github.webtransport4j.api;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;

/** Codec interface for stream message encoding/decoding. */
public interface StreamCodec<T> {

  @NonNull WebTransportBuffer encode(@NonNull T message);

  void decode(@NonNull WebTransportBuffer incoming, @NonNull Consumer<T> consumer);

  default void release(@NonNull T message) {
    // Implementation can release the message if necessary
  }

  default void close() {}
}
