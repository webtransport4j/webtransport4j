package io.github.webtransport4j.api;

import io.github.webtransport4j.example.StreamCodec;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Represents a WebTransport stream. */
public interface WebTransportStream {

  /* ---------- Metadata ---------- */
  long sessionId();

  long streamId();

  boolean isBidirectional();

  /* ---------- Lifecycle ---------- */
  void close();

  void reset(long appErrorCode);

  /* ---------- Callbacks ---------- */
  void onData(@NonNull Consumer<WebTransportBuffer> consumer);

  /** On Data. */
  default <T> void onData(@NonNull StreamCodec<T> codec, @NonNull Consumer<T> consumer) {
    onData(
        buf ->
            codec.decode(
                buf,
                msg -> {
                  try {
                    consumer.accept(msg);
                  } finally {
                    codec.release(msg);
                  }
                }));
  }

  void onClose(@NonNull OnCloseListener listener);

  void onError(@NonNull Consumer<Throwable> handler);

  @Nullable Consumer<Throwable> getErrorHandler();

  @Nullable Consumer<WebTransportBuffer> getDataConsumer();

  @Nullable OnCloseListener getCloseHandler();

  /* ---------- Primitive Write ---------- */
  @NonNull CompletableFuture<Void> write(@NonNull WebTransportBuffer data);

  @NonNull CompletableFuture<Void> write(@NonNull BinarySource binarySource);

  @NonNull CompletableFuture<Void> write(@NonNull BinarySource binarySource, int chunkSize);

  /* ---------- Convenience Writes ---------- */
  @NonNull CompletableFuture<Void> write(byte @NonNull [] data);

  @NonNull CompletableFuture<Void> write(byte @NonNull [] data, int offset, int length);

  @NonNull CompletableFuture<Void> write(@NonNull ByteBuffer buffer);

  @NonNull CompletableFuture<Void> writeText(@NonNull String text);

  @NonNull CompletableFuture<Void> writeText(@NonNull String text, @NonNull Charset charset);

  /* ---------- Attributes ---------- */
  boolean hasAttribute(@NonNull String key);

  @Nullable Object setAttribute(@NonNull String key, @Nullable Object value);

  <T> @Nullable T getAttribute(@NonNull String key, @NonNull Class<T> type);

  <T> @Nullable T getAttributeOrDefault(
      @NonNull String key, @NonNull Class<T> type, @NonNull T defaultValue);

  @Nullable Object removeAttribute(@NonNull String key);

  void clearAttributes();

  int attributeCount();

  boolean hasAttributes();

  @NonNull Set<String> attributeNames();

  @NonNull Map<String, Object> getAttributes();

  boolean isActive();

  @NonNull CompletableFuture<Void> shutdown(int error);
}
