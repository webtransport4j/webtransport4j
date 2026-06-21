package io.github.webtransport4j.api;

import io.github.webtransport4j.example.StreamCodec;
import io.github.webtransport4j.server.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class WebTransportStream {
  private final QuicStreamChannel channel;
  private final long sessionId;
  private final long streamId;
  private final boolean bidirectional;
  private Consumer<ByteBuf> dataConsumer;
  private Runnable closeHandler;
  private Consumer<Throwable> errorHandler;
  private final java.util.Map<String, Object> attributes = new java.util.concurrent.ConcurrentHashMap<>();

  public WebTransportStream(QuicStreamChannel channel, long sessionId) {
    this.channel = channel;
    this.sessionId = sessionId;
    this.streamId = channel.streamId();
    this.bidirectional = (channel.type() == io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL);
  }

  public long sessionId() {
    return sessionId;
  }

  public long streamId() {
    return streamId;
  }

  public boolean isBidirectional() {
    return bidirectional;
  }

  public QuicStreamChannel channel() {
    return channel;
  }

  public void onData(Consumer<ByteBuf> consumer) {

    if (this.dataConsumer != null) {
        throw new IllegalStateException(
            "onData handler already registered");
    }

    this.dataConsumer = consumer;
  }

  public <T> void onData(
        StreamCodec<T> codec,
        Consumer<T> consumer) {

    // Create the auto-releasing wrapper once per stream instead of once per chunk
    // to prevent unnecessary garbage collection overhead in high-throughput streams.
    Consumer<T> autoReleasingConsumer = msg -> {
        try {
            consumer.accept(msg);
        } finally {
            // safeRelease prevents IllegalReferenceCountExceptions from masking
            // primary exceptions if the user manually released it already.
            io.netty.util.ReferenceCountUtil.safeRelease(msg);
        }
    };

    this.onData(data -> {
        codec.decode(data, autoReleasingConsumer);
    });
  }

  public void onClose(Runnable handler) {
    this.closeHandler = handler;
  }

  public void onError(Consumer<Throwable> handler) {
    this.errorHandler = handler;
  }

  public Consumer<ByteBuf> getDataConsumer() {
    return dataConsumer;
  }

  public Runnable getCloseHandler() {
    return closeHandler;
  }

  public Consumer<Throwable> getErrorHandler() {
    return errorHandler;
  }

  public Future<Void> write(ByteBuf data) {
    return channel.writeAndFlush(data);
  }

  public Future<Void> writeText(String text) {
    return channel.writeAndFlush(Unpooled.copiedBuffer(text, CharsetUtil.UTF_8));
  }

  public void close() {
    channel.close();
  }

  public void reset(long appErrorCode) {
    WebTransportUtils.resetStream(channel, appErrorCode);
  }

  public boolean hasAttribute(String key) {
      return attributes.containsKey(key);
  }

  public Object setAttribute(String key, Object value) {
      if (value == null) {
          return attributes.remove(key);
      }
      return attributes.put(key, value);
  }

  public <T> T getAttribute(String key, Class<T> type) {
      Object value = attributes.get(key);
      return value == null ? null : type.cast(value);
  }

  public <T> T getAttributeOrDefault(
          String key,
          Class<T> type,
          T defaultValue) {
      Object value = attributes.get(key);
      return value == null ? defaultValue : type.cast(value);
  }

  public Object removeAttribute(String key) {
      return attributes.remove(key);
  }

  public void clearAttributes() {
      attributes.clear();
  }

  public int attributeCount() {
      return attributes.size();
  }

  public boolean hasAttributes() {
      return !attributes.isEmpty();
  }

  public Set<String> attributeNames() {
      return Collections.unmodifiableSet(attributes.keySet());
  }

  public Map<String, Object> getAttributes() {
      return Collections.unmodifiableMap(attributes);
  }
}
