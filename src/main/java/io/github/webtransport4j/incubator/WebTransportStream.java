package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
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
    this.dataConsumer = consumer;
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

  public WebTransportStream setAttribute(String key, Object value) {
    if (value == null) {
      attributes.remove(key);
    } else {
      attributes.put(key, value);
    }
    return this;
  }

  public Object getAttribute(String key) {
    return attributes.get(key);
  }
}
