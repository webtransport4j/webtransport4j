package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import org.jspecify.annotations.NonNull;

/** Interface for WebTransport frames. */
public interface WebTransportFrame extends ByteBufHolder {

  /** The session ID that this frame belongs to. */
  long sessionId();

  @Override
  @NonNull WebTransportFrame copy();

  @Override
  @NonNull WebTransportFrame duplicate();

  @Override
  @NonNull WebTransportFrame retainedDuplicate();

  @Override
  @NonNull WebTransportFrame replace(@NonNull ByteBuf content);

  @Override
  @NonNull WebTransportFrame retain();

  @Override
  @NonNull WebTransportFrame retain(int increment);

  @Override
  @NonNull WebTransportFrame touch();

  @Override
  @NonNull WebTransportFrame touch(@NonNull Object hint);
}
