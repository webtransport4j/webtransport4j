package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

public interface WebTransportFrame extends ByteBufHolder {
    /**
     * The session ID that this frame belongs to.
     */
    long sessionId();

    @Override
    WebTransportFrame copy();

    @Override
    WebTransportFrame duplicate();

    @Override
    WebTransportFrame retainedDuplicate();

    @Override
    WebTransportFrame replace(ByteBuf content);

    @Override
    WebTransportFrame retain();

    @Override
    WebTransportFrame retain(int increment);

    @Override
    WebTransportFrame touch();

    @Override
    WebTransportFrame touch(Object hint);
}
