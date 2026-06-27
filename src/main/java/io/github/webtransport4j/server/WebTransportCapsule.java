package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import org.jspecify.annotations.NonNull;

public class WebTransportCapsule extends DefaultByteBufHolder implements WebTransportFrame {

    private final long sessionId;

    private final long capsuleType;

    public WebTransportCapsule(long sessionId, long capsuleType, ByteBuf data) {
        super(data);
        this.sessionId = sessionId;
        this.capsuleType = capsuleType;
    }

    @Override
    public long sessionId() {
        return sessionId;
    }

    public long capsuleType() {
        return capsuleType;
    }

    @Override
    public @NonNull WebTransportCapsule copy() {
        return new WebTransportCapsule(sessionId, capsuleType, content().copy());
    }

    @Override
    public @NonNull WebTransportCapsule duplicate() {
        return new WebTransportCapsule(sessionId, capsuleType, content().duplicate());
    }

    @Override
    public @NonNull WebTransportCapsule retainedDuplicate() {
        return new WebTransportCapsule(sessionId, capsuleType, content().retainedDuplicate());
    }

    @Override
    public @NonNull WebTransportCapsule replace(@NonNull ByteBuf content) {
        return new WebTransportCapsule(sessionId, capsuleType, content);
    }

    @Override
    public @NonNull WebTransportCapsule retain() {
        super.retain();
        return this;
    }

    @Override
    public @NonNull WebTransportCapsule retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public @NonNull WebTransportCapsule touch() {
        super.touch();
        return this;
    }

    @Override
    public @NonNull WebTransportCapsule touch(@NonNull Object hint) {
        super.touch(hint);
        return this;
    }

    @Override
    public @NonNull String toString() {
        return "WebTransportCapsule(sessionId=" + sessionId + ", capsuleType=0x" + Long.toHexString(capsuleType) + ", content=" + content() + ")";
    }
}
