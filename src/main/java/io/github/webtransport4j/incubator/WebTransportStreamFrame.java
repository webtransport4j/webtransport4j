package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

public class WebTransportStreamFrame extends DefaultByteBufHolder implements WebTransportFrame {
    private final long sessionId;
    private final long streamId;
    private final boolean bidirectional;

    public WebTransportStreamFrame(long sessionId, long streamId, boolean bidirectional, ByteBuf data) {
        super(data);
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.bidirectional = bidirectional;
    }

    @Override
    public long sessionId() {
        return sessionId;
    }

    public long streamId() {
        return streamId;
    }

    public boolean isBidirectional() {
        return bidirectional;
    }

    @Override
    public WebTransportStreamFrame copy() {
        return new WebTransportStreamFrame(sessionId, streamId, bidirectional, content().copy());
    }

    @Override
    public WebTransportStreamFrame duplicate() {
        return new WebTransportStreamFrame(sessionId, streamId, bidirectional, content().duplicate());
    }

    @Override
    public WebTransportStreamFrame retainedDuplicate() {
        return new WebTransportStreamFrame(sessionId, streamId, bidirectional, content().retainedDuplicate());
    }

    @Override
    public WebTransportStreamFrame replace(ByteBuf content) {
        return new WebTransportStreamFrame(sessionId, streamId, bidirectional, content);
    }

    @Override
    public WebTransportStreamFrame retain() {
        super.retain();
        return this;
    }

    @Override
    public WebTransportStreamFrame retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public WebTransportStreamFrame touch() {
        super.touch();
        return this;
    }

    @Override
    public WebTransportStreamFrame touch(Object hint) {
        super.touch(hint);
        return this;
    }

    @Override
    public String toString() {
        return "WebTransportStreamFrame(sessionId=" + sessionId + 
               ", streamId=" + streamId + 
               ", bidirectional=" + bidirectional + 
               ", content=" + content() + ")";
    }
}
