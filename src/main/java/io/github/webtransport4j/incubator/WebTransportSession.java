package io.github.webtransport4j.incubator;

import io.netty.handler.codec.quic.QuicStreamChannel;

/**
 * @author https://github.com/sanjomo
 * @date 24/12/25 1:21 am
 */
public class WebTransportSession {

    public final long sessionStreamId;
    final QuicStreamChannel connectStream;

    WebTransportSession(long sessionStreamId,
                        QuicStreamChannel connectStream) {
        this.sessionStreamId = sessionStreamId;
        this.connectStream = connectStream;
    }
}
