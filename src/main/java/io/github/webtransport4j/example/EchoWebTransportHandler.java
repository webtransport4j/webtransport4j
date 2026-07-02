package io.github.webtransport4j.example;

/**
 * @author https://github.com/sanjomo
 * @date 02/07/26 9:19 pm
 */


import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple echo server.
 *
 * - Echoes every bidirectional stream back on the same stream.
 * - Echoes unidirectional streams back using a new server unidirectional stream.
 * - Echoes datagrams back as datagrams.
 */
public final class EchoWebTransportHandler implements WebTransportHandler {

    private static final Logger logger =
            LoggerFactory.getLogger(EchoWebTransportHandler.class);

    @Override
    public void onSessionReady(@NonNull WebTransportSession session) {
        logger.info("Session opened: {}", session.getSessionStreamId());
    }

    @Override
    public void onSessionClosed(@NonNull WebTransportSession session) {
        logger.info("Session closed: {}", session.getSessionStreamId());
    }

    @Override
    public void onIncomingStream(
            @NonNull WebTransportSession session,
            @NonNull WebTransportStream stream) {

        logger.info("Incoming {} stream {}",
                stream.isBidirectional() ? "bidirectional" : "unidirectional",
                stream.streamId());

        stream.onClose(() ->
                logger.info("Stream {} closed", stream.streamId()));

        stream.onError(error ->
                logger.error("Stream {} error", stream.streamId(), error));

        stream.onData(data -> {


            data.skipBytes(data.readableBytes());
        });
    }

    @Override
    public void onDatagramReceived(
            @NonNull WebTransportSession session,
            @NonNull WebTransportBuffer data) {

        // Echo datagram back
        data.skipBytes(data.readableBytes());
    }
}
