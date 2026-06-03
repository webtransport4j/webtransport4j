package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * @author https://github.com/sanjomo
 * @date 24/12/25 1:20 am
 */

public class WebTransportSessionManager {
    private static final Logger logger = Logger.getLogger(WebTransportSessionManager.class.getName());

    // Key used to attach this manager to the Parent QUIC Channel
    public static final AttributeKey<WebTransportSessionManager> WT_SESSION_MGR = AttributeKey
            .valueOf("wt.session.manager");

    // Key: The Session ID (which is the Stream ID of the CONNECT stream)
    // Value: The Session object containing state
    private final Map<Long, WebTransportSession> sessions = new ConcurrentHashMap<>();

    public static class PendingStream {
        public final QuicStreamChannel channel;
        public final ByteBuf data;

        public PendingStream(QuicStreamChannel channel, ByteBuf data) {
            this.channel = channel;
            this.data = data;
        }
    }

    private final Map<Long, java.util.List<PendingStream>> bufferedStreams = new java.util.HashMap<>();
    private int bufferedStreamCount = 0;
    private static final int MAX_BUFFERED_STREAMS = 50;

    /**
     * Called when a CONNECT webtransport request is accepted (200 OK).
     */
    public void register(QuicStreamChannel connectStream) {
        long sessionStreamId = connectStream.streamId();
        connectStream.attr(WebTransportUtils.SESSION_ID_KEY).set(sessionStreamId);

        // Create the session state
        WebTransportSession session = new WebTransportSession(sessionStreamId, connectStream);

        sessions.put(sessionStreamId, session);
        logger.debug("📝 SessionManager: Registered Session ID " + sessionStreamId);

        // Release any buffered streams waiting for this session
        java.util.List<PendingStream> pending = bufferedStreams.remove(sessionStreamId);

        if (pending != null) {
            for (PendingStream pendingStream : pending) {
                bufferedStreamCount--;
                logger.debug("🚀 Releasing buffered stream: " + pendingStream.channel.id() + " for Session " + sessionStreamId);
                
                // Turn autoRead back on
                pendingStream.channel.config().setAutoRead(true);
                
                // Replay the buffered read on the stream's pipeline
                pendingStream.channel.pipeline().fireChannelRead(pendingStream.data);
            }
        }
    }

    public boolean bufferStream(long sessionId, QuicStreamChannel stream, ByteBuf data) {
        if (hasSession(sessionId)) {
            return false; // Already registered, do not buffer
        }

        if (bufferedStreamCount >= MAX_BUFFERED_STREAMS) {
            logger.warn("❌ Max buffered streams exceeded. Rejecting stream " + stream.id());
            return false;
        }

        if (hasSession(sessionId)) {
            return false;
        }
        bufferedStreams.computeIfAbsent(sessionId, k -> new java.util.ArrayList<>()).add(new PendingStream(stream, data.retain()));
        bufferedStreamCount++;

        logger.debug("📥 Buffered stream " + stream.id() + " waiting for Session " + sessionId + ". Total buffered: " + bufferedStreamCount);
        return true;
    }

    /**
     * Required by the Demux handler to validate incoming Bidi streams.
     */
    public boolean hasSession(long sessionStreamId) {
        return sessions.containsKey(sessionStreamId);
    }

    public WebTransportSession get(long sessionStreamId) {
        return sessions.get(sessionStreamId);
    }

    /**
     * Removes a specific session (e.g., when the CONNECT stream is closed).
     */
    public void remove(long sessionStreamId) {
        WebTransportSession removed = sessions.remove(sessionStreamId);
        if (removed != null) {
            logger.debug("🗑️ SessionManager: Removed Session ID " + sessionStreamId);
        }
    }

    /**
     * Cleanup: Called when the main QUIC Connection is lost/closed.
     * Prevents memory leaks by clearing the map.
     */
    public void closeAllWithFlowControlError() {
        for (WebTransportSession session : sessions.values()) {
            logger.info("❌ Closing CONNECT stream for session " + session.sessionStreamId + " with WT_FLOW_CONTROL_ERROR (0x045d4487)");
            session.connectStream.shutdown(0x045d4487, session.connectStream.newPromise());
            if (session.connectStream.parent() != null) {
                session.connectStream.parent().close();
            }
        }
        closeAll();
    }

    public void closeAll() {
        if (!sessions.isEmpty()) {
            logger.debug(
                    "💥 SessionManager: Closing all " + sessions.size() + " active sessions due to connection close.");
            sessions.clear();
        }
        for (java.util.List<PendingStream> pendingList : bufferedStreams.values()) {
            for (PendingStream pending : pendingList) {
                pending.data.release();
                pending.channel.close();
            }
        }
        bufferedStreams.clear();
        bufferedStreamCount = 0;
    }
}