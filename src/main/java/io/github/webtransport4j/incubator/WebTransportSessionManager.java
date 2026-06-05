package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicChannel;
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
        if (connectStream.attr(WebTransportUtils.SESSION_ID_KEY) != null) {
            connectStream.attr(WebTransportUtils.SESSION_ID_KEY).set(sessionStreamId);
        }

        QuicChannel quic = (QuicChannel) connectStream.parent();
        
        Long uniMax = quic != null && quic.attr(WebTransportUtils.SETTINGS_MAX_STREAMS_UNI) != null 
                ? quic.attr(WebTransportUtils.SETTINGS_MAX_STREAMS_UNI).get() : null;
        Long biMax = quic != null && quic.attr(WebTransportUtils.SETTINGS_MAX_STREAMS_BIDI) != null 
                ? quic.attr(WebTransportUtils.SETTINGS_MAX_STREAMS_BIDI).get() : null;
        Long dataMax = quic != null && quic.attr(WebTransportUtils.SETTINGS_MAX_DATA) != null 
                ? quic.attr(WebTransportUtils.SETTINGS_MAX_DATA).get() : null;
        boolean flowControlEnabled = false;
        if ((uniMax != null && uniMax > 0L) || (biMax != null && biMax > 0L) || (dataMax != null && dataMax > 0L)) {
            flowControlEnabled = true;
        }
        if((uniMax == null || uniMax == 0L) && flowControlEnabled) {
            uniMax = 100L;
            WebTransportUtils.sendMaxStreamsCapsule(connectStream, false, uniMax);
        }
        if((biMax == null || biMax == 0L) && flowControlEnabled){
            biMax = 100L;
            WebTransportUtils.sendMaxStreamsCapsule(connectStream, true, biMax);
        }
        if((dataMax == null || dataMax == 0L) && flowControlEnabled){
            dataMax = 10000L;
            WebTransportUtils.sendMaxDataCapsule(connectStream, dataMax);
        }

        // Create the session state
        long uniMaxVal = uniMax != null ? uniMax : 0L;
        long biMaxVal = biMax != null ? biMax : 0L;
        long dataMaxVal = dataMax != null ? dataMax : 0L;
        WebTransportSession session = new WebTransportSession(sessionStreamId, connectStream, uniMaxVal, biMaxVal, dataMaxVal, flowControlEnabled);

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
    public void unregister(QuicStreamChannel connecStreamChannel) {
        long sessionStreamId = connecStreamChannel.streamId();
        WebTransportSession removed = sessions.remove(sessionStreamId);
        if (removed != null) {
            for (QuicStreamChannel activeStream : removed.getActiveBi()) {
                activeStream.close();
            }
            for (QuicStreamChannel activeStream : removed.getActiveUni()) {
                activeStream.close();
            }
            logger.debug("🗑️ SessionManager: Removed Session ID " + sessionStreamId);
        }
    }

    /**
     * Closes a specific session with WT_FLOW_CONTROL_ERROR (0x045d4487).
     */
    public void closeSessionWithFlowControlError(long sessionId) {
        WebTransportSession session = sessions.remove(sessionId);
        if (session != null) {
            logger.info("❌ Closing CONNECT stream for session " + sessionId + " with WT_FLOW_CONTROL_ERROR (0x045d4487)");
            session.getConnectStream().shutdown(0x045d4487, session.getConnectStream().newPromise());
        }
    }

    /**
     * Cleanup: Called when the main QUIC Connection is lost/closed.
     * Prevents memory leaks by clearing the map.
     */
    public void closeAllWithFlowControlError() {
        for (WebTransportSession session : sessions.values()) {
            logger.info("❌ Closing CONNECT stream for session " + session.getSessionStreamId() + " with WT_FLOW_CONTROL_ERROR (0x045d4487)");
            session.getConnectStream().shutdown(0x045d4487, session.getConnectStream().newPromise());
            if (session.getConnectStream().parent() != null) {
                session.getConnectStream().parent().close();
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