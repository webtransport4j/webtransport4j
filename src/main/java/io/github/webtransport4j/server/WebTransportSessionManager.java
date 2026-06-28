package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

/**
 * @author https://github.com/sanjomo
 * @date 24/12/25 1:20 am
 */
class WebTransportSessionManager {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportSessionManager.class);

    private boolean keepAliveStarted = false;
    private java.util.concurrent.ScheduledFuture<?> keepAliveFuture = null;

    // Key: The Session ID (which is the Stream ID of the CONNECT stream)
    // Value: The Session object containing state
    private final Map<Long, WebTransportSession> sessions = new ConcurrentHashMap<>();

    /**
     * Called when a CONNECT webtransport request is accepted (200 OK).
     */
    public void register(@NonNull QuicStreamChannel connectStream) {
        long sessionStreamId = connectStream.streamId();
        if (connectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY) != null) {
            connectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionStreamId);
        }
        QuicChannel quic = connectStream.parent();
        Long uniMax = quic != null && quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI) != null ? quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).get() : null;
        Long biMax = quic != null && quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI) != null ? quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).get() : null;
        Long dataMax = quic != null && quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA) != null ? quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).get() : null;
        // Flow control is enabled if any of the settings are explicitly set to non-zero values.
        // Zero values are treated as "use fallback default", not as "unlimited".
        // This allows per-deployment configuration of flow control defaults.
        boolean flowControlEnabled = (uniMax != null && uniMax > 0L) || (biMax != null && biMax > 0L) || (dataMax != null && dataMax > 0L);
        // Apply fallback defaults for any zero-valued settings when flow control is enabled.
        // This ensures clients always have explicit limits, preventing denial-of-service scenarios.
        if ((uniMax == null || uniMax == 0L) && flowControlEnabled) {
            uniMax = WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.fallback.streams.uni", 100L);
            if (logger.isDebugEnabled()) {
                logger.debug("Using fallback uni streams limit: {}", uniMax);
            }
            WebTransportUtils.sendMaxStreamsCapsule(connectStream, false, uniMax);
        }
        if ((biMax == null || biMax == 0L) && flowControlEnabled) {
            biMax = WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.fallback.streams.bidi", 100L);
            if (logger.isDebugEnabled()) {
                logger.debug("Using fallback bidi streams limit: {}", biMax);
            }
            WebTransportUtils.sendMaxStreamsCapsule(connectStream, true, biMax);
        }
        if ((dataMax == null || dataMax == 0L) && flowControlEnabled) {
            dataMax = WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.fallback.data", 10000L);
            if (logger.isDebugEnabled()) {
                logger.debug("Using fallback data limit: {}", dataMax);
            }
            WebTransportUtils.sendMaxDataCapsule(connectStream, dataMax);
        }
        // Create the session state
        long uniMaxVal = uniMax != null ? uniMax : 0L;
        long biMaxVal = biMax != null ? biMax : 0L;
        long dataMaxVal = dataMax != null ? dataMax : 0L;
        Long peerUni = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI) != null ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).get() : null;
        Long peerBidi = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI) != null ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get() : null;
        Long peerData = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA) != null ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).get() : null;
        long peerUniVal = peerUni != null ? peerUni : Long.MAX_VALUE;
        long peerBidiVal = peerBidi != null ? peerBidi : Long.MAX_VALUE;
        long peerDataVal = peerData != null ? peerData : Long.MAX_VALUE;
        String pathStr = null;
        if (quic != null && quic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY) != null) {
            pathStr = quic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).get();
        }
        WebTransportSession session = new WebTransportSession(sessionStreamId, connectStream, pathStr, uniMaxVal, biMaxVal, dataMaxVal, peerUniVal, peerBidiVal, peerDataVal, flowControlEnabled);
        sessions.put(sessionStreamId, session);
        
        if (quic != null) {
            io.netty.util.Attribute<java.util.concurrent.atomic.AtomicInteger> globalAttr = quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT);
            if (globalAttr != null && globalAttr.get() != null) {
                globalAttr.get().incrementAndGet();
            }
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("📝 SessionManager: Registered Session ID {}", sessionStreamId);
        }
        
        // Fire metrics: session opened
        WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(quic);
        if (metrics != null) {
            metrics.onSessionOpened(sessionStreamId, pathStr != null ? pathStr : "/");
        }
        
        if (!keepAliveStarted) {
            boolean keepAliveEnabled = WebTransportConfig.getBoolean("webtransport4j.server.keepalive.enabled", true);
            if (keepAliveEnabled && quic != null && quic.eventLoop() != null) {
                long timeoutSecs = WebTransportConfig.getLong("webtransport4j.server.keepalive.timeout.secs", 30L);
                long checkIntervalSecs = WebTransportConfig.getLong("webtransport4j.server.keepalive.interval.secs", 5L);
                long checkIntervalMs = checkIntervalSecs * 1000L;
                keepAliveFuture = quic.eventLoop().scheduleWithFixedDelay(
                    () -> checkKeepAlive(timeoutSecs * 1000L),
                    checkIntervalMs,
                    checkIntervalMs,
                    java.util.concurrent.TimeUnit.MILLISECONDS
                );
                keepAliveStarted = true;
            }
        }

        Attribute<WebTransportServer> serverAttr = quic != null ? quic.attr(WebTransportAttributeKeys.SERVER_KEY) : null;
        WebTransportServer server = serverAttr != null ? serverAttr.get() : null;
        WebTransportHandler handler = server != null ? server.getHandler(pathStr) : new WebTransportHandler() {
        };
        if (handler != null) {
            try {
                handler.onSessionReady(session);
            } catch (Exception e) {
                logger.error("Error in onSessionReady callback", e);
            }
        }
    }

    /**
     * Required by the Demux handler to validate incoming Bidi streams.
     */
    public boolean hasSession(long sessionStreamId) {
        return sessions.containsKey(sessionStreamId);
    }

    public @Nullable WebTransportSession get(long sessionStreamId) {
        return sessions.get(sessionStreamId);
    }

    public int sessionsSize() {
        return sessions.size();
    }

    public @NonNull Collection<WebTransportSession> getSessions() {
        return sessions.values();
    }

    /**
     * Removes a specific session (e.g., when the CONNECT stream is closed).
     */
    public void unregister(@NonNull QuicStreamChannel connecStreamChannel) {
        long sessionStreamId = connecStreamChannel.streamId();
        WebTransportSession removed = sessions.remove(sessionStreamId);
        if (removed != null) {
            for (QuicStreamChannel activeStream : removed.getActiveClientInitiatedBi()) {
                activeStream.close();
            }
            for (QuicStreamChannel activeStream : removed.getActiveServerInitiatedBi()) {
                activeStream.close();
            }
            for (QuicStreamChannel activeStream : removed.getActiveClientInitiatedUni()) {
                activeStream.close();
            }
            for (QuicStreamChannel activeStream : removed.getActiveServerInitiatedUni()) {
                activeStream.close();
            }
            QuicChannel quic = (QuicChannel) connecStreamChannel.parent();
            Attribute<WebTransportServer> serverAttr = quic != null ? quic.attr(WebTransportAttributeKeys.SERVER_KEY) : null;
            WebTransportServer server = serverAttr != null ? serverAttr.get() : null;
            WebTransportHandler handler = server != null ? server.getHandler(removed.path()) : new WebTransportHandler() {
            };
            if (handler != null) {
                try {
                    handler.onSessionClosed(removed);
                } catch (Exception e) {
                    logger.error("Error in onSessionClosed callback", e);
                }
            }
            
            if (quic != null) {
                io.netty.util.Attribute<java.util.concurrent.atomic.AtomicInteger> globalAttr = quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT);
                if (globalAttr != null && globalAttr.get() != null) {
                    globalAttr.get().decrementAndGet();
                }
                // Fire metrics: session closed using the code set on the session
                WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(quic);
                if (metrics != null) {
                    metrics.onSessionClosed(sessionStreamId, removed.getCloseCode());
                }
            }
            
            if (logger.isDebugEnabled()) {
                logger.debug("🗑️ SessionManager: Removed Session ID {}", sessionStreamId);
            }
        }
    }

    /**
     * Closes a specific session with WT_FLOW_CONTROL_ERROR (0x045d4487).
     */
    public void closeSessionWithFlowControlError(long sessionId) {
        WebTransportSession session = sessions.get(sessionId);
        if (session != null) {
            session.setCloseCode(WebTransportUtils.WT_FLOW_CONTROL_ERROR);
            logger.info("❌ Closing CONNECT stream for session {} with WT_FLOW_CONTROL_ERROR (0x045d4487)", sessionId);
            session.getConnectStream().shutdown(WebTransportUtils.WT_FLOW_CONTROL_ERROR, session.getConnectStream().newPromise());
        }
    }

    /**
     * Periodic check to reap sessions that have timed out.
     */
    private void checkKeepAlive(long timeoutMs) {
        long now = System.currentTimeMillis();
        for (WebTransportSession session : sessions.values()) {
            long lastRead = session.getLastReadTime();
            if (now - lastRead > timeoutMs) {
                logger.warn("❌ Keep-Alive Timeout: Session {} has been idle for {}ms (limit: {}ms). Reaping.",
                        session.getSessionStreamId(), (now - lastRead), timeoutMs);
                session.setCloseCode(WebTransportUtils.WT_SESSION_GONE);
                session.getConnectStream().close();
            }
        }
    }

    /**
     * Cleanup: Called when the main QUIC Connection is lost/closed. Prevents memory leaks by clearing
     * the map.
     */
    public void closeAllWithFlowControlError() {
        for (WebTransportSession session : sessions.values()) {
            session.setCloseCode(WebTransportUtils.WT_FLOW_CONTROL_ERROR);
            logger.info("❌ Closing CONNECT stream for session {} with WT_FLOW_CONTROL_ERROR (0x045d4487)", session.getSessionStreamId());
            session.getConnectStream().shutdown(WebTransportUtils.WT_FLOW_CONTROL_ERROR, session.getConnectStream().newPromise());
            if (session.getConnectStream().parent() != null) {
                session.getConnectStream().parent().close();
            }
        }
    }

    public void closeAll() {
        if (!sessions.isEmpty()) {
            int count = sessions.size();
            
            // Get the first session's connection to decrement global count
            if (count > 0) {
                WebTransportSession first = sessions.values().iterator().next();
                QuicChannel quic = first.getConnectStream().parent();
                if (quic != null) {
                    io.netty.util.Attribute<java.util.concurrent.atomic.AtomicInteger> globalAttr = quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT);
                    if (globalAttr != null && globalAttr.get() != null) {
                        globalAttr.get().addAndGet(-count);
                    }
                }
            }
            
            if (keepAliveFuture != null) {
                keepAliveFuture.cancel(false);
                keepAliveFuture = null;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("💥 SessionManager: Closing all {} active sessions due to connection close.", count);
            }
            sessions.clear();
        }
    }
}
