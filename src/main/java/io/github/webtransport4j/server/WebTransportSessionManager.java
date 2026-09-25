package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages WebTransport session lifecycle and state.
 *
 * @author <a href="https://github.com/sanjomo">...</a>
 * @date 24/12/25 1:20 am
 */
public class WebTransportSessionManager {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportSessionManager.class);

  // Key: The Session ID (which is the Stream ID of the CONNECT stream)
  // Value: The Session object containing state
  private final Map<Long, WebTransportSession> sessions = new ConcurrentHashMap<>();
  private final AtomicInteger occupiedSlots = new AtomicInteger();

  boolean reserveSession(int limit) {
    if (limit <= 0) {
      return false;
    }
    for (;;) {
      int current = occupiedSlots.get();
      if (current >= limit || current == Integer.MAX_VALUE) {
        return false;
      }
      if (occupiedSlots.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  void releaseReservation() {
    occupiedSlots.updateAndGet(c -> Math.max(0, c - 1));
  }

  int getOccupiedSlots() {
    return occupiedSlots.get();
  }

  void registerReserved(@NonNull QuicStreamChannel connectStream) {
    register(connectStream, true);
  }

  /** Called when a CONNECT webtransport request is accepted (200 OK). */
  public void register(@NonNull QuicStreamChannel connectStream) {
    register(connectStream, false);
  }

  private void register(@NonNull QuicStreamChannel connectStream, boolean reserved) {
    logger.debug("Registering started, connect-stream-id: {}", connectStream.streamId());
    long sessionStreamId = connectStream.streamId();
    if (connectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY) != null) {
      connectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionStreamId);
    }
    QuicChannel quic = connectStream.parent();
    Long uniMax = quic != null && quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI) != null
        ? quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).get()
        : null;
    Long biMax = quic != null && quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI) != null
        ? quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).get()
        : null;
    Long dataMax = quic != null && quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA) != null
        ? quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).get()
        : null;

    // Flow control is enabled if any of the settings are explicitly set to non-zero
    // values.
    // Zero values are treated as "use fallback default", not as "unlimited".
    // This allows per-deployment configuration of flow control defaults.
    boolean flowControlEnabled = (uniMax != null && uniMax > 0L)
        || (biMax != null && biMax > 0L)
        || (dataMax != null && dataMax > 0L);

    // Apply fallback defaults for any zero-valued settings when flow control is
    // enabled.
    // This ensures clients always have explicit limits, preventing
    // denial-of-service scenarios.
    if ((uniMax == null || uniMax == 0L) && flowControlEnabled) {
      uniMax = WebTransportConfig.getLong(
          "webtransport4j.webtransport.flowcontrol.fallback.streams.uni", 100L);
      if (logger.isDebugEnabled()) {
        logger.debug("Using fallback uni streams limit: {}", uniMax);
      }
      WebTransportUtils.sendMaxStreamsCapsule(connectStream, false, uniMax);
    }
    if ((biMax == null || biMax == 0L) && flowControlEnabled) {
      biMax = WebTransportConfig.getLong(
          "webtransport4j.webtransport.flowcontrol.fallback.streams.bidi", 100L);
      if (logger.isDebugEnabled()) {
        logger.debug("Using fallback bidi streams limit: {}", biMax);
      }
      WebTransportUtils.sendMaxStreamsCapsule(connectStream, true, biMax);
    }
    if ((dataMax == null || dataMax == 0L) && flowControlEnabled) {
      dataMax = WebTransportConfig.getLong(
          "webtransport4j.webtransport.flowcontrol.fallback.data", 10000L);
      if (logger.isDebugEnabled()) {
        logger.debug("Using fallback data limit: {}", dataMax);
      }
      WebTransportUtils.sendMaxDataCapsule(connectStream, dataMax);
    }

    // Create the session state
    long uniMaxVal = uniMax != null ? uniMax : 0L;
    long biMaxVal = biMax != null ? biMax : 0L;
    long dataMaxVal = dataMax != null ? dataMax : 0L;
    Long peerUni = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI) != null
        ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).get()
        : null;
    Long peerBidi = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI) != null
        ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get()
        : null;
    Long peerData = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA) != null
        ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).get()
        : null;
    long peerUniVal = peerUni != null ? peerUni : 0L;
    long peerBidiVal = peerBidi != null ? peerBidi : 0L;
    long peerDataVal = peerData != null ? peerData : 0L;
    boolean peerMaxDataNegotiated = peerData != null;

    String pathStr = "/";
    if (quic != null && quic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY) != null) {
      String resolved = quic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).get();
      if (resolved != null) {
        pathStr = resolved;
      }
    }

    WebTransportSession session = new WebTransportSession(
        sessionStreamId,
        connectStream,
        pathStr,
        uniMaxVal,
        biMaxVal,
        dataMaxVal,
        peerUniVal,
        peerBidiVal,
        peerDataVal,
        peerMaxDataNegotiated,
        flowControlEnabled);
    session.setOnClosedCallback(() -> unregister(connectStream));
    sessions.put(sessionStreamId, session);

    if (!reserved) {
      occupiedSlots.incrementAndGet();
    }

    if (quic != null) {
      Attribute<AtomicInteger> globalAttr = quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT);
      if (globalAttr != null && globalAttr.get() != null) {
        globalAttr.get().incrementAndGet();
      }
      if (!reserved) {
        Attribute<AtomicInteger> slots = quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS);
        if (slots != null && slots.get() != null) {
          slots.get().incrementAndGet();
        }
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("📝 SessionManager: Registered Session ID {}", sessionStreamId);
    }

    // Fire metrics: session opened
    try {
      WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(quic);
      if (metrics != null) {
        metrics.onSessionOpened(sessionStreamId, pathStr);
      }
    } catch (Exception e) {
      logger.error("Error firing onSessionOpened metric for session {}", sessionStreamId, e);
    }

    Attribute<WebTransportServer> serverAttr = quic != null ? quic.attr(WebTransportAttributeKeys.SERVER_KEY) : null;
    WebTransportServer server = serverAttr != null ? serverAttr.get() : null;
    WebTransportHandler handler = server != null ? server.getHandler(pathStr) : null;
    if (handler != null) {
      try {
        handler.onSessionReady(session);
      } catch (Exception e) {
        logger.error("Error in onSessionReady callback for path {}", pathStr, e);
      }
    } else if (logger.isDebugEnabled()) {
      logger.debug("No handler registered for path: {}", pathStr);
    }
  }

  /** Required by the Demux handler to validate incoming Bidi streams. */
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

  /** Removes a specific session (e.g., when the CONNECT stream is closed). */
  public void unregister(@NonNull QuicStreamChannel connectStreamChannel) {
    unregister(connectStreamChannel.streamId(), connectStreamChannel.parent());
  }

  /** Removes a specific session by its CONNECT stream ID. */
  public void unregister(long sessionStreamId) {
    unregister(sessionStreamId, null);
  }

  /**
   * Atomically removes the session and cleans up active streams, slots, and
   * counters.
   *
   * @param sessionStreamId the stream ID of the CONNECT stream
   * @param fallbackQuic    optional fallback QUIC channel if the stream is
   *                        already detached
   */
  public void unregister(long sessionStreamId, @Nullable QuicChannel fallbackQuic) {
    WebTransportSession removed = sessions.remove(sessionStreamId);
    if (removed == null) {
      return;
    }

    occupiedSlots.updateAndGet(c -> Math.max(0, c - 1));

    QuicStreamChannel connectStream = removed.getConnectStream();
    QuicChannel quic = (connectStream != null && connectStream.parent() != null)
        ? connectStream.parent()
        : fallbackQuic;

    if (quic != null) {
      Attribute<AtomicInteger> globalAttr = quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT);
      if (globalAttr != null && globalAttr.get() != null) {
        globalAttr.get().decrementAndGet();
      }
      Attribute<AtomicInteger> slots = quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS);
      if (slots != null && slots.get() != null) {
        slots.get().decrementAndGet();
      }
    }

    int closeCode = removed.getCloseCode();
    for (QuicStreamChannel activeStream : removed.getAllActiveWebTransportStreams()) {
      try {
        if (closeCode != 0) {
          activeStream
              .shutdown(closeCode, activeStream.newPromise())
              .addListener(f -> activeStream.close());
        } else {
          activeStream.close();
        }
      } catch (Exception e) {
        logger.warn(
            "Error closing active stream {} for session {}", activeStream.streamId(), sessionStreamId, e);
      }
    }

    Attribute<WebTransportServer> serverAttr = quic != null ? quic.attr(WebTransportAttributeKeys.SERVER_KEY) : null;
    WebTransportServer server = serverAttr != null ? serverAttr.get() : null;
    WebTransportHandler handler = server != null ? server.getHandler(removed.path()) : null;
    if (handler != null) {
      try {
        handler.onSessionClosed(removed);
      } catch (Exception e) {
        logger.error("Error in onSessionClosed callback for path {}", removed.path(), e);
      }
    } else if (logger.isDebugEnabled()) {
      logger.debug("No handler registered for path: {}", removed.path());
    }

    if (quic != null) {
      try {
        WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(quic);
        if (metrics != null) {
          metrics.onSessionClosed(sessionStreamId, removed.getCloseCode());
        }
      } catch (Exception e) {
        logger.error("Error in metrics onSessionClosed for session {}", sessionStreamId, e);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("🗑️ SessionManager: Removed Session ID {}", sessionStreamId);
    }
  }

  /** Closes a specific session with WT_FLOW_CONTROL_ERROR (0x045d4487). */
  public void closeSessionWithFlowControlError(long sessionId) {
    WebTransportSession session = sessions.get(sessionId);
    if (session != null) {
      session.setCloseCode(WebTransportUtils.WT_FLOW_CONTROL_ERROR);
      logger.info(
          "❌ Closing CONNECT stream for session {} with WT_FLOW_CONTROL_ERROR (0x045d4487)",
          sessionId);
      try {
        session
            .getConnectStream()
            .shutdown(
                WebTransportUtils.WT_FLOW_CONTROL_ERROR, session.getConnectStream().newPromise());
      } catch (Exception e) {
        logger.warn("Error sending shutdown on session {}", sessionId, e);
      }
      session.close();
    }
  }

  /**
   * Cleanup: Called when the main QUIC Connection is lost/closed with a flow
   * control error.
   */
  public void closeAllWithFlowControlError() {
    QuicChannel quic = null;
    for (WebTransportSession session : sessions.values()) {
      session.setCloseCode(WebTransportUtils.WT_FLOW_CONTROL_ERROR);
      if (logger.isInfoEnabled()) {
        logger.info(
            "❌ Closing CONNECT stream for session {} with WT_FLOW_CONTROL_ERROR (0x045d4487)",
            session.getSessionStreamId());
      }
      try {
        session
            .getConnectStream()
            .shutdown(
                WebTransportUtils.WT_FLOW_CONTROL_ERROR, session.getConnectStream().newPromise());
      } catch (Exception e) {
        logger.warn("Error sending shutdown for session {}", session.getSessionStreamId(), e);
      }
      if (quic == null && session.getConnectStream().parent() != null) {
        quic = session.getConnectStream().parent();
      }
    }
    if (quic != null) {
      quic.close();
    }
    closeAll(quic);
  }

  /** Closes all managed sessions. */
  public void closeAll() {
    closeAll(null);
  }

  /** Closes all managed sessions associated with a QUIC channel. */
  public void closeAll(@Nullable QuicChannel quicChannel) {
    if (sessions.isEmpty()) {
      return;
    }
    QuicChannel quic = quicChannel;
    if (quic == null) {
      for (WebTransportSession s : sessions.values()) {
        if (s != null && s.getConnectStream() != null && s.getConnectStream().parent() != null) {
          quic = s.getConnectStream().parent();
          break;
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("💥 SessionManager: Closing all active sessions due to connection close.");
    }
    for (Long sessionStreamId : sessions.keySet().toArray(new Long[0])) {
      unregister(sessionStreamId, quic);
    }
  }
}
