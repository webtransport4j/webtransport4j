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

  /**
   * Attempts to reserve a session slot on the connection, taking into account whether
   * flow control has been negotiated. Per draft-ietf-webtrans-http3-16 Section 3 and 5,
   * if flow control is not established (or peer settings have not yet arrived),
   * at most 1 session is permitted per connection regardless of configured limits.
   *
   * @param quic the underlying QuicChannel, or null
   * @param configuredLimit the configured max sessions per connection
   * @return true if reservation was successful, false if limit reached
   */
  public boolean reserveSession(@Nullable QuicChannel quic, int configuredLimit) {
    int effectiveLimit = configuredLimit;
    if (effectiveLimit > 1 && !isFlowControlNegotiated(quic)) {
      effectiveLimit = 1;
    }
    return reserveSession(effectiveLimit);
  }

  /**
   * Checks whether WebTransport session flow control is negotiated on the underlying QUIC
   * connection.
   *
   * <p>Per draft-ietf-webtrans-http3-16 Section 5.1, flow control is enabled only when both
   * endpoints declare their intent to use flow control by sending non-zero initial stream or data
   * limits in their HTTP/3 SETTINGS frame.
   *
   * @param quic the underlying QuicChannel, or null
   * @return true if flow control is enabled and peer settings have been received; false otherwise
   */
  public static boolean isFlowControlNegotiated(@Nullable QuicChannel quic) {
    if (quic == null) {
      return false;
    }
    Attribute<Boolean> peerReceivedAttr =
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED);
    if (peerReceivedAttr == null || !Boolean.TRUE.equals(peerReceivedAttr.get())) {
      return false;
    }

    Attribute<Long> localUniAttr =
        quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI);
    Attribute<Long> localBidiAttr =
        quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI);
    Attribute<Long> localDataAttr =
        quic.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA);

    Attribute<Long> peerUniAttr =
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI);
    Attribute<Long> peerBidiAttr =
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI);
    Attribute<Long> peerDataAttr =
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA);

    Long localUni = localUniAttr != null ? localUniAttr.get() : null;
    Long localBidi = localBidiAttr != null ? localBidiAttr.get() : null;
    Long localData = localDataAttr != null ? localDataAttr.get() : null;

    Long peerUni = peerUniAttr != null ? peerUniAttr.get() : null;
    Long peerBidi = peerBidiAttr != null ? peerBidiAttr.get() : null;
    Long peerData = peerDataAttr != null ? peerDataAttr.get() : null;

    boolean localFlowControlDeclared = (localUni != null && localUni > 0L)
        || (localBidi != null && localBidi > 0L)
        || (localData != null && localData > 0L);
    boolean peerFlowControlDeclared = (peerUni != null && peerUni > 0L)
        || (peerBidi != null && peerBidi > 0L)
        || (peerData != null && peerData > 0L);

    return localFlowControlDeclared && peerFlowControlDeclared;
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

    Long peerUni = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI) != null
        ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).get()
        : null;
    Long peerBidi = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI) != null
        ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).get()
        : null;
    Long peerData = quic != null && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA) != null
        ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).get()
        : null;

    Boolean peerSettingsReceived = quic != null
        && quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED) != null
        ? quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).get()
        : null;

    // Per draft-ietf-webtrans-http3-16 Section 5.1:
    // Flow control is enabled when BOTH endpoints declare their intent to use flow control
    // by sending any of the initial stream or data limits with a value other than "0".
    boolean localFlowControlDeclared = (uniMax != null && uniMax > 0L)
        || (biMax != null && biMax > 0L)
        || (dataMax != null && dataMax > 0L);
    boolean peerFlowControlDeclared = (peerUni != null && peerUni > 0L)
        || (peerBidi != null && peerBidi > 0L)
        || (peerData != null && peerData > 0L);
    boolean flowControlEnabled = Boolean.TRUE.equals(peerSettingsReceived)
        ? (localFlowControlDeclared && peerFlowControlDeclared)
        : (peerSettingsReceived == null ? localFlowControlDeclared : false);

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

  /**
   * Closes a specific session with WT_FLOW_CONTROL_ERROR (0x045d4487).
   *
   * @param sessionId the session stream ID
   * @return true if the session was found and closed, false otherwise
   */
  public boolean closeSessionWithFlowControlError(long sessionId) {
    WebTransportSession session = sessions.get(sessionId);
    if (session != null) {
      closeSessionWithFlowControlError(session);
      return true;
    }
    return false;
  }

  /**
   * Closes a specific session with WT_FLOW_CONTROL_ERROR (0x045d4487).
   *
   * @param session the session to close
   */
  public void closeSessionWithFlowControlError(@NonNull WebTransportSession session) {
    session.setCloseCode(WebTransportUtils.WT_FLOW_CONTROL_ERROR);
    logger.info(
        "❌ Closing CONNECT stream for session {} with WT_FLOW_CONTROL_ERROR (0x045d4487)",
        session.getSessionStreamId());
    try {
      session
          .getConnectStream()
          .shutdown(
              WebTransportUtils.WT_FLOW_CONTROL_ERROR, session.getConnectStream().newPromise());
    } catch (Exception e) {
      logger.warn("Error sending shutdown on session {}", session.getSessionStreamId(), e);
    }
    session.close();
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
