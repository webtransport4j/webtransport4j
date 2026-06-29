package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.quic.QuicChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

public class WebTransportCapsuleHandler extends SimpleChannelInboundHandler<WebTransportCapsule> {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportCapsuleHandler.class);

    @Override
    protected void channelRead0(@NonNull ChannelHandlerContext ctx, @NonNull WebTransportCapsule capsule) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("\uD83D\uDD27 Received protocol capsule on EventLoop: 0x{}", Long.toHexString(capsule.capsuleType()));
        }
        if (capsule.capsuleType() == 0x2843L) {
            // Read 32-bit error code if payload is present
            long errorCode = 0;
            String errorMessage = "";
            ByteBuf content = capsule.content();
            if (content.readableBytes() >= 4) {
                errorCode = content.readUnsignedInt();
                if (content.isReadable()) {
                    errorMessage = content.toString(io.netty.util.CharsetUtil.UTF_8);
                }
            }
            logger.info("❌ CLOSE_WEBTRANSPORT_SESSION capsule received. Code: {}, Message: '{}'. Closing session.", errorCode, errorMessage);
            // Cleanly close only this WebTransport session stream
            ctx.close();
        } else /*
     * 5.6.2. WT_MAX_STREAMS Capsule
     *
     * <p>An HTTP capsule [HTTP-DATAGRAM] called WT_MAX_STREAMS is introduced to inform the peer of
     * the cumulative number of streams of a given type it is permitted to open. A WT_MAX_STREAMS
     * capsule with a type of 0x190B4D3F applies to bidirectional streams, and a WT_MAX_STREAMS
     * capsule with a type of 0x190B4D40 applies to unidirectional streams.
     *
     * <p>Note that, because Maximum Streams is a cumulative value representing the total allowed
     * number of streams, including previously closed streams, endpoints repeatedly send new
     * WT_MAX_STREAMS capsules with increasing Maximum Streams values as streams are opened.
     *
     * <p>WT_MAX_STREAMS Capsule { Type (i) = 0x190B4D3F..0x190B4D40, Length (i), Maximum Streams
     * (i), } Figure 7: WT_MAX_STREAMS Capsule Format WT_MAX_STREAMS capsules contain the following
     * field:
     *
     * <p>Maximum Streams: A count of the cumulative number of streams of the corresponding type
     * that can be opened over the lifetime of the session. This value cannot exceed 260, as it is
     * not possible to encode stream IDs larger than 262-1. An endpoint MUST NOT open more streams
     * than permitted by the current stream limit set by its peer. For instance, a server that
     * receives a unidirectional stream limit of 3 is permitted to open streams 3, 7, and 11, but
     * not stream 15.
     *
     * <p>Note that this limit includes streams that have been closed as well as those that are
     * open.
     *
     * <p>Unlike in QUIC, where MAX_STREAMS frames can be delivered in any order, WT_MAX_STREAMS
     * capsules are sent on the WebTransport session's connect stream and are delivered in order. If
     * an endpoint receives a WT_MAX_STREAMS capsule with a Maximum Streams value less than a
     * previously received value, it MUST close the WebTransport session by resetting the connect
     * stream with the WT_FLOW_CONTROL_ERROR error code.
     *
     * <p>The WT_MAX_STREAMS capsule defines special intermediary handling, as described in Section
     * 3.2 of [HTTP-DATAGRAM]. Intermediaries MUST consume WT_MAX_STREAMS capsules for flow control
     * purposes and MUST generate and send appropriate flow control signals for their limits.
     *
     * <p>Initial values for these limits MAY be communicated by sending non-zero values for
     * SETTINGS_WT_INITIAL_MAX_STREAMS_UNI and SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI.
     */
        if (capsule.capsuleType() == 0x190B4D3FL || capsule.capsuleType() == 0x190B4D40L) {
            boolean isBidi = (capsule.capsuleType() == 0x190B4D3FL);
            ByteBuf content = capsule.content();
            long maxStreams = WebTransportUtils.readVariableLengthInt(content);
            if (maxStreams != -1) {
                // RFC: "This value cannot exceed 2^60"
                if (maxStreams > (1L << 60)) {
                    logger.warn("Received WT_MAX_STREAMS exceeding 2^60 limit. Closing session.");
                    // Depending on your API, close with a protocol error here
                    ctx.close();
                    return;
                }
                QuicChannel quic = WebTransportUtils.getQuicChannel(ctx);
                if (quic != null) {
                    WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
                    if (mgr != null) {
                        WebTransportSession session = mgr.get(capsule.sessionId());
                        if (session != null) {
                            // Note: Assuming you have getters/setters for the *current* limit,
                            // not just the "initial" limit. The current limit tracks the highest
                            // cumulative value received so far.
                            long currentLimit = isBidi ? session.getPeerSettingsMaxStreamsBidi() : session.getPeerSettingsMaxStreamsUni();
                            if (maxStreams < currentLimit) {
                                // RFC: "If an endpoint receives a WT_MAX_STREAMS capsule with a Maximum
                                // Streams value less than a previously received value, it MUST close the
                                // WebTransport session by resetting the connect stream with the
                                // WT_FLOW_CONTROL_ERROR error code."
                                logger.warn("Received lower {} max streams value ({}) than previously set ({}). Closing session with WT_FLOW_CONTROL_ERROR.", isBidi ? "Bidirectional" : "Unidirectional", maxStreams, currentLimit);
                                mgr.closeSessionWithFlowControlError(capsule.sessionId());
                            } else {
                                // The new limit is the absolute cumulative value. No math is needed.
                                if (isBidi) {
                                    session.setPeerSettingsMaxStreamsBidi(maxStreams);
                                } else {
                                    session.setPeerSettingsMaxStreamsUni(maxStreams);
                                }
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Updated {} max streams to {} based on received WT_MAX_STREAMS capsule", isBidi ? "Bidirectional" : "Unidirectional", maxStreams);
                                }
                            }
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug("No session found for ID {} when processing WT_MAX_STREAMS capsule", capsule.sessionId());
                            }
                        }
                    }
                }
            }
        } else /*
     * 5.6.4. WT_MAX_DATA Capsule
     *
     * <p>An HTTP capsule [HTTP-DATAGRAM] called WT_MAX_DATA (type=0x190B4D3D) is introduced to
     * inform the peer of the maximum amount of data that can be sent on the WebTransport session as
     * a whole.
     *
     * <p>This limit counts all data that is sent on streams of the corresponding type, excluding
     * the stream header (see Section 4.2 and Section 4.3). For streams that were reset,
     * implementing WT_MAX_DATA requires that the QUIC stack provide the WebTransport implementation
     * with information about the final size of streams (see Section 4.5 of [RFC9000]).
     *
     * <p>WT_MAX_DATA Capsule { Type (i) = 0x190B4D3D, Length (i), Maximum Data (i), } Figure 9:
     * WT_MAX_DATA Capsule Format WT_MAX_DATA capsules contain the following field:
     *
     * <p>Maximum Data: A variable-length integer indicating the maximum amount of data that can be
     * sent on the entire session, in units of bytes. All data sent in WT_STREAM capsules counts
     * toward this limit. The sum of the lengths of Stream Data fields in WT_STREAM capsules MUST
     * NOT exceed the value advertised by a receiver.
     *
     * <p>Unlike in QUIC, where MAX_DATA frames can be delivered in any order, WT_MAX_DATA capsules
     * are sent on the WebTransport session's connect stream and are delivered in order. If an
     * endpoint receives a WT_MAX_DATA capsule with a Maximum Data value less than a previously
     * received value, it MUST close the WebTransport session by resetting the connect stream with
     * the WT_FLOW_CONTROL_ERROR error code.
     *
     * <p>The WT_MAX_DATA capsule defines special intermediary handling, as described in Section 3.2
     * of [HTTP-DATAGRAM]. Intermediaries MUST consume WT_MAX_DATA capsules for flow control
     * purposes and MUST generate and send appropriate flow control signals for their limits (see
     * Section 5.6.1).
     *
     * <p>The initial value for this limit MAY be communicated by sending a non-zero value for
     * SETTINGS_WT_INITIAL_MAX_DATA.
     */
        if (capsule.capsuleType() == 0x190B4D3DL) {
            ByteBuf content = capsule.content();
            long maxData = WebTransportUtils.readVariableLengthInt(content);
            if (maxData != -1) {
                QuicChannel quic = WebTransportUtils.getQuicChannel(ctx);
                if (quic != null) {
                    WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
                    if (mgr != null) {
                        WebTransportSession session = mgr.get(capsule.sessionId());
                        if (session != null) {
                            long currentPeerLimit = session.getPeerSettingsMaxData();
                            boolean isFirstUpdate = session.markPeerMaxDataCapsuleReceived();
                            if (!isFirstUpdate && maxData < currentPeerLimit) {
                                logger.info("❌ Received WT_MAX_DATA ({}) less than previous limit ({}). Closing session.", maxData, currentPeerLimit);
                                mgr.closeSessionWithFlowControlError(capsule.sessionId());
                            } else {
                                session.setPeerSettingsMaxData(maxData);
                                logger.info("\uD83D\uDCC8 WT_MAX_DATA received. Updated peer settings max data to {} for session {}", maxData, capsule.sessionId());
                            }
                        }
                    }
                }
            }
        } else /*
     * 5.6.5. WT_DATA_BLOCKED Capsule
     *
     * <p>A sender SHOULD send a WT_DATA_BLOCKED capsule (type=0x190B4D41) when it wishes to send
     * data but is unable to do so due to WebTransport session-level flow control. WT_DATA_BLOCKED
     * capsules can be used as input to tuning of flow control algorithms.
     *
     * <p>WT_DATA_BLOCKED Capsule { Type (i) = 0x190B4D41, Length (i), Maximum Data (i), } Figure
     * 10: WT_DATA_BLOCKED Capsule Format WT_DATA_BLOCKED capsules contain the following field:
     *
     * <p>Maximum Data: A variable-length integer indicating the session-level limit at which
     * blocking occurred. The WT_DATA_BLOCKED capsule defines special intermediary handling, as
     * described in Section 3.2 of [HTTP-DATAGRAM]. Intermediaries MUST consume WT_DATA_BLOCKED
     * capsules for flow control purposes and MUST generate and send appropriate flow control
     * signals for their limits (see Section 5.6.1).
     */
        if (capsule.capsuleType() == 0x190B4D41L) {
            ByteBuf content = capsule.content();
            long maxData = WebTransportUtils.readVariableLengthInt(content);
            if (maxData != -1) {
                QuicChannel quic = WebTransportUtils.getQuicChannel(ctx);
                if (quic != null) {
                    WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
                    if (mgr != null) {
                        WebTransportSession session = mgr.get(capsule.sessionId());
                        if (session != null && session.isFlowControlEnabled()) {
                            long extendAmount = WebTransportConfig.getLong("webtransport4j.webtransport.flowcontrol.extend.data", 10000L);
                            if (extendAmount > 0) {
                                long newLimit = session.getSettingsMaxData() + extendAmount;
                                session.setSettingsMaxData(newLimit);
                                logger.info("\uD83D\uDCC8 Received WT_DATA_BLOCKED. Extending local settings max data to {} (extended by {}) for session {}", newLimit, extendAmount, capsule.sessionId());
                                WebTransportUtils.sendMaxDataCapsule(session.getConnectStream(), newLimit);
                            } else {
                                logger.info("ℹ️ Received WT_DATA_BLOCKED but extend.data is 0 or negative. No extension.");
                            }
                        }
                    }
                }
            }
        } else /*
     * 5.6.3. WT_STREAMS_BLOCKED Capsule
     *
     * <p>A sender SHOULD send a WT_STREAMS_BLOCKED capsule when it wishes to open a stream but is
     * unable to do so due to the maximum stream limit set by its peer. A WT_STREAMS_BLOCKED capsule
     * with a type of 0x190B4D43 is used to indicate reaching the bidirectional stream limit, and a
     * WT_STREAMS_BLOCKED capsule with a type of 0x190B4D44 is used to indicate reaching the
     * unidirectional stream limit.
     */
        if (capsule.capsuleType() == 0x190B4D43L || capsule.capsuleType() == 0x190B4D44L) {
            boolean isBidi = (capsule.capsuleType() == 0x190B4D43L);
            ByteBuf content = capsule.content();
            long blockedAtMax = WebTransportUtils.readVariableLengthInt(content);
            if (blockedAtMax != -1) {
                QuicChannel quic = WebTransportUtils.getQuicChannel(ctx);
                if (quic != null) {
                    WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
                    if (mgr != null) {
                        WebTransportSession session = mgr.get(capsule.sessionId());
                        if (session != null) {
                            // Calculate remaining allowed active slots
                            long activeCount = isBidi ? session.getActiveClientInitiatedBi().size() : session.getActiveClientInitiatedUni().size();
                            long initialLimit = isBidi ? session.getInitialMaxStreamsBidi() : session.getInitialMaxStreamsUni();
                            long remaining = initialLimit - activeCount;
                            // Cap the remaining slots by the configured max.active.streams setting.
                            // This prevents unbounded stream limit growth in response to WT_STREAMS_BLOCKED.
                            long maxActiveStreams = WebTransportConfig.getLong(isBidi ? "webtransport4j.webtransport.flowcontrol.max.active.streams.bidi" : "webtransport4j.webtransport.flowcontrol.max.active.streams.uni", 100L);
                            long cappedRemaining = Math.min(remaining, maxActiveStreams);
                            if (cappedRemaining > 0) {
                                long newLimit = blockedAtMax + cappedRemaining;
                                if (isBidi) {
                                    session.setSettingsMaxStreamsBidi(newLimit);
                                } else {
                                    session.setSettingsMaxStreamsUni(newLimit);
                                }
                                logger.info("\uD83D\uDCC8 WT_STREAMS_BLOCKED received. Extending {} limit to {} (blocked={} + capped_remaining={} / {} max_active={})", isBidi ? "bidi" : "uni", newLimit, blockedAtMax, cappedRemaining, remaining, maxActiveStreams);
                                WebTransportUtils.sendMaxStreamsCapsule(session.getConnectStream(), isBidi, newLimit);
                            } else {
                                logger.info("ℹ️ WT_STREAMS_BLOCKED received but no remaining active slots available (active={} initial={} max_extension={}). Not extending limit.", activeCount, initialLimit, maxActiveStreams);
                            }
                        }
                    }
                }
            }
        } else {
            logger.warn("⚠️ Received unhandled protocol capsule: 0x{}", Long.toHexString(capsule.capsuleType()));
        }
    }
}
