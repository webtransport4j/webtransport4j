package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

class RawWebTransportHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(RawWebTransportHandler.class);

    private static final byte HEARTBEAT_MAGIC_BYTE = (byte) WebTransportConfig.getInt("webtransport4j.server.keepalive.stream.magic.byte", 0x3F);

    private final boolean keepAliveEnabled;

    private final String keepAliveMode;

    private final byte keepAlivePingByte;

    private final byte keepAlivePongByte;

    // Track state per handler instance (per stream)
    private boolean protocolHeaderConsumed = false;

    private boolean outboundHeaderSent = false;

    private ByteBuf cumulation = null;

    public RawWebTransportHandler() {
        this.keepAliveEnabled = WebTransportConfig.getBoolean("webtransport4j.server.keepalive.enabled", true);
        this.keepAliveMode = WebTransportConfig.get("webtransport4j.server.keepalive.mode", "STRICT").toUpperCase();
        this.keepAlivePingByte = (byte) WebTransportConfig.getInt("webtransport4j.server.keepalive.ping.byte", 1);
        this.keepAlivePongByte = (byte) WebTransportConfig.getInt("webtransport4j.server.keepalive.pong.byte", 1);
    }

    @Override
    public void handlerRemoved(@NonNull ChannelHandlerContext ctx) throws Exception {
        if (cumulation != null) {
            cumulation.release();
            cumulation = null;
        }
        super.handlerRemoved(ctx);
    }

    @Override
    public void channelInactive(@NonNull ChannelHandlerContext ctx) throws Exception {
        if (cumulation != null) {
            cumulation.release();
            cumulation = null;
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Object msg) throws Exception {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        ByteBuf data = (ByteBuf) msg;
        if (ctx.channel() instanceof QuicStreamChannel) {
            long streamId = ((QuicStreamChannel) ctx.channel()).streamId();
            if (streamId == 0) {
                ctx.fireChannelRead(msg);
                return;
            }
            if (!protocolHeaderConsumed) {
                Attribute<Boolean> serverInitiatedAttr = ctx.channel().attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY);
                Boolean serverInitiated = serverInitiatedAttr != null ? serverInitiatedAttr.get() : null;
                if (Boolean.TRUE.equals(serverInitiated)) {
                    protocolHeaderConsumed = true;
                }
            }
            if (!protocolHeaderConsumed) {
                // Accumulate fragmented stream header bytes
                if (cumulation == null) {
                    cumulation = (ctx.alloc() != null) ? ctx.alloc().buffer() : io.netty.buffer.Unpooled.buffer();
                }
                cumulation.writeBytes(data);
                data.release();
                cumulation.markReaderIndex();
                long streamType = WebTransportUtils.readVariableLengthInt(cumulation);
                if (streamType == -1) {
                    cumulation.resetReaderIndex();
                    return;
                }
                long sessionId = WebTransportUtils.readVariableLengthInt(cumulation);
                if (sessionId == -1) {
                    cumulation.resetReaderIndex();
                    return;
                }
                QuicChannel quic = (QuicChannel) ctx.channel().parent();
                WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
                if (mgr == null || !mgr.hasSession(sessionId)) {
                    logger.warn("❌ Unknown Session ID: {}", sessionId);
                    // Fire metrics: datagram/stream discarded due to unknown session
                    WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(quic);
                    if (metrics != null) {
                        metrics.onDatagramDiscarded(sessionId, "unknown_session_id");
                    }
                    cumulation.release();
                    cumulation = null;
                    if (ctx.channel() instanceof QuicStreamChannel) {
                        ((QuicStreamChannel) ctx.channel()).shutdown(WebTransportUtils.WT_BUFFERED_STREAM_REJECTED, ctx.newPromise());
                    } else {
                        ctx.close();
                    }
                    return;
                }
                if (streamType == WebTransportUtils.BI_STREAM_TYPE) {
                    logger.info("🆕 Client Initiated BIDIRECTIONAL Stream | Session: {} | StreamID: {}", sessionId, ctx.channel().id());
                } else if (streamType == WebTransportUtils.UNI_STREAM_TYPE) {
                    logger.info("➡️ Client Initiated UNIDIRECTIONAL Stream | Session: {}", sessionId);
                } else {
                    logger.warn("❓ Unknown Stream Type: {}", streamType);
                }
                ctx.channel().attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).set(streamType);
                ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionId);
                WebTransportSession session = mgr.get(sessionId);
                if (session == null) {
                    cumulation.release();
                    cumulation = null;
                    return;
                }
                boolean isBidi = (streamType == WebTransportUtils.BI_STREAM_TYPE);
                long value = isBidi ? session.incrementAndGetClientInitiatedStreamsBidi() : session.incrementAndGetClientInitiatedStreamsUni();
                long maxAllowed = isBidi ? session.getSettingsMaxStreamsBidi() : session.getSettingsMaxStreamsUni();
                if (value > maxAllowed) {
                    logger.warn("❌ WebTransport stream limit exceeded for session {}: {} > {}", sessionId, value, maxAllowed);
                    mgr.closeSessionWithFlowControlError(sessionId);
                    cumulation.release();
                    cumulation = null;
                    if (ctx.channel() instanceof QuicStreamChannel) {
                        ((QuicStreamChannel) ctx.channel()).shutdown(WebTransportUtils.WT_FLOW_CONTROL_ERROR, ctx.newPromise());
                    } else {
                        ctx.close();
                    }
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("✅ Protocol Header Consumed | Type: {} Session: {}", streamType, sessionId);
                }
                protocolHeaderConsumed = true;
                QuicStreamChannel streamChannel = (QuicStreamChannel) ctx.channel();
                if (isBidi) {
                    session.getActiveClientInitiatedBi().add(streamChannel);
                    streamChannel.closeFuture().addListener(future -> session.getActiveClientInitiatedBi().remove(streamChannel));
                } else {
                    session.getActiveClientInitiatedUni().add(streamChannel);
                    streamChannel.closeFuture().addListener(future -> session.getActiveClientInitiatedUni().remove(streamChannel));
                }
                // Fire metrics: stream opened
                WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(quic);
                if (metrics != null) {
                    final long metricSessionId = sessionId;
                    final long metricStreamId = streamChannel.streamId();
                    final boolean metricBidi = isBidi;
                    metrics.onStreamOpened(metricSessionId, metricStreamId, metricBidi);
                    streamChannel.closeFuture().addListener(f -> metrics.onStreamClosed(metricSessionId, metricStreamId));
                }
                // Fire any remaining bytes in the cumulated buffer down the pipeline
                if (cumulation.isReadable()) {
                    ByteBuf remaining = cumulation.readBytes(cumulation.readableBytes());
                    cumulation.release();
                    cumulation = null;
                    data = remaining;
                } else {
                    cumulation.release();
                    cumulation = null;
                    return;
                }
            }
            if (protocolHeaderConsumed) {
                int payloadBytes = data.readableBytes();
                if (payloadBytes > 0) {
                    Attribute<Long> sessionIdAttr = ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY);
                    Long sessionId = sessionIdAttr != null ? sessionIdAttr.get() : null;
                    if (sessionId != null) {
                        QuicChannel quic = (QuicChannel) ctx.channel().parent();
                        WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
                        if (mgr != null) {
                            WebTransportSession session = mgr.get(sessionId);
                            if (session != null) {
                                session.updateLastReadTime();
                                
                                 Attribute<Boolean> isHeartbeatAttr = ctx.channel().attr(WebTransportAttributeKeys.IS_HEARTBEAT_STREAM);
                                Boolean isHeartbeat = isHeartbeatAttr != null ? isHeartbeatAttr.get() : null;
                                if (isHeartbeat == null) {
                                    if (keepAliveEnabled) {
                                        data.markReaderIndex();
                                        byte firstByte = data.readByte();
                                        if (firstByte == HEARTBEAT_MAGIC_BYTE) {
                                            if (isHeartbeatAttr != null) {
                                                isHeartbeatAttr.set(true);
                                            }
                                            isHeartbeat = true;
                                            if (logger.isDebugEnabled()) {
                                                logger.debug("💓 Intercepted Client Keep-Alive Heartbeat Stream (Session ID: {})", sessionId);
                                            }
                                        } else {
                                            if (isHeartbeatAttr != null) {
                                                isHeartbeatAttr.set(false);
                                            }
                                            isHeartbeat = false;
                                            data.resetReaderIndex();
                                        }
                                    } else {
                                        if (isHeartbeatAttr != null) {
                                            isHeartbeatAttr.set(false);
                                        }
                                        isHeartbeat = false;
                                    }
                                }

                                 if (Boolean.TRUE.equals(isHeartbeat)) {
                                     if ("ECHO".equals(keepAliveMode)) {
                                         if (data.isReadable()) {
                                             if (logger.isDebugEnabled()) {
                                                 logger.debug("📥 Received Keep-Alive PING of size {} on Session ID: {}. Echoing PONG (ECHO mode).", data.readableBytes(), sessionId);
                                             }
                                             ctx.writeAndFlush(data.retain());
                                         }
                                     } else {
                                         while (data.isReadable()) {
                                             byte b = data.readByte();
                                             if (b == keepAlivePingByte) {
                                                 if (logger.isDebugEnabled()) {
                                                     logger.debug("📥 Received Keep-Alive PING (0x{}) on Session ID: {}. Sending PONG (0x{}).", Integer.toHexString(keepAlivePingByte & 0xFF), sessionId, Integer.toHexString(keepAlivePongByte & 0xFF));
                                                 }
                                                 ByteBuf pong = ctx.alloc().buffer(1);
                                                 pong.writeByte(keepAlivePongByte);
                                                 ctx.writeAndFlush(pong);
                                             }
                                         }
                                     }
                                     data.release();
                                     return;
                                 }

                                if (session.isFlowControlEnabled()) {
                                    long localLimit = session.getSettingsMaxData();
                                    long newCumulativeReceived = session.incrementCumulativeBytesReceived(payloadBytes);
                                    // Validate that the increment didn't exceed the limit
                                    if (newCumulativeReceived > localLimit) {
                                        logger.warn("❌ Flow control: Read blocked. Cumulative received ({}) exceeds local limit ({}). Closing session.", newCumulativeReceived, localLimit);
                                        mgr.closeSessionWithFlowControlError(sessionId);
                                        data.release();
                                        ((QuicStreamChannel) ctx.channel()).shutdown(WebTransportUtils.WT_FLOW_CONTROL_ERROR, ctx.newPromise());
                                        return;
                                    }
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("Flow control: Received {} bytes, cumulative = {}/{}", payloadBytes, newCumulativeReceived, localLimit);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if (!data.isReadable()) {
            data.release();
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("   -> Firing Body ({} bytes) to App Layer...", data.readableBytes());
        }
        // message dispatcher
        ctx.fireChannelRead(data);
    }

    /**
     * Intercepts outbound write operations on the WebTransport stream. This is invoked whenever the
     * application (e.g. MessageDispatcher or custom controllers) writes data to a WebTransport stream
     * channel. It tracks and enforces WebTransport session-level flow control rules (Section 5.6 of
     * the WebTransport draft-15 spec).
     *
     * @param ctx the handler context
     * @param msg the outgoing message (normally a ByteBuf containing stream payload)
     * @param promise the channel promise for completion tracking
     */
    @Override
    public void write(@NonNull ChannelHandlerContext ctx, @NonNull Object msg, @NonNull ChannelPromise promise) throws Exception {
        // Only intercept ByteBuf payloads being written outbound on a QUIC stream channel.
        if (!(msg instanceof ByteBuf) || !(ctx.channel() instanceof QuicStreamChannel)) {
            super.write(ctx, msg, promise);
            return;
        }
        ByteBuf data = (ByteBuf) msg;
        Attribute<Boolean> serverInitiatedAttr = ctx.channel().attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY);
        Boolean serverInitiated = serverInitiatedAttr != null ? serverInitiatedAttr.get() : null;
        // For server-initiated streams, the very first write is the stream header
        // (Stream Type and Session ID). Under WebTransport specs, flow control limits
        // only apply to stream payload data and EXCLUDE the stream header bytes.
        // Therefore, we bypass flow control checks for this first write.
        if (Boolean.TRUE.equals(serverInitiated) && !outboundHeaderSent) {
            outboundHeaderSent = true;
            super.write(ctx, msg, promise);
            return;
        }
        // Retrieve the WebTransport Session ID mapped to this stream channel.
        Attribute<Long> sessionIdAttr = ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY);
        Long sessionId = sessionIdAttr != null ? sessionIdAttr.get() : null;
        if (sessionId == null) {
            super.write(ctx, msg, promise);
            return;
        }
        // Retrieve the WebTransportSessionManager attached to the parent QUIC Connection Channel.
        QuicChannel quic = (QuicChannel) ctx.channel().parent();
        WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
        if (mgr == null) {
            super.write(ctx, msg, promise);
            return;
        }
        // Retrieve the active WebTransportSession.
        WebTransportSession session = mgr.get(sessionId);
        int bytesToWrite = data.readableBytes();
        // If the session does not exist, or session-level flow control is not enabled/negotiated,
        // let the write request bypass checks and pass to the network.
        if (session == null || !session.isFlowControlEnabled()) {
            super.write(ctx, msg, promise);
            return;
        }
        long currentSent = session.getCumulativeBytesSent();
        long peerLimit = session.getPeerSettingsMaxData();
        // Enforce the session-level flow control limit.
        // If the current write would exceed the peer's total allowed sent bytes limit:
        if (currentSent + bytesToWrite > peerLimit) {
            // Peer is blocking us. Send WT_DATA_BLOCKED capsule (at most once per unique blocked limit).
            // We use CAS on lastSentDataBlockedLimit to avoid sending duplicate capsules for the same
            // peer limit value. This implements RFC 9000 flow control with WebTransport optimizations.
            long lastLimit;
            while ((lastLimit = session.getLastSentDataBlockedLimit().get()) < peerLimit) {
                if (session.getLastSentDataBlockedLimit().compareAndSet(lastLimit, peerLimit)) {
                    logger.warn("❌ Flow control: Write blocked. Cumulative sent ({}) + write ({}) exceeds peer limit ({}). Sending WT_DATA_BLOCKED.", currentSent, bytesToWrite, peerLimit);
                    WebTransportUtils.sendDataBlockedCapsule(session.getConnectStream(), peerLimit);
                    break;
                }
            }
            // Fail the write request immediately and release buffer to avoid leaks
            try {
                data.release();
            } catch (Exception ignored) {
            }
            promise.setFailure(new IOException("Flow control limit exceeded: cumulative sent (" + currentSent + ") + write (" + bytesToWrite + ") exceeds peer limit (" + peerLimit + ")"));
            return;
        }
        // If the write fits within limits, update the session's cumulative sent bytes count
        // and forward the write downstream toward the QUIC network.
        session.incrementCumulativeBytesSent(bytesToWrite);
        super.write(ctx, msg, promise);
    }
}
