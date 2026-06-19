package io.github.webtransport4j.incubator;

import org.apache.log4j.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;

public class RawWebTransportHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = Logger.getLogger(RawWebTransportHandler.class.getName());
    
    // Track state per handler instance (per stream)
    private boolean protocolHeaderConsumed = false;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
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
                data.markReaderIndex();

                long streamType = WebTransportUtils.readVariableLengthInt(data);
                if (streamType == -1) {
                    data.resetReaderIndex();
                    return;
                }

                long sessionId = WebTransportUtils.readVariableLengthInt(data);
                if (sessionId == -1) {
                    data.resetReaderIndex();
                    return;
                }
                QuicChannel quic = (QuicChannel) ctx.channel().parent();
                WebTransportSessionManager mgr = quic.attr(WebTransportSessionManager.WT_SESSION_MGR).get();
                if (mgr == null || !mgr.hasSession(sessionId)) {
                    if (mgr != null && ctx.channel() instanceof QuicStreamChannel) {
                        data.resetReaderIndex();
                        boolean buffered = mgr.bufferStream(sessionId, (QuicStreamChannel) ctx.channel(), data);
                        if (buffered) {
                            ctx.channel().config().setAutoRead(false);
                            return;
                        }
                    }
                    logger.warn("❌ Unknown Session ID (or buffering failed): " + sessionId);
                    data.release();
                    if (ctx.channel() instanceof QuicStreamChannel) {
                        ((QuicStreamChannel) ctx.channel()).shutdown(0x3994bd84, ctx.newPromise());
                    } else {
                        ctx.close();
                    }
                    return;
                }

                if (streamType == 0x41) {
                    logger.info("🆕 Client Initiated BIDIRECTIONAL Stream | Session: " + sessionId + " | StreamID: " + ctx.channel().id());
                } else if (streamType == 0x54) {
                    logger.info("➡️ Client Initiated UNIDIRECTIONAL Stream | Session: " + sessionId);
                } else {
                    logger.warn("❓ Unknown Stream Type: " + streamType);
                }
                
                ctx.channel().attr(WebTransportUtils.STREAM_TYPE_KEY).set(streamType);
                ctx.channel().attr(WebTransportUtils.SESSION_ID_KEY).set(sessionId);
                
                WebTransportSession session = mgr.get(sessionId);
                if (session == null) {
                    return;
                }
                boolean isBidi = (streamType == 0x41);
                long value = isBidi ? session.incrementAndGetClientInitiatedStreamsBidi() : session.incrementAndGetClientInitiatedStreamsUni();
                long maxAllowed = isBidi ? session.getSettingsMaxStreamsBidi() : session.getSettingsMaxStreamsUni();

                if (value > maxAllowed) {
                    logger.warn("❌ WebTransport stream limit exceeded for session " + sessionId + ": " + value + " > " + maxAllowed);
                    mgr.closeSessionWithFlowControlError(sessionId);
                    if (ctx.channel() instanceof QuicStreamChannel) {
                        ((QuicStreamChannel) ctx.channel()).shutdown(0x045d4487, ctx.newPromise());
                    } else {
                        ctx.close();
                    }
                    return;
                }

                logger.debug("✅ Protocol Header Consumed | Type: " + streamType + " Session: " + sessionId);
                protocolHeaderConsumed = true;

                QuicStreamChannel streamChannel = (QuicStreamChannel) ctx.channel();
                if (isBidi) {
                    session.getActiveClientInitiatedBi().add(streamChannel);
                    streamChannel.closeFuture().addListener(future -> session.getActiveClientInitiatedBi().remove(streamChannel));
                } else {
                    session.getActiveClientInitiatedUni().add(streamChannel);
                    streamChannel.closeFuture().addListener(future -> session.getActiveClientInitiatedUni().remove(streamChannel));
                }
            }

        }
        if (!data.isReadable()) {
            data.release();
            return;
        }

        logger.debug("   -> Firing Body (" + data.readableBytes() + " bytes) to App Layer...");
        
        ctx.fireChannelRead(data); //message dispatcher
    }
}