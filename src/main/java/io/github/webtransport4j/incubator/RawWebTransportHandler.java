package io.github.webtransport4j.incubator;

import org.apache.log4j.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class RawWebTransportHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = Logger.getLogger(RawWebTransportHandler.class.getName());
    
    // Track state per handler instance (per stream)
    private boolean protocolHeaderConsumed = false;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (ctx.channel() instanceof io.netty.handler.codec.quic.QuicStreamChannel) {
        long streamId = ((io.netty.handler.codec.quic.QuicStreamChannel) ctx.channel()).streamId();
    
        if (streamId == 0) {
            ctx.fireChannelRead(msg);
            return;
        }
    }
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        ByteBuf data = (ByteBuf) msg;
       
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
            io.netty.handler.codec.quic.QuicChannel quic = (io.netty.handler.codec.quic.QuicChannel) ctx.channel().parent();
            WebTransportSessionManager mgr = quic.attr(WebTransportSessionManager.WT_SESSION_MGR).get();
            if (mgr == null || !mgr.hasSession(sessionId)) {
                if (mgr != null && ctx.channel() instanceof io.netty.handler.codec.quic.QuicStreamChannel) {
                    data.resetReaderIndex();
                    boolean buffered = mgr.bufferStream(sessionId, (io.netty.handler.codec.quic.QuicStreamChannel) ctx.channel(), data);
                    if (buffered) {
                        ctx.channel().config().setAutoRead(false);
                        return;
                    }
                }
                logger.warn("❌ Unknown Session ID (or buffering failed): " + sessionId);
                data.release();
                if (ctx.channel() instanceof io.netty.handler.codec.quic.QuicStreamChannel) {
                    ((io.netty.handler.codec.quic.QuicStreamChannel) ctx.channel()).shutdown(0x3994bd84, ctx.newPromise());
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
            
            logger.debug("✅ Protocol Header Consumed | Type: " + streamType + " Session: " + sessionId);
            protocolHeaderConsumed = true;
        }

       
        if (!data.isReadable()) {
            data.release();
            return;
        }

        logger.debug("   -> Firing Body (" + data.readableBytes() + " bytes) to App Layer...");
        
        ctx.fireChannelRead(data); //message dispatcher
    }
}