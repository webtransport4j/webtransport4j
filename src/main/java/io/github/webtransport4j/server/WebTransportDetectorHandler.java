package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

/**
 * Detects whether a newly created QUIC bidirectional stream is:
 *
 * 1. A normal HTTP/3 request stream
 * 2. A WebTransport WT_STREAM (0x41)
 *
 * This implementation:
 * - Uses Netty's built-in cumulation via ByteToMessageDecoder
 * - Handles fragmented packets correctly
 * - Uses QUIC varint decoding instead of raw byte matching
 * - Does not consume bytes during detection
 */
final class WebTransportDetectorHandler extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportDetectorHandler.class);

    private boolean detected;

    @Override
    protected void decode(@NonNull ChannelHandlerContext ctx, @NonNull ByteBuf in, @NonNull List<Object> out) {
        if (detected) {
            if (in.isReadable()) {
                out.add(in.readRetainedSlice(in.readableBytes()));
            }
            return;
        }
        if (!in.isReadable()) {
            return;
        }
        if (logger.isDebugEnabled()) {
            if (logger.isTraceEnabled()) {
                logger.trace("📦 [SNIFFER] Bytes: {} | HEX: [{}]", in.readableBytes(), ByteBufUtil.hexDump(in));
            }
        }
        long marker = peekVarInt(in);
        // Not enough bytes yet to decode a complete QUIC varint.
        if (marker == -1) {
            return;
        }
        if (marker == WebTransportUtils.BI_STREAM_TYPE) {
            logger.info("🚀 Decision: WebTransport WT_STREAM detected (0x{})", Long.toHexString(marker));
            detected = true;
            hijackPipeline(ctx);
            if (in.isReadable()) {
                out.add(in.readRetainedSlice(in.readableBytes()));
            }
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("👉 Decision: Standard HTTP/3 request stream (first varint = 0x{})", Long.toHexString(marker));
        }
        detected = true;
        ctx.pipeline().remove(this);
        if (in.isReadable()) {
            out.add(in.readRetainedSlice(in.readableBytes()));
        }
    }

    /**
     * Peeks a QUIC variable-length integer without consuming bytes.
     *
     * Returns:
     *   - decoded value
     *   - -1 if more bytes are needed
     */
    private static long peekVarInt(@NonNull ByteBuf in) {
        in.markReaderIndex();
        try {
            return WebTransportUtils.readVariableLengthInt(in);
        } finally {
            in.resetReaderIndex();
        }
    }

    /**
     * Removes HTTP/3 request-stream handlers and keeps only the handlers
     * needed for raw WebTransport processing.
     */
    private void hijackPipeline(@NonNull ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        List<String> toRemove = new ArrayList<>();
        for (String name : p.names()) {
            ChannelHandler h = p.get(name);
            if (h == null) {
                continue;
            }
            if (h == this) {
                continue;
            }
            if (h instanceof QuicGlobalSniffer || h instanceof RawWebTransportHandler || h instanceof WebTransportStreamFrameDecoder || h instanceof MessageDispatcher || h instanceof GlobalTrafficShapingHandler || h instanceof ChannelTrafficShapingHandler) {
                continue;
            }
            toRemove.add(name);
        }
        for (String name : toRemove) {
            try {
                p.remove(name);
                if (logger.isDebugEnabled()) {
                    logger.debug("🗑 Removed: {}", name);
                }
            } catch (Exception e) {
                logger.warn("Failed removing handler: {}", name, e);
            }
        }
        p.remove(this);
    }
}
