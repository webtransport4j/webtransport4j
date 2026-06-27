package io.github.webtransport4j.server;

import org.jspecify.annotations.Nullable;
import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

/**
 * @author https://github.com/sanjomo
 * @date 20/01/26 10:58 pm
 */
public class WebTransportUtils {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportUtils.class);

    // WebTransport Unidirectional ID
    public static final long UNI_STREAM_TYPE = 0x54;

    // WebTransport Bidirectional
    public static final long BI_STREAM_TYPE = 0x41;

    // WebTransport HTTP/3 Error Codes (Section 9.5 of draft-15 spec)
    public static final int WT_BUFFERED_STREAM_REJECTED = 0x3994bd84;

    public static final int WT_SESSION_GONE = 0x170d7b68;

    public static final int WT_FLOW_CONTROL_ERROR = 0x045d4487;

    /**
     * Creates a new Server-Initiated Unidirectional Stream.
     *
     * @param connectStreamChannel The parent connect stream channel
     * @param byPassLimit Whether to bypass flow control stream limits
     * @param streamHandler The channel handler to add to the stream pipeline
     * @return A Future that completes with the QuicStreamChannel
     */
    public static @NonNull Future<QuicStreamChannel> createUniStream(@NonNull QuicStreamChannel connectStreamChannel, boolean byPassLimit, @NonNull ChannelHandler streamHandler) {
        return wrapFutureStreamChannel(QuicStreamType.UNIDIRECTIONAL, streamHandler, connectStreamChannel, byPassLimit);
    }

    /**
     * Creates a new Server-Initiated Bidirectional Stream.
     *
     * @param connectStreamChannel The parent connect stream channel
     * @param byPassLimit Whether to bypass flow control stream limits
     * @param streamHandler The channel handler to add to the stream pipeline
     * @return A Future that completes with the QuicStreamChannel
     */
    public static @NonNull Future<QuicStreamChannel> createBiStream(@NonNull QuicStreamChannel connectStreamChannel, boolean byPassLimit, @NonNull ChannelHandler streamHandler) {
        // 1. Request Stream Creation
        return wrapFutureStreamChannel(QuicStreamType.BIDIRECTIONAL, streamHandler, connectStreamChannel, byPassLimit);
    }

    private static @NonNull Future<QuicStreamChannel> wrapFutureStreamChannel(@NonNull QuicStreamType quicStreamType, @NonNull ChannelHandler streamHandler, @NonNull QuicStreamChannel connectStreamChannel, boolean byPassLimit) {
        Promise<QuicStreamChannel> promise = connectStreamChannel.parent().eventLoop().newPromise();
        WebTransportSessionManager mgr = connectStreamChannel.parent().attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
        WebTransportSession session = mgr != null ? mgr.get(connectStreamChannel.streamId()) : null;
        if (session == null) {
            promise.setFailure(new IllegalStateException("Session not found: " + connectStreamChannel.streamId()));
            return promise;
        }
        if (!byPassLimit) {
            long current = QuicStreamType.BIDIRECTIONAL == quicStreamType ? session.getServerInitiatedStreamsBidi() : session.getServerInitiatedStreamsUni();
            long max = QuicStreamType.BIDIRECTIONAL == quicStreamType ? session.getPeerSettingsMaxStreamsBidi() : session.getPeerSettingsMaxStreamsUni();
            if (current >= max) {
                logger.warn("❌ WebTransport stream limit exceeded for session {}: {} >= {}. Stream creation failed.", connectStreamChannel.streamId(), current, max);
                sendStreamsBlockedCapsule(session.getConnectStream(), QuicStreamType.BIDIRECTIONAL == quicStreamType, max);
                promise.setFailure(new IllegalStateException(QuicStreamType.BIDIRECTIONAL == quicStreamType ? "Bidirectional" : "Unidirectional" + " stream limit exceeded"));
                return promise;
            }
            long l = QuicStreamType.BIDIRECTIONAL == quicStreamType ? session.incrementAndGetServerInitiatedStreamsBidi() : session.incrementAndGetServerInitiatedStreamsUni();
        }
        return connectStreamChannel.parent().createStream(quicStreamType, streamHandler).addListener((Future<QuicStreamChannel> future) -> {
            if (!future.isSuccess()) {
                promise.setFailure(future.cause());
                return;
            }
            QuicStreamChannel stream = future.getNow();
            // Set channel attributes so other handlers/logs can retrieve them
            stream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(connectStreamChannel.streamId());
            stream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).set(stream.type() == QuicStreamType.BIDIRECTIONAL ? BI_STREAM_TYPE : UNI_STREAM_TYPE);
            stream.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).set(true);
            // 2. Write the Mandatory Header: [0x41] [SessionID]
            ByteBuf header = Unpooled.buffer(16);
            try {
                writeVarInt(header, stream.type() == QuicStreamType.BIDIRECTIONAL ? BI_STREAM_TYPE : UNI_STREAM_TYPE);
                writeVarInt(header, connectStreamChannel.streamId());
                stream.writeAndFlush(header);
                session.getActiveServerInitiatedBi().add(stream);
                stream.closeFuture().addListener(f -> {
                    if (stream.type() == QuicStreamType.BIDIRECTIONAL) {
                        session.getActiveServerInitiatedBi().remove(stream);
                    } else {
                        session.getActiveServerInitiatedUni().remove(stream);
                    }
                });
                promise.setSuccess(stream);
            } catch (Exception e) {
                header.release();
                stream.close();
                promise.setFailure(e);
            }
        });
    }

    public static void writeVarInt(@NonNull ByteBuf out, long value) {
        // QUIC VarInts are limited to 62 bits (max roughly 4.6 quintillion)
        // 0x3FFFFFFFFFFFFFFF is the max valid value (2^62 - 1)
        if (value < 0 || value > 0x3FFFFFFFFFFFFFFFL) {
            throw new IllegalArgumentException("Invalid QUIC VarInt: " + value);
        }
        // "LZCNT" calculation - extremely fast on modern CPUs
        int requiredBits = 64 - Long.numberOfLeadingZeros(value);
        if (requiredBits <= 6) {
            // 1 Byte (00xxxxxx)
            out.writeByte((int) value);
        } else if (requiredBits <= 14) {
            // 2 Bytes (01xxxxxx...)
            // We cast to int, but writeShort only uses the lower 16 bits
            out.writeShort((int) (value | 0x4000L));
        } else if (requiredBits <= 30) {
            // 4 Bytes (10xxxxxx...)
            out.writeInt((int) (value | 0x80000000L));
        } else {
            // 8 Bytes (11xxxxxx...)
            out.writeLong(value | 0xC000000000000000L);
        }
    }

    public static int varIntLength(long value) {
        if (value < 0 || value > 0x3FFFFFFFFFFFFFFFL) {
            throw new IllegalArgumentException("Invalid QUIC VarInt: " + value);
        }
        int requiredBits = 64 - Long.numberOfLeadingZeros(value);
        if (requiredBits <= 6) {
            return 1;
        } else if (requiredBits <= 14) {
            return 2;
        } else if (requiredBits <= 30) {
            return 4;
        } else {
            return 8;
        }
    }

    public static void writeCapsule(@NonNull ByteBuf out, long type, long value) {
        int valueLen = varIntLength(value);
        writeVarInt(out, type);
        writeVarInt(out, valueLen);
        writeVarInt(out, value);
    }

    public static long readVariableLengthInt(@NonNull ByteBuf in) {
        // 1. Quick check: Is there even 1 byte to peek?
        if (!in.isReadable()) {
            return -1;
        }
        // 2. Peek at the first byte (unsigned) to determine the length
        // We do NOT move the readerIndex yet.
        int first = in.getUnsignedByte(in.readerIndex());
        // 3. OPTIMIZATION: Calculate length using bit shifts
        // The top 2 bits (0-3) map perfectly to powers of 2 (1, 2, 4, 8)
        // 00 (0) -> 1 << 0 = 1 byte
        // 01 (1) -> 1 << 1 = 2 bytes
        // 10 (2) -> 1 << 2 = 4 bytes
        // 11 (3) -> 1 << 3 = 8 bytes
        int len = 1 << (first >> 6);
        // 4. Check if we have the full integer available
        if (in.readableBytes() < len) {
            return -1;
        }
        // 5. Read and mask (strip the prefix bits)
        switch(len) {
            case 1:
                // Prefix 00: No masking needed (value is 0-63)
                return in.readUnsignedByte();
            case 2:
                // Prefix 01: Mask with 0x3FFF (14 bits)
                return in.readShort() & 0x3FFF;
            case 4:
                // Prefix 10: Mask with 0x3FFFFFFF (30 bits)
                return in.readInt() & 0x3FFFFFFF;
            case 8:
                // Prefix 11: Mask with 0x3FFFF... (62 bits)
                return in.readLong() & 0x3FFFFFFFFFFFFFFFL;
            default:
                throw new IllegalStateException("Should not happen");
        }
    }

    private static void sendCapsule(@NonNull QuicStreamChannel connectStream, long capsuleType, long value, @NonNull String capsuleName) {
        ByteBuf buf = (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
        try {
            writeCapsule(buf, capsuleType, value);
            int totalBytes = buf.readableBytes();
            // Hex dump of raw capsule bytes (Type, Length, Value)
            String capsuleHex = io.netty.buffer.ByteBufUtil.hexDump(buf);
            // Hex dump of the actual HTTP/3 DATA frame bytes (Type=0x00, Length=totalBytes,
            // Payload=capsuleBytes)
            String frameHex = "";
            ByteBuf frameBuf = (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
            try {
                // HTTP/3 DATA Frame Type
                writeVarInt(frameBuf, 0x00);
                // Payload Length
                writeVarInt(frameBuf, totalBytes);
                frameBuf.writeBytes(buf.duplicate());
                frameHex = io.netty.buffer.ByteBufUtil.hexDump(frameBuf);
            } finally {
                frameBuf.release();
            }
            if (logger.isTraceEnabled()) {
                logger.trace("📤 Writing DefaultHttp3DataFrame with {} Capsule (Type: 0x{}, Value/Limit: {}, Total Bytes: {})", capsuleName, Long.toHexString(capsuleType), value, totalBytes);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("   ├── Capsule Bytes:      {}", capsuleHex);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("   └── HTTP/3 Frame Bytes: {}", frameHex);
            }
            ChannelFuture future = connectStream.writeAndFlush(new DefaultHttp3DataFrame(buf));
            if (future != null) {
                future.addListener(f -> {
                    if (f.isSuccess()) {
                        logger.info("✅ Capsule writeAndFlush SUCCESS (buffered/handed off to QUIC stack) for Type 0x{}", Long.toHexString(capsuleType));
                    } else {
                        logger.error("❌ Capsule writeAndFlush FAILED for Type 0x{}", Long.toHexString(capsuleType), f.cause());
                    }
                });
            } else {
                logger.info("✅ Capsule writeAndFlush called (no future returned) for Type 0x{}", Long.toHexString(capsuleType));
            }
        } catch (Exception e) {
            if (buf.refCnt() > 0) {
                buf.release();
            }
            logger.error("Failed to send {} capsule", capsuleName, e);
        }
    }

    public static void sendMaxStreamsCapsule(@NonNull QuicStreamChannel connectStream, boolean isBidi, long maxStreams) {
        long capsuleType = isBidi ? 0x190B4D3FL : 0x190B4D40L;
        sendCapsule(connectStream, capsuleType, maxStreams, "WT_MAX_STREAMS");
    }

    public static void sendMaxDataCapsule(@NonNull QuicStreamChannel connectStream, long maxData) {
        sendCapsule(connectStream, 0x190B4D3DL, maxData, "WT_MAX_DATA");
    }

    public static void sendDataBlockedCapsule(@NonNull QuicStreamChannel connectStream, long maxData) {
        sendCapsule(connectStream, 0x190B4D41L, maxData, "WT_DATA_BLOCKED");
    }

    public static void sendStreamsBlockedCapsule(@NonNull QuicStreamChannel connectStream, boolean isBidi, long maxStreams) {
        long capsuleType = isBidi ? 0x190B4D43L : 0x190B4D44L;
        sendCapsule(connectStream, capsuleType, maxStreams, "WT_STREAMS_BLOCKED");
    }

    public static final long WT_ERROR_FIRST = 0x52e4a40fa8dbL;

    public static final long WT_ERROR_LAST = 0x52e5ac983162L;

    /**
     * Maps a 32-bit unsigned WebTransport application error code to a 62-bit HTTP/3 error code as per
     * draft-15 Section 4.4.
     */
    public static long webTransportCodeToHttpCode(long n) {
        if (n < 0 || n > 0xffffffffL) {
            throw new IllegalArgumentException("WebTransport error code must be an unsigned 32-bit integer: " + n);
        }
        return WT_ERROR_FIRST + n + (n / 30L);
    }

    /**
     * Maps a 62-bit HTTP/3 error code back to a 32-bit unsigned WebTransport application error code
     * as per draft-15 Section 4.4.
     */
    public static long httpCodeToWebTransportCode(long h) {
        if (h >= WT_ERROR_FIRST && h <= WT_ERROR_LAST) {
            if ((h - 0x21L) % 31L == 0L) {
                throw new IllegalArgumentException("HTTP/3 error code is a reserved codepoint: " + h);
            }
            long shifted = h - WT_ERROR_FIRST;
            return shifted - (shifted / 31L);
        }
        // Fallback for Netty QUIC limitation where large 62-bit codes crash the JVM
        if (h >= 0 && h <= Integer.MAX_VALUE) {
            return h;
        }
        throw new IllegalArgumentException("HTTP/3 error code is outside WT_APPLICATION_ERROR range: " + h);
    }

    /**
     * Checks if a given HTTP/3 error code is within the WT_APPLICATION_ERROR range.
     */
    public static boolean isWebTransportApplicationError(long h) {
        return (h >= WT_ERROR_FIRST && h <= WT_ERROR_LAST && (h - 0x21L) % 31L != 0L) || // fallback for Netty QUIC limitation
        (h >= 0 && h <= Integer.MAX_VALUE);
    }

    public static void resetStream(@NonNull QuicStreamChannel streamChannel, long appErrorCode) {
        long httpErrorCode = webTransportCodeToHttpCode(appErrorCode);
        // Netty's QuicStreamChannel.shutdown(int) takes a signed int.
        // If we cast a 62-bit HTTP/3 error code to int and it becomes negative,
        // Netty's JNI layer sign-extends it to a 64-bit value greater than 2^62 - 1,
        // causing Quiche to panic and crash the JVM.
        // To prevent this crash under Netty QUIC's limitation, we check if the casted value is
        // negative.
        // If it is, we fall back to sending the unmapped appErrorCode (if it fits in positive int)
        // or a default fallback positive error code (like 0) to avoid crashing the JVM.
        int code = (int) httpErrorCode;
        if (code < 0) {
            code = (appErrorCode >= 0 && appErrorCode <= Integer.MAX_VALUE) ? (int) appErrorCode : 0;
        }
        streamChannel.shutdown(code, streamChannel.newPromise());
    }

    public static @NonNull String formatHexBytes(@NonNull ByteBuf buf) {
        int len = buf.readableBytes();
        if (len == 0)
            return "\n    ├── Wire Bytes: [ ]\n    └── Characters: ( )";
        StringBuilder hexLine = new StringBuilder("[ ");
        StringBuilder charLine = new StringBuilder("( ");
        int readerIndex = buf.readerIndex();
        for (int i = 0; i < len; i++) {
            byte b = buf.getByte(readerIndex + i);
            String hexStr = String.format("0x%02X", b);
            String charStr;
            if (b >= 32 && b < 127) {
                charStr = String.format("'%c'", (char) b);
            } else {
                charStr = "'.'";
            }
            int maxLen = Math.max(hexStr.length(), charStr.length());
            StringBuilder hexVal = new StringBuilder(hexStr);
            while (hexVal.length() < maxLen) {
                hexVal.append(" ");
            }
            StringBuilder charVal = new StringBuilder(charStr);
            while (charVal.length() < maxLen) {
                charVal.insert(0, " ").append(" ");
            }
            if (charVal.length() > maxLen) {
                charVal.setLength(maxLen);
            }
            hexLine.append(hexVal);
            charLine.append(charVal);
            if (i < len - 1) {
                hexLine.append(", ");
                charLine.append(", ");
            }
        }
        hexLine.append(" ]");
        charLine.append(" )");
        return String.format("\n    ├── Wire Bytes: %s\n    └── Characters: %s", hexLine, charLine);
    }

    public static @Nullable QuicChannel getQuicChannel(@NonNull ChannelHandlerContext ctx) {
        if (ctx.channel() instanceof QuicStreamChannel) {
            return ((QuicStreamChannel) ctx.channel()).parent();
        } else if (ctx.channel() instanceof QuicChannel) {
            return (QuicChannel) ctx.channel();
        }
        return null;
    }

    public static void addTrafficShapers(@NonNull QuicStreamChannel stream) {
        QuicChannel quic = stream.parent();
        GlobalTrafficShapingHandler globalTrafficShapingHandler = quic.parent() != null ? quic.parent().attr(WebTransportAttributeKeys.GLOBAL_TRAFFIC_SHAPER).get() : null;
        if (globalTrafficShapingHandler != null) {
            stream.pipeline().addFirst("global-traffic-shaper", globalTrafficShapingHandler);
            if (logger.isDebugEnabled()) {
                logger.debug("🔧 Added global traffic shaper to stream {}", stream.streamId());
            }
        }
        GlobalTrafficShapingHandler connShaper = quic.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER).get();
        if (connShaper != null) {
            stream.pipeline().addFirst("conn-traffic-shaper", connShaper);
            if (logger.isDebugEnabled()) {
                logger.debug("🔧 Added connection traffic shaper to stream {}", stream.streamId());
            }
        }
        long streamWriteLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.stream.write.limit", 0L);
        long streamReadLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.stream.read.limit", 0L);
        if (streamWriteLimit > 0 || streamReadLimit > 0) {
            ChannelTrafficShapingHandler streamShaper = new ChannelTrafficShapingHandler(streamWriteLimit, streamReadLimit);
            stream.pipeline().addFirst("stream-traffic-shaper", streamShaper);
            if (logger.isDebugEnabled()) {
                logger.debug("🔧 Added stream traffic shaper to stream {}", stream.streamId());
            }
        }
    }
}
