package io.github.webtransport4j.incubator;

/**
 * @author https://github.com/sanjomo
 * @date 20/01/26 10:58 pm
 */

import io.github.webtransport4j.incubator.applayer.ServerPushService;
import io.github.webtransport4j.incubator.applayer.StreamSender;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class WebTransportUtils {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(WebTransportUtils.class.getName());

    public static final AttributeKey<Long> SESSION_ID_KEY = AttributeKey.valueOf("wt.session.id");
    public static final AttributeKey<Long> STREAM_TYPE_KEY = AttributeKey.valueOf("wt.stream.type");

    public static final int UNI_STREAM_TYPE = 0x54; // WebTransport Unidirectional ID
    public static final int BI_STREAM_TYPE = 0x41; // WebTransport Bidirectional
    public static final AttributeKey<AtomicLong> SETTINGS_WT_INITIAL_MAX_STREAMS_UNI =
            AttributeKey.valueOf("SETTINGS_WT_INITIAL_MAX_STREAMS_UNI");

    public static final AttributeKey<java.util.concurrent.atomic.AtomicLong> SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI =
            AttributeKey.valueOf("SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI");

    public static final AttributeKey<java.util.concurrent.atomic.AtomicLong> SETTINGS_WT_INITIAL_MAX_DATA =
            AttributeKey.valueOf("SETTINGS_WT_INITIAL_MAX_DATA");

    public static final AttributeKey<AtomicLong> CURRENT_STREAMS_UNI =
            AttributeKey.valueOf("CURRENT_STREAMS_UNI");

    public static final  AttributeKey<java.util.concurrent.atomic.AtomicLong> CURRENT_STREAMS_BIDI =
            AttributeKey.valueOf("CURRENT_STREAMS_BIDI");

    public static boolean tryCreateStream(AtomicLong current, AtomicLong max) {
        if (current.get() >= max.get()) {
            logger.warn("❌ WebTransport stream limit exceeded: " + current.get() + " >= " + max.get() + ". Stream creation failed.");
            return false;
        }
        current.incrementAndGet();
        return true;
    }


    public static long incrementCounter(Channel channel,
                                         AttributeKey<AtomicLong> key) {
        AtomicLong counter = channel.attr(key).get();
        if (counter == null) {
            channel.attr(key).setIfAbsent(new AtomicLong());
            counter = channel.attr(key).get();
        }
        return counter.incrementAndGet();
    }
    public static long decrementCounter(Channel channel,
                                        AttributeKey<AtomicLong> key) {
        AtomicLong counter = channel.attr(key).get();
        if (counter == null) {
            channel.attr(key).setIfAbsent(new AtomicLong());
            counter = channel.attr(key).get();
        }
        return counter.decrementAndGet();
    }
    /**
     * Creates a new Server-Initiated Unidirectional Stream.
     * @param connection The parent QUIC Connection
     * @param sessionId The WebTransport Session ID to bind to
     * @return A Future that completes with the StreamSender
     */
    public static Future<StreamSender> createUniStream(QuicChannel connection, long sessionId, String key, Optional<Boolean> byPassLimit) {
        Promise<StreamSender> promise = connection.eventLoop().newPromise();
        if (!byPassLimit.orElse(false) && !tryCreateStream(connection.attr(CURRENT_STREAMS_UNI).get(), connection.attr(SETTINGS_WT_INITIAL_MAX_STREAMS_UNI).get())){
            promise.setFailure(new IllegalStateException("WT_FLOW_CONTROL_ERROR: Unidirectional stream limit exceeded"));
            return promise;
        }

        // 1. Request Stream Creation
        connection.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInboundHandlerAdapter())
                .addListener((Future<QuicStreamChannel> future) -> {
                    if (!future.isSuccess()) {
                        promise.setFailure(future.cause());
                        return;
                    }

                    QuicStreamChannel stream = future.getNow();
                    
                    // Set channel attributes so other handlers/logs can retrieve them
                    stream.attr(SESSION_ID_KEY).set(sessionId);
                    stream.attr(STREAM_TYPE_KEY).set((long) UNI_STREAM_TYPE);

                    // 2. Write the Mandatory Header: [0x54] [SessionID]
                    // We write this synchronously before giving the stream to the user.
                    ByteBuf header = Unpooled.buffer(16);
                    try {
                        writeVarInt(header, UNI_STREAM_TYPE);
                        writeVarInt(header, sessionId);
                        stream.writeAndFlush(header);
                        StreamSender sender = new StreamSender(stream);
                        if (key != null) {
                            ServerPushService.INSTANCE.register(key, sender);
                        }

                        promise.setSuccess(sender);
                    } catch (Exception e) {
                        header.release();
                        stream.close();
                        promise.setFailure(e);
                    }
                });

        return promise;
    }

    /**
     * Creates a new Server-Initiated Bidirectional Stream.
     * @param connection The parent QUIC Connection
     * @param sessionId The WebTransport Session ID to bind to
     * @return A Future that completes with the StreamSender
     */
    public static Future<StreamSender> createBiStream(QuicChannel connection, long sessionId, String key, Optional<Boolean> byPassLimit)  {
        Promise<StreamSender> promise = connection.eventLoop().newPromise();
        if (!byPassLimit.orElse(false) && !tryCreateStream(connection.attr(CURRENT_STREAMS_BIDI).get(), connection.attr(SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI).get())){
            promise.setFailure(new IllegalStateException("WT_FLOW_CONTROL_ERROR: Bidirectional stream limit exceeded"));
            return promise;
        }

        // 1. Request Stream Creation
        connection.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInboundHandlerAdapter())
                .addListener((Future<QuicStreamChannel> future) -> {
                    if (!future.isSuccess()) {
                        promise.setFailure(future.cause());
                        return;
                    }

                    QuicStreamChannel stream = future.getNow();

                    // Set channel attributes so other handlers/logs can retrieve them
                    stream.attr(SESSION_ID_KEY).set(sessionId);
                    stream.attr(STREAM_TYPE_KEY).set((long) BI_STREAM_TYPE);

                    // Configure pipeline to handle incoming data from client
                    stream.pipeline().addFirst(new QuicGlobalSniffer("STREAM-" + stream.streamId()));
                    String path = connection.attr(WebTransportServer.SESSION_PATH_KEY).get();
                    if (path != null && path.contains("socket.io")) {
                        stream.pipeline().addLast(new EngineIoFrameDecoder());
                    }
                    stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                    stream.pipeline().addLast(new MessageDispatcher());

                    // 2. Write the Mandatory Header: [0x41] [SessionID]
                    ByteBuf header = Unpooled.buffer(16);
                    try {
                        writeVarInt(header, BI_STREAM_TYPE);
                        writeVarInt(header, sessionId);
                        stream.writeAndFlush(header);
                        StreamSender sender = new StreamSender(stream);
                        if (key != null) {
                            ServerPushService.INSTANCE.register(key, sender);
                        }

                        promise.setSuccess(sender);
                    } catch (Exception e) {
                        header.release();
                        stream.close();
                        promise.setFailure(e);
                    }
                });

        return promise;
    }

    public static void writeVarInt(ByteBuf out, long value) {
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

    public static void writeCapsule(ByteBuf out, long type, long value) {
        ByteBuf temp = out.alloc().buffer();
        try {
            writeVarInt(temp, value);
            writeVarInt(out, type);
            writeVarInt(out, temp.readableBytes());
            out.writeBytes(temp);
        } finally {
            temp.release();
        }
    }

    public static long readVariableLengthInt(ByteBuf in) {
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
        switch (len) {
            case 1:
                // Prefix 00: No masking needed (value is 0-63)
                return in.readByte();

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

    public static void sendMaxStreamsCapsule(QuicStreamChannel connectStream, boolean isBidi, long maxStreams) {
        ByteBuf buf = connectStream.alloc().buffer();
        try {
            long capsuleType = isBidi ? 0x190B4D3FL : 0x190B4D40L;
            writeVarInt(buf, capsuleType);
            
            ByteBuf valBuf = connectStream.alloc().buffer();
            try {
                writeVarInt(valBuf, maxStreams);
                int len = valBuf.readableBytes();
                writeVarInt(buf, len);
                buf.writeBytes(valBuf);
            } finally {
                valBuf.release();
            }
            
            int totalBytes = buf.readableBytes();
            
            // Hex dump of raw capsule bytes (Type, Length, Value)
            String capsuleHex = io.netty.buffer.ByteBufUtil.hexDump(buf);
            
            // Hex dump of the actual HTTP/3 DATA frame bytes (Type=0x00, Length=totalBytes, Payload=capsuleBytes)
            String frameHex = "";
            ByteBuf frameBuf = connectStream.alloc().buffer();
            try {
                writeVarInt(frameBuf, 0x00); // HTTP/3 DATA Frame Type
                writeVarInt(frameBuf, totalBytes); // Payload Length
                frameBuf.writeBytes(buf.duplicate());
                frameHex = io.netty.buffer.ByteBufUtil.hexDump(frameBuf);
            } finally {
                frameBuf.release();
            }

            logger.info("📤 Writing DefaultHttp3DataFrame with WT_MAX_STREAMS Capsule (Type: 0x" + Long.toHexString(capsuleType) + ", Limit: " + maxStreams + ", Total Bytes: " + totalBytes + ")");
            logger.info("   ├── Capsule Bytes:      " + capsuleHex);
            logger.info("   └── HTTP/3 Frame Bytes: " + frameHex);

            connectStream.writeAndFlush(new io.netty.handler.codec.http3.DefaultHttp3DataFrame(buf))
                .addListener(future -> {
                    if (future.isSuccess()) {
                        logger.info("✅ Capsule writeAndFlush SUCCESS (buffered/handed off to QUIC stack) for Type 0x" + Long.toHexString(capsuleType));
                    } else {
                        logger.error("❌ Capsule writeAndFlush FAILED for Type 0x" + Long.toHexString(capsuleType), future.cause());
                    }
                });
        } catch (Exception e) {
            buf.release();
            logger.error("Failed to send WT_MAX_STREAMS capsule", e);
        }
    }
}