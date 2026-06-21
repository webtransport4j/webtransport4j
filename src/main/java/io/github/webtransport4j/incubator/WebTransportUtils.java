package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.util.Optional;

/**
 * @author https://github.com/sanjomo
 * @date 20/01/26 10:58 pm
 */
public class WebTransportUtils {
  private static final org.apache.log4j.Logger logger =
      org.apache.log4j.Logger.getLogger(WebTransportUtils.class.getName());



  public static final int UNI_STREAM_TYPE = 0x54; // WebTransport Unidirectional ID
  public static final int BI_STREAM_TYPE = 0x41; // WebTransport Bidirectional

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
  public static Future<QuicStreamChannel> createUniStream(
      QuicStreamChannel connectStreamChannel,
      Optional<Boolean> byPassLimit,
      ChannelHandler streamHandler) {
    Promise<QuicStreamChannel> promise = connectStreamChannel.parent().eventLoop().newPromise();
    WebTransportSessionManager mgr =
        connectStreamChannel.parent().attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    WebTransportSession session = mgr != null ? mgr.get(connectStreamChannel.streamId()) : null;
    if (session == null) {
      promise.setFailure(
          new IllegalStateException("Session not found: " + connectStreamChannel.streamId()));
      return promise;
    }

    if (!byPassLimit.orElse(false)) {
      long current = session.getServerInitiatedStreamsUni();
      long max = session.getPeerSettingsMaxStreamsUni();
      if (current >= max) {
        logger.warn(
            "❌ WebTransport stream limit exceeded for session "
                + connectStreamChannel.streamId()
                + ": "
                + current
                + " >= "
                + max
                + ". Stream creation failed.");
        sendStreamsBlockedCapsule(session.getConnectStream(), false, max);
        promise.setFailure(
            new IllegalStateException(
                "Unidirectional stream limit exceeded"));
        return promise;
      }
      session.incrementAndGetServerInitiatedStreamsUni();
    }

    // 1. Request Stream Creation
    connectStreamChannel
        .parent()
        .createStream(QuicStreamType.UNIDIRECTIONAL, streamHandler)
        .addListener(
            (Future<QuicStreamChannel> future) -> {
              if (!future.isSuccess()) {
                promise.setFailure(future.cause());
                return;
              }

              QuicStreamChannel stream = future.getNow();

              // Set channel attributes so other handlers/logs can retrieve them
              stream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(connectStreamChannel.streamId());
              stream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).set((long) UNI_STREAM_TYPE);
              stream.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).set(true);

              // 2. Write the Mandatory Header: [0x54] [SessionID]
              // We write this synchronously before giving the stream to the user.
              ByteBuf header = Unpooled.buffer(16);
              try {
                writeVarInt(header, UNI_STREAM_TYPE);
                writeVarInt(header, connectStreamChannel.streamId());
                stream.writeAndFlush(header);
                session.getActiveServerInitiatedUni().add(stream);
                stream
                    .closeFuture()
                    .addListener(f -> session.getActiveServerInitiatedUni().remove(stream));

                promise.setSuccess(stream);
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
   *
   * @param connectStreamChannel The parent connect stream channel
   * @param byPassLimit Whether to bypass flow control stream limits
   * @param streamHandler The channel handler to add to the stream pipeline
   * @return A Future that completes with the QuicStreamChannel
   */
  public static Future<QuicStreamChannel> createBiStream(
      QuicStreamChannel connectStreamChannel,
      Optional<Boolean> byPassLimit,
      ChannelHandler streamHandler) {
    Promise<QuicStreamChannel> promise = connectStreamChannel.parent().eventLoop().newPromise();
    WebTransportSessionManager mgr =
        connectStreamChannel.parent().attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    WebTransportSession session = mgr != null ? mgr.get(connectStreamChannel.streamId()) : null;
    if (session == null) {
      promise.setFailure(
          new IllegalStateException("Session not found: " + connectStreamChannel.streamId()));
      return promise;
    }

    if (!byPassLimit.orElse(false)) {
      long current = session.getServerInitiatedStreamsBidi();
      long max = session.getPeerSettingsMaxStreamsBidi();
      if (current >= max) {
        logger.warn(
            "❌ WebTransport stream limit exceeded for session "
                + connectStreamChannel.streamId()
                + ": "
                + current
                + " >= "
                + max
                + ". Stream creation failed.");
        sendStreamsBlockedCapsule(session.getConnectStream(), true, max);
        promise.setFailure(
            new IllegalStateException(
                "Bidirectional stream limit exceeded"));
        return promise;
      }
      session.incrementAndGetServerInitiatedStreamsBidi();
    }

    // 1. Request Stream Creation
    connectStreamChannel
        .parent()
        .createStream(QuicStreamType.BIDIRECTIONAL, streamHandler)
        .addListener(
            (Future<QuicStreamChannel> future) -> {
              if (!future.isSuccess()) {
                promise.setFailure(future.cause());
                return;
              }

              QuicStreamChannel stream = future.getNow();

              // Set channel attributes so other handlers/logs can retrieve them
              stream.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(connectStreamChannel.streamId());
              stream.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).set((long) BI_STREAM_TYPE);
              stream.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).set(true);

              // 2. Write the Mandatory Header: [0x41] [SessionID]
              ByteBuf header = Unpooled.buffer(16);
              try {
                writeVarInt(header, BI_STREAM_TYPE);
                writeVarInt(header, connectStreamChannel.streamId());
                stream.writeAndFlush(header);
                session.getActiveServerInitiatedBi().add(stream);
                stream
                    .closeFuture()
                    .addListener(f -> session.getActiveServerInitiatedBi().remove(stream));

                promise.setSuccess(stream);
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

  public static void sendMaxStreamsCapsule(
      QuicStreamChannel connectStream, boolean isBidi, long maxStreams) {
    ByteBuf buf =
        (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
    try {
      long capsuleType = isBidi ? 0x190B4D3FL : 0x190B4D40L;
      writeVarInt(buf, capsuleType);

      ByteBuf valBuf =
          (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
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

      // Hex dump of the actual HTTP/3 DATA frame bytes (Type=0x00, Length=totalBytes,
      // Payload=capsuleBytes)
      String frameHex = "";
      ByteBuf frameBuf =
          (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
      try {
        writeVarInt(frameBuf, 0x00); // HTTP/3 DATA Frame Type
        writeVarInt(frameBuf, totalBytes); // Payload Length
        frameBuf.writeBytes(buf.duplicate());
        frameHex = io.netty.buffer.ByteBufUtil.hexDump(frameBuf);
      } finally {
        frameBuf.release();
      }

      logger.info(
          "📤 Writing DefaultHttp3DataFrame with WT_MAX_STREAMS Capsule (Type: 0x"
              + Long.toHexString(capsuleType)
              + ", Limit: "
              + maxStreams
              + ", Total Bytes: "
              + totalBytes
              + ")");
      logger.info("   ├── Capsule Bytes:      " + capsuleHex);
      logger.info("   └── HTTP/3 Frame Bytes: " + frameHex);

      Object future =
          connectStream.writeAndFlush(new io.netty.handler.codec.http3.DefaultHttp3DataFrame(buf));
      if (future instanceof io.netty.channel.ChannelFuture) {
        ((io.netty.channel.ChannelFuture) future)
            .addListener(
                f -> {
                  if (f.isSuccess()) {
                    logger.info(
                        "✅ Capsule writeAndFlush SUCCESS (buffered/handed off to QUIC stack) for"
                            + " Type 0x"
                            + Long.toHexString(capsuleType));
                  } else {
                    logger.error(
                        "❌ Capsule writeAndFlush FAILED for Type 0x"
                            + Long.toHexString(capsuleType),
                        f.cause());
                  }
                });
      } else if (future instanceof io.netty.util.concurrent.Future) {
        ((io.netty.util.concurrent.Future<?>) future)
            .addListener(
                f -> {
                  if (f.isSuccess()) {
                    logger.info(
                        "✅ Capsule writeAndFlush SUCCESS (buffered/handed off to QUIC stack) for"
                            + " Type 0x"
                            + Long.toHexString(capsuleType));
                  } else {
                    logger.error(
                        "❌ Capsule writeAndFlush FAILED for Type 0x"
                            + Long.toHexString(capsuleType),
                        f.cause());
                  }
                });
      } else {
        logger.info(
            "✅ Capsule writeAndFlush called (no future returned) for Type 0x"
                + Long.toHexString(capsuleType));
      }
    } catch (Exception e) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
      logger.error("Failed to send WT_MAX_STREAMS capsule", e);
    }
  }

  public static void sendMaxDataCapsule(QuicStreamChannel connectStream, long maxData) {
    ByteBuf buf =
        (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
    try {
      long capsuleType = 0x190B4D3DL;
      writeVarInt(buf, capsuleType);

      ByteBuf valBuf =
          (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
      try {
        writeVarInt(valBuf, maxData);
        int len = valBuf.readableBytes();
        writeVarInt(buf, len);
        buf.writeBytes(valBuf);
      } finally {
        valBuf.release();
      }

      int totalBytes = buf.readableBytes();

      // Hex dump of raw capsule bytes (Type, Length, Value)
      String capsuleHex = io.netty.buffer.ByteBufUtil.hexDump(buf);

      // Hex dump of the actual HTTP/3 DATA frame bytes (Type=0x00, Length=totalBytes,
      // Payload=capsuleBytes)
      String frameHex = "";
      ByteBuf frameBuf =
          (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
      try {
        writeVarInt(frameBuf, 0x00); // HTTP/3 DATA Frame Type
        writeVarInt(frameBuf, totalBytes); // Payload Length
        frameBuf.writeBytes(buf.duplicate());
        frameHex = io.netty.buffer.ByteBufUtil.hexDump(frameBuf);
      } finally {
        frameBuf.release();
      }

      logger.info(
          "📤 Writing DefaultHttp3DataFrame with WT_MAX_DATA Capsule (Type: 0x"
              + Long.toHexString(capsuleType)
              + ", Limit: "
              + maxData
              + ", Total Bytes: "
              + totalBytes
              + ")");
      logger.info("   ├── Capsule Bytes:      " + capsuleHex);
      logger.info("   └── HTTP/3 Frame Bytes: " + frameHex);

      Object future =
          connectStream.writeAndFlush(new io.netty.handler.codec.http3.DefaultHttp3DataFrame(buf));
      if (future instanceof io.netty.channel.ChannelFuture) {
        ((io.netty.channel.ChannelFuture) future)
            .addListener(
                f -> {
                  if (f.isSuccess()) {
                    logger.info(
                        "✅ Capsule writeAndFlush SUCCESS (buffered/handed off to QUIC stack) for"
                            + " Type 0x"
                            + Long.toHexString(capsuleType));
                  } else {
                    logger.error(
                        "❌ Capsule writeAndFlush FAILED for Type 0x"
                            + Long.toHexString(capsuleType),
                        f.cause());
                  }
                });
      } else if (future instanceof io.netty.util.concurrent.Future) {
        ((io.netty.util.concurrent.Future<?>) future)
            .addListener(
                f -> {
                  if (f.isSuccess()) {
                    logger.info(
                        "✅ Capsule writeAndFlush SUCCESS (buffered/handed off to QUIC stack) for"
                            + " Type 0x"
                            + Long.toHexString(capsuleType));
                  } else {
                    logger.error(
                        "❌ Capsule writeAndFlush FAILED for Type 0x"
                            + Long.toHexString(capsuleType),
                        f.cause());
                  }
                });
      } else {
        logger.info(
            "✅ Capsule writeAndFlush called (no future returned) for Type 0x"
                + Long.toHexString(capsuleType));
      }
    } catch (Exception e) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
      logger.error("Failed to send WT_MAX_DATA capsule", e);
    }
  }

  public static void sendDataBlockedCapsule(QuicStreamChannel connectStream, long maxData) {
    ByteBuf buf =
        (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
    try {
      long capsuleType = 0x190B4D41L;
      writeVarInt(buf, capsuleType);

      ByteBuf valBuf =
          (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
      try {
        writeVarInt(valBuf, maxData);
        int len = valBuf.readableBytes();
        writeVarInt(buf, len);
        buf.writeBytes(valBuf);
      } finally {
        valBuf.release();
      }

      int totalBytes = buf.readableBytes();
      logger.info(
          "📤 Writing DefaultHttp3DataFrame with WT_DATA_BLOCKED Capsule (Type: 0x"
              + Long.toHexString(capsuleType)
              + ", Limit: "
              + maxData
              + ", Total Bytes: "
              + totalBytes
              + ")");

      connectStream.writeAndFlush(new io.netty.handler.codec.http3.DefaultHttp3DataFrame(buf));
    } catch (Exception e) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
      logger.error("Failed to send WT_DATA_BLOCKED capsule", e);
    }
  }

  public static void sendStreamsBlockedCapsule(
      QuicStreamChannel connectStream, boolean isBidi, long maxStreams) {
    ByteBuf buf =
        (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
    try {
      long capsuleType = isBidi ? 0x190B4D43L : 0x190B4D44L;
      writeVarInt(buf, capsuleType);

      ByteBuf valBuf =
          (connectStream.alloc() != null) ? connectStream.alloc().buffer() : Unpooled.buffer();
      try {
        writeVarInt(valBuf, maxStreams);
        int len = valBuf.readableBytes();
        writeVarInt(buf, len);
        buf.writeBytes(valBuf);
      } finally {
        valBuf.release();
      }

      int totalBytes = buf.readableBytes();
      logger.info(
          "📤 Writing DefaultHttp3DataFrame with WT_STREAMS_BLOCKED Capsule (Type: 0x"
              + Long.toHexString(capsuleType)
              + ", Maximum Streams: "
              + maxStreams
              + ", Total Bytes: "
              + totalBytes
              + ")");

      connectStream.writeAndFlush(new io.netty.handler.codec.http3.DefaultHttp3DataFrame(buf));
    } catch (Exception e) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
      logger.error("Failed to send WT_STREAMS_BLOCKED capsule", e);
    }
  }

  public static final long WT_ERROR_FIRST = 0x52e4a40fa8dbL;
  public static final long WT_ERROR_LAST = 0x52e5ac983162L;

  /**
   * Maps a 32-bit unsigned WebTransport application error code to a 62-bit HTTP/3 error code as per
   * draft-15 Section 4.4.
   */
  public static long webTransportCodeToHttpCode(long n) {
    if (n < 0 || n > 0xffffffffL) {
      throw new IllegalArgumentException(
          "WebTransport error code must be an unsigned 32-bit integer: " + n);
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
    throw new IllegalArgumentException(
        "HTTP/3 error code is outside WT_APPLICATION_ERROR range: " + h);
  }

  /** Checks if a given HTTP/3 error code is within the WT_APPLICATION_ERROR range. */
  public static boolean isWebTransportApplicationError(long h) {
    return (h >= WT_ERROR_FIRST && h <= WT_ERROR_LAST && (h - 0x21L) % 31L != 0L)
        || (h >= 0 && h <= Integer.MAX_VALUE); // fallback for Netty QUIC limitation
  }

  public static void resetStream(QuicStreamChannel streamChannel, long appErrorCode) {
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
}
