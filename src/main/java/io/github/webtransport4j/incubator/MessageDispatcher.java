package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.AttributeKey;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class MessageDispatcher extends SimpleChannelInboundHandler<WebTransportFrame> {

  public static final AttributeKey<WebTransportStream> WT_STREAM_KEY = AttributeKey.valueOf("wt.stream.instance");
  public static final AttributeKey<Boolean> STREAM_NOTIFIED = AttributeKey.valueOf("wt.stream.notified");

  private static final Logger logger = Logger.getLogger(MessageDispatcher.class.getName());
  private static final ExecutorService businessPool =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
  private java.util.concurrent.ScheduledFuture<?> pingFuture;

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, WebTransportFrame msg) {

    Channel channel = ctx.channel();

    // 1. Debug: Log the raw hex to see invisible bytes (like 0x00)
    if (logger.isDebugEnabled()) {
      logger.debug("📦 [RAW PAYLOAD] " + formatHexBytes(msg.content()));
    }

    String path;
    String transportType;
    long sessionId = msg.sessionId();

    // Determine Context
    if (msg instanceof WebTransportStreamFrame) {
      WebTransportStreamFrame streamFrame = (WebTransportStreamFrame) msg;
      transportType = streamFrame.isBidirectional() ? "BIDIRECTIONAL" : "UNIDIRECTIONAL";

      String pathAttr = null;
      if (channel instanceof QuicStreamChannel) {
        pathAttr =
            ((QuicStreamChannel) channel).parent().attr(WebTransportServer.SESSION_PATH_KEY).get();
      }
      path = (pathAttr != null) ? pathAttr : "?";
    } else if (msg instanceof WebTransportDatagramFrame) {
      transportType = "DATAGRAM";
      String pathAttr = channel.attr(WebTransportServer.SESSION_PATH_KEY).get();
      path = (pathAttr != null) ? pathAttr : "?";
    } else if (msg instanceof WebTransportCapsule) {
      transportType = "CAPSULE";
      String pathAttr = null;
      if (channel instanceof QuicStreamChannel) {
        pathAttr =
            ((QuicStreamChannel) channel).parent().attr(WebTransportServer.SESSION_PATH_KEY).get();
      }
      path = (pathAttr != null) ? pathAttr : "?";
    } else {
      transportType = "UNKNOWN";
      path = "?";
    }

    // 2. Offload to Business Logic
    msg.retain();
    final String finalPath = path;
    final String finalType = transportType;
    final long finalSessionId = sessionId;

    java.util.concurrent.ExecutorService executor = null;
    if (channel instanceof QuicStreamChannel) {
      executor =
          ((QuicStreamChannel) channel).parent().attr(WebTransportServer.BUSINESS_EXECUTOR).get();
    } else {
      executor = channel.attr(WebTransportServer.BUSINESS_EXECUTOR).get();
    }
    if (executor == null) {
      executor = businessPool;
    }

    executor.submit(
        () -> {
          try {
            boolean isSocketIo = (finalPath != null && finalPath.contains("socket.io"));
            if (isSocketIo) {
              processSocketIOPacket(channel, finalPath, finalType, finalSessionId, msg.content());
            } else {
              tryDispatchToHandler(channel, finalSessionId, msg);
            }
          } finally {
            msg.release();
          }
        });
  }

  private void processSocketIOPacket(
      Channel channel, String path, String transportType, long sessionId, ByteBuf payload) {
    try {
      // Convert to String
      String content = payload.toString(StandardCharsets.UTF_8);
      logger.debug("⚡️ [SOCKET.IO] " + transportType + " | Raw: " + content);
      if (content.isEmpty()) return;

      // 1. Parse Socket.IO Packet Type
      char packetType = content.charAt(0);
      String data = (content.length() > 1) ? content.substring(1) : "";

      logger.debug(
          "⚡️ [SOCKET.IO] " + transportType + " | Type: " + packetType + " | Data: " + data);

      // 2. Handle Types
      switch (packetType) {
        case '0': // OPEN
          logger.info("👋 Received OPEN (Handshake). Data: " + data);
          // Standard Socket.IO reply to Open is usually an Open packet back with session
          // ID
          reply(
              channel,
              "0{\"sid\":\""
                  + channel.id().asShortText()
                  + "\",\"upgrades\":[],\"pingInterval\":25000,\"pingTimeout\":20000}");
          startPingSchedule(channel);
          break;

        case '1': // CLOSE
          logger.info("❌ Received CLOSE.");
          channel.close();
          break;

        case '2': // PING
          logger.debug("❤️ Received PING. Sending PONG...");
          reply(channel, "3");
          break;

        case '3': // PONG
          logger.debug("💓 Received PONG (Client alive).");
          break;

        case '4': // MESSAGE
          logger.info("📩 Received MESSAGE: " + data);
          if ("0".equals(data)) {
            logger.info("🤝 Received Socket.IO Connect request. Acknowledging connection.");
            reply(channel, "40{\"sid\":\"" + channel.id().asShortText() + "\"}");
          } else if (data.startsWith("2")) {
            // Socket.IO EVENT: 2[<ackId>]["eventName", ...args]
            int idx = 1;
            while (idx < data.length() && Character.isDigit(data.charAt(idx))) {
              idx++;
            }
            String ackId = data.substring(1, idx);
            String eventPayload = data.substring(idx);

            if (eventPayload.startsWith("[") && eventPayload.endsWith("]")) {
              String inner = eventPayload.substring(1, eventPayload.length() - 1);
              if (inner.startsWith("\"")) {
                int endQuote = inner.indexOf("\"", 1);
                if (endQuote != -1) {
                  String eventName = inner.substring(1, endQuote);
                  String argsString = "";
                  if (endQuote + 1 < inner.length()) {
                    String rem = inner.substring(endQuote + 1).trim();
                    if (rem.startsWith(",")) {
                      argsString = rem.substring(1).trim();
                    }
                  }
                  logger.info(
                      "Parsed Socket.IO Event: '"
                          + eventName
                          + "' with args: "
                          + argsString
                          + " | ackId: "
                          + ackId);

                  if ("trigger_server_message".equals(eventName)) {
                    logger.info(
                        "Emitting server_message to client with ACK request (ackId: 777)...");
                    reply(channel, "42777[\"server_message\",{\"request\":\"hello\"}]");
                  } else {
                    if (!ackId.isEmpty()) {
                      String ackResponse =
                          "43" + ackId + "[{\"status\":\"ok\",\"received\":\"" + eventName + "\"}]";
                      logger.info("Sending ACK reply to client: " + ackResponse);
                      reply(channel, ackResponse);
                    }
                  }
                }
              }
            }
          } else if (data.startsWith("3")) {
            // Socket.IO ACK: 3<ackId>[<args>]
            int idx = 1;
            while (idx < data.length() && Character.isDigit(data.charAt(idx))) {
              idx++;
            }
            String ackId = data.substring(1, idx);
            String ackPayload = data.substring(idx);
            logger.info(
                "👍 Received ACK from client for ackId " + ackId + " | Payload: " + ackPayload);
          }
          break;

        default:
          logger.warn(
              "⚠️ Unknown Packet Type: '" + packetType + "' (Ascii: " + (int) packetType + ")");
      }

    } catch (Exception e) {
      logger.error("Error processing packet", e);
    }
  }

  /**
   * Sends a text-based reply back to the client over the specified channel.
   *
   * <h3>First Principles & System Architecture:</h3>
   *
   * <h4>1. Thread Safety via Netty EventLoop delegation</h4>
   * <p>
   * Writing bytes and flushing buffers directly to a Netty channel is not
   * thread-safe.
   * Since this message-processing pipeline executes business logic inside a
   * concurrent
   * thread pool ({@code businessPool}), we must schedule all socket write
   * operations back
   * onto the channel's designated {@link io.netty.channel.EventLoop} thread via
   * {@code channel.eventLoop().execute(...)}. This ensures sequential,
   * thread-safe writes
   * and avoids concurrent modification or ordering races.
   * </p>
   *
   * <h4>2. Dynamic Transport Protocol Negotiation</h4>
   * <p>
   * The server acts as a unified gatekeeper handling two client formats on the
   * same port:
   * <ul>
   * <li><b>Socket.IO over WebTransport</b> (connected via the {@code /socket.io/}
   * path)</li>
   * <li><b>Raw WebTransport Clients</b> (connected via root or custom paths)</li>
   * </ul>
   * We dynamically look up the connection path stored in {@code SESSION_PATH_KEY}
   * (on the parent
   * {@link QuicStreamChannel} for streams, or the datagram channel itself) to
   * identify the protocol stack.
   * </p>
   *
   * <h4>3. Outbound Message Framing Formats</h4>
   * <ul>
   * <li>
   * <b>Socket.IO Clients</b>: Expect message frames prefixed with WebSocket-like
   * length headers:
   * <ul>
   * <li>If length &lt; 126: Writes a 1-byte length prefix.</li>
   * <li>If length &lt; 65,536: Writes {@code 126} followed by a 2-byte length
   * short value.</li>
   * <li>Otherwise: Writes {@code 127} followed by an 8-byte length long
   * value.</li>
   * </ul>
   * </li>
   * <li>
   * <b>Raw WebTransport Clients</b>: Expect raw byte payloads directly. We write
   * the UTF-8 bytes
   * without prepending any length indicators.
   * </li>
   * </ul>
   *
   * <h4>Example Wire-Level Payload Transformations:</h4>
   *
   * <pre>{@code
   * // =========================================================================
   * // SCENARIO 1: SOCKET.IO OVER WEBTRANSPORT (path contains "/socket.io/")
   * // =========================================================================
   * // Socket.IO expects all packets to be framed. A frame consists of:
   * // [Length Prefix Header] + [Engine.IO Type] + [Socket.IO Type] + [Payload]
   * //
   * // Example A: Sending a Socket.IO Event Acknowledgement (ACK)
   * // Event payload to send: "431[{\"status\":\"ok\"}]"
   * //   - Engine.IO Type: '4' (MESSAGE)
   * //   - Socket.IO Type: '3' (ACK)
   * //   - Ack ID: '1'
   * //   - Payload: "[{"status":"ok"}]" (length is 21 bytes)
   * //   - Total string length: 1 + 1 + 1 + 16 = 19 characters (19 bytes in UTF-8)
   * //
   * // Transformation:
   * //   - Since 19 < 126, a 1-byte length prefix containing the value 19 (0x13) is prepended.
   * // Wire bytes sent: [ 0x13, 0x34, 0x33, 0x31, 0x5B, 0x7B, 0x22, 0x73, ... ]
   * //                  (Length, '4',  '3',  '1',  '[',  '{',  '"',  's', ... )
   * //
   * // =========================================================================
   * // SCENARIO 2: RAW WEBTRANSPORT CLIENT (path does NOT contain "/socket.io/")
   * // =========================================================================
   * // Raw WebTransport clients read raw streams directly without packet wrappers.
   * //
   * // Example B: Replying to a raw WebTransport request
   * // Text payload to send: "ACK BI: hello"
   * //   - Total string length: 13 bytes in UTF-8
   * //
   * // Transformation:
   * //   - No length header is prepended.
   * // Wire bytes sent: [ 0x41, 0x43, 0x4B, 0x20, 0x42, 0x49, 0x3A, 0x20, 0x68, 0x65, 0x6C, 0x6C, 0x6F ]
   * //                  ( 'A',  'C',  'K',  ' ',  'B',  'I',  ':',  ' ',  'h',  'e',  'l',  'l',  'o' )
   * }</pre>
   */
  private void reply(Channel channel, String text) {
    channel
        .eventLoop()
        .execute(
            () -> {
              byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
              int len = bytes.length;
              ByteBuf buffer;

              String path = null;
              if (channel instanceof QuicStreamChannel) {
                path =
                    ((QuicStreamChannel) channel)
                        .parent()
                        .attr(WebTransportServer.SESSION_PATH_KEY)
                        .get();
              } else {
                path = channel.attr(WebTransportServer.SESSION_PATH_KEY).get();
              }
              boolean isSocketIo = (path != null && path.contains("socket.io"));

              if (isSocketIo) {
                if (len < 126) {
                  buffer = channel.alloc().directBuffer(1 + len);
                  buffer.writeByte(len);
                } else if (len < 65536) {
                  buffer = channel.alloc().directBuffer(3 + len);
                  buffer.writeByte(126);
                  buffer.writeShort(len);
                } else {
                  buffer = channel.alloc().directBuffer(9 + len);
                  buffer.writeByte(127);
                  buffer.writeLong(len);
                }
              } else {
                buffer = channel.alloc().directBuffer(len);
              }
              buffer.writeBytes(bytes);
              if (logger.isDebugEnabled()) {
                logger.debug("✅ Sent Reply: " + text + formatHexBytes(buffer));
              }
              channel.writeAndFlush(buffer);
            });
  }


  private void startPingSchedule(Channel channel) {
    if (pingFuture != null) {
      pingFuture.cancel(false);
    }

    pingFuture =
        channel
            .eventLoop()
            .scheduleAtFixedRate(
                () -> {
                  if (channel.isActive()) {
                    logger.debug("⚡️ Sending PING to client...");
                    reply(channel, "2");
                  } else {
                    if (pingFuture != null) {
                      pingFuture.cancel(false);
                    }
                  }
                },
                25,
                25,
                TimeUnit.SECONDS);

    // Also cancel schedule if stream is closed
    channel
        .closeFuture()
        .addListener(
            future -> {
              if (pingFuture != null) {
                pingFuture.cancel(false);
              }
            });
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (ctx.channel() instanceof QuicStreamChannel) {
      WebTransportStream stream = ctx.channel().attr(WT_STREAM_KEY).get();
      if (stream != null && stream.getErrorHandler() != null) {
        try {
          stream.getErrorHandler().accept(cause);
        } catch (Exception e) {
          logger.error("Error in stream onError handler", e);
        }
      }
    }
    if (cause instanceof io.netty.handler.codec.quic.QuicStreamResetException) {
      io.netty.handler.codec.quic.QuicStreamResetException reset =
          (io.netty.handler.codec.quic.QuicStreamResetException) cause;
      long httpErrorCode = reset.applicationProtocolCode();
      if (WebTransportUtils.isWebTransportApplicationError(httpErrorCode)) {
        long wtErrorCode = WebTransportUtils.httpCodeToWebTransportCode(httpErrorCode);
        logger.info(
            "🌊 Stream reset by peer with WebTransport application error code: 0x"
                + Long.toHexString(wtErrorCode)
                + " ("
                + wtErrorCode
                + ")");
      } else {
        logger.debug(
            "🌊 Stream reset by peer with HTTP/3 error code: 0x" + Long.toHexString(httpErrorCode));
      }
    } else {
      logger.error("❌ Pipeline error: ", cause);
    }
    ctx.close();
  }

  private boolean tryDispatchToHandler(Channel channel, long sessionId, WebTransportFrame frame) {
    WebTransportSessionManager mgr = null;
    if (channel instanceof QuicStreamChannel) {
      mgr = ((QuicStreamChannel) channel).parent().attr(WebTransportSessionManager.WT_SESSION_MGR).get();
    } else {
      mgr = channel.attr(WebTransportSessionManager.WT_SESSION_MGR).get();
    }

    if (mgr == null) {
      return false;
    }

    WebTransportSession session = mgr.get(sessionId);
    if (session == null || session.path() == null) {
      return false;
    }

    WebTransportHandler handler = WebTransportServer.getHandler(session.path());
    if (handler == null) {
      return false;
    }

    try {
      if (frame instanceof WebTransportStreamFrame) {
        QuicStreamChannel streamChannel = (QuicStreamChannel) channel;
        WebTransportStream stream = streamChannel.attr(WT_STREAM_KEY).get();
        if (stream == null) {
          stream = new WebTransportStream(streamChannel, sessionId);
          streamChannel.attr(WT_STREAM_KEY).set(stream);
          
          final WebTransportStream finalStream = stream;
          streamChannel.closeFuture().addListener(f -> {
            if (finalStream.getCloseHandler() != null) {
              try {
                finalStream.getCloseHandler().run();
              } catch (Exception e) {
                logger.error("Error in stream onClose handler", e);
              }
            }
          });
        }

        // Notify incoming stream if client-initiated and not yet notified
        Boolean serverInitiated = streamChannel.attr(WebTransportUtils.SERVER_INITIATED_KEY).get();
        if (!Boolean.TRUE.equals(serverInitiated)) {
          if (!Boolean.TRUE.equals(streamChannel.attr(STREAM_NOTIFIED).get())) {
            streamChannel.attr(STREAM_NOTIFIED).set(true);
            try {
              handler.onIncomingStream(session, stream);
            } catch (Exception e) {
              logger.error("Error in onIncomingStream callback", e);
            }
          }
        }

        // Dispatch data
        if (stream.getDataConsumer() != null) {
          try {
            stream.getDataConsumer().accept(frame.content());
          } catch (Exception e) {
            logger.error("Error in stream onData callback", e);
          }
        }
      } else if (frame instanceof WebTransportDatagramFrame) {
        try {
          handler.onDatagramReceived(session, frame.content());
        } catch (Exception e) {
          logger.error("Error in onDatagramReceived callback", e);
        }
      }
      return true;
    } catch (Exception e) {
      logger.error("Exception in tryDispatchToHandler", e);
      return false;
    }
  }

  private static String formatHexBytes(ByteBuf buf) {
    int len = buf.readableBytes();
    if (len == 0) return "\n    ├── Wire Bytes: [ ]\n    └── Characters: ( )";

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
    return "\n    ├── Wire Bytes: "
        + hexLine.toString()
        + "\n    └── Characters: "
        + charLine.toString();
  }
}
