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
import java.nio.channels.ClosedChannelException;
import org.apache.log4j.Logger;

class RawWebTransportHandler extends ChannelDuplexHandler {
  private static final Logger logger = Logger.getLogger(RawWebTransportHandler.class.getName());

  // Track state per handler instance (per stream)
  private boolean protocolHeaderConsumed = false;
  private boolean outboundHeaderSent = false;

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
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
        Attribute<Boolean> serverInitiatedAttr =
            ctx.channel().attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY);
        Boolean serverInitiated = serverInitiatedAttr != null ? serverInitiatedAttr.get() : null;
        if (Boolean.TRUE.equals(serverInitiated)) {
          protocolHeaderConsumed = true;
        }
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
        WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
        if (mgr == null || !mgr.hasSession(sessionId)) {
          logger.warn("❌ Unknown Session ID: " + sessionId);
          data.release();
          if (ctx.channel() instanceof QuicStreamChannel) {
            ((QuicStreamChannel) ctx.channel())
                .shutdown(WebTransportUtils.WT_BUFFERED_STREAM_REJECTED, ctx.newPromise());
          } else {
            ctx.close();
          }
          return;
        }

        if (streamType == WebTransportUtils.BI_STREAM_TYPE) {
          logger.info(
              "🆕 Client Initiated BIDIRECTIONAL Stream | Session: "
                  + sessionId
                  + " | StreamID: "
                  + ctx.channel().id());
        } else if (streamType == WebTransportUtils.UNI_STREAM_TYPE) {
          logger.info("➡️ Client Initiated UNIDIRECTIONAL Stream | Session: " + sessionId);
        } else {
          logger.warn("❓ Unknown Stream Type: " + streamType);
        }

        ctx.channel().attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).set(streamType);
        ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionId);

        WebTransportSession session = mgr.get(sessionId);
        if (session == null) {
          return;
        }
        boolean isBidi = (streamType == WebTransportUtils.BI_STREAM_TYPE);
        long value =
            isBidi
                ? session.incrementAndGetClientInitiatedStreamsBidi()
                : session.incrementAndGetClientInitiatedStreamsUni();
        long maxAllowed =
            isBidi ? session.getSettingsMaxStreamsBidi() : session.getSettingsMaxStreamsUni();

        if (value > maxAllowed) {
          logger.warn(
              "❌ WebTransport stream limit exceeded for session "
                  + sessionId
                  + ": "
                  + value
                  + " > "
                  + maxAllowed);
          mgr.closeSessionWithFlowControlError(sessionId);
          if (ctx.channel() instanceof QuicStreamChannel) {
            ((QuicStreamChannel) ctx.channel())
                .shutdown(WebTransportUtils.WT_FLOW_CONTROL_ERROR, ctx.newPromise());
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
          streamChannel
              .closeFuture()
              .addListener(future -> session.getActiveClientInitiatedBi().remove(streamChannel));
        } else {
          session.getActiveClientInitiatedUni().add(streamChannel);
          streamChannel
              .closeFuture()
              .addListener(future -> session.getActiveClientInitiatedUni().remove(streamChannel));
        }
      }

      if (protocolHeaderConsumed) {
        int payloadBytes = data.readableBytes();
        if (payloadBytes > 0) {
          Attribute<Long> sessionIdAttr =
              ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY);
          Long sessionId = sessionIdAttr != null ? sessionIdAttr.get() : null;
          if (sessionId != null) {
            QuicChannel quic = (QuicChannel) ctx.channel().parent();
            WebTransportSessionManager mgr =
                quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
            if (mgr != null) {
              WebTransportSession session = mgr.get(sessionId);
              if (session != null && session.isFlowControlEnabled()) {
                long currentReceived = session.getCumulativeBytesReceived();
                long localLimit = session.getSettingsMaxData();
                if (currentReceived + payloadBytes > localLimit) {
                  logger.warn(
                      "❌ Flow control: Read blocked. Cumulative received ("
                          + currentReceived
                          + ") + read ("
                          + payloadBytes
                          + ") exceeds local limit ("
                          + localLimit
                          + "). Closing session.");
                  mgr.closeSessionWithFlowControlError(sessionId);
                  data.release();
                  ((QuicStreamChannel) ctx.channel())
                      .shutdown(WebTransportUtils.WT_FLOW_CONTROL_ERROR, ctx.newPromise());
                  return;
                }
                logger.debug(
                    String.format(
                        ">>>>>>>>>>>> %d,%d,%d,%d",
                        currentReceived,
                        (long) payloadBytes,
                        session.getCumulativeBytesReceived(),
                        session.incrementCumulativeBytesReceived(payloadBytes)));
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

    logger.debug("   -> Firing Body (" + data.readableBytes() + " bytes) to App Layer...");

    ctx.fireChannelRead(data); // message dispatcher
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
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    // Only intercept ByteBuf payloads being written outbound on a QUIC stream channel.
    if (!(msg instanceof ByteBuf) || !(ctx.channel() instanceof QuicStreamChannel)) {
      super.write(ctx, msg, promise);
      return;
    }

    ByteBuf data = (ByteBuf) msg;
    Attribute<Boolean> serverInitiatedAttr =
        ctx.channel().attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY);
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
    Attribute<Long> sessionIdAttr =
        ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY);
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
      // Send WT_DATA_BLOCKED capsule if not already sent for this peerLimit
      long lastLimit;
      while ((lastLimit = session.getLastSentDataBlockedLimit().get()) < peerLimit) {
        if (session.getLastSentDataBlockedLimit().compareAndSet(lastLimit, peerLimit)) {
          logger.warn(
              "❌ Flow control: Write blocked. Cumulative sent ("
                  + currentSent
                  + ") + write ("
                  + bytesToWrite
                  + ") exceeds peer limit ("
                  + peerLimit
                  + "). Sending WT_DATA_BLOCKED.");
          WebTransportUtils.sendDataBlockedCapsule(session.getConnectStream(), peerLimit);
          break;
        }
      }

      // Fail the write request immediately and release buffer to avoid leaks
      try {
        data.release();
      } catch (Exception ignored) {
      }
      promise.setFailure(
          new IOException(
              "Flow control limit exceeded: cumulative sent ("
                  + currentSent
                  + ") + write ("
                  + bytesToWrite
                  + ") exceeds peer limit ("
                  + peerLimit
                  + ")"));
      return;
    }

    // If the write fits within limits, update the session's cumulative sent bytes count
    // and forward the write downstream toward the QUIC network.
    session.incrementCumulativeBytesSent(bytesToWrite);
    super.write(ctx, msg, promise);
  }
}
