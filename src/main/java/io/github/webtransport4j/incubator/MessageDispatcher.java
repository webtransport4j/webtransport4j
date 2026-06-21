package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.concurrent.RejectedExecutionException;
import org.apache.log4j.Logger;

public class MessageDispatcher extends SimpleChannelInboundHandler<WebTransportFrame> {

  private static final Logger logger = Logger.getLogger(MessageDispatcher.class.getName());

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, WebTransportFrame msg) {

    Channel channel = ctx.channel();

    // 1. Debug: Log the raw hex to see invisible bytes (like 0x00)
    if (logger.isDebugEnabled()) {
      logger.debug("📦 [RAW PAYLOAD] " + formatHexBytes(msg.content()));
    }

    long sessionId = msg.sessionId();

    // 2. Offload to Business Logic
    msg.retain();
    final long finalSessionId = sessionId;

    java.util.concurrent.ExecutorService executor = null;
    if (channel instanceof QuicStreamChannel) {
      executor =
          ((QuicStreamChannel) channel).parent().attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).get();
    } else {
      executor = channel.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).get();
    }
    if (executor == null) {
      executor = WebTransportServer.getBusinessExecutor();
    }

    try {
      executor.execute(
          () -> {
            try {
              tryDispatchToHandler(channel, finalSessionId, msg);
            } catch (Throwable t) {
              logger.error("Uncaught exception/error during business logic execution", t);
            } finally {
              msg.release();
            }
          });
    } catch (RejectedExecutionException e) {
      logger.error("Task submission rejected by business executor. Releasing message buffer.", e);
      msg.release();
    }
  }



  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (ctx.channel() instanceof QuicStreamChannel) {
      WebTransportStream stream = ctx.channel().attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
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
      mgr = ((QuicStreamChannel) channel).parent().attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    } else {
      mgr = channel.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
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
      logger.error("No handler registered for path: " + session.path());
      return false;
    }

    try {
      if (frame instanceof WebTransportStreamFrame) {
        QuicStreamChannel streamChannel = (QuicStreamChannel) channel;
        WebTransportStream stream = streamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
        if (stream == null) {
          stream = new WebTransportStream(streamChannel, sessionId);
          streamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
          
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
        Boolean serverInitiated = streamChannel.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).get();
        if (!Boolean.TRUE.equals(serverInitiated)) {
          if (!Boolean.TRUE.equals(streamChannel.attr(WebTransportAttributeKeys.STREAM_NOTIFIED).get())) {
            streamChannel.attr(WebTransportAttributeKeys.STREAM_NOTIFIED).set(true);
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
