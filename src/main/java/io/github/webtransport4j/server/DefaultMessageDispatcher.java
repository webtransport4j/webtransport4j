package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.util.Attribute;
import java.util.concurrent.ExecutorService;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default dispatcher routing WebTransport frames to handlers. */
@ChannelHandler.Sharable
public class DefaultMessageDispatcher extends SimpleChannelInboundHandler<WebTransportFrame>
    implements MessageDispatcher {

  public static final DefaultMessageDispatcher INSTANCE = new DefaultMessageDispatcher();

  private static final Logger logger = LoggerFactory.getLogger(DefaultMessageDispatcher.class);

  private static final WebTransportHandler NOOP_HANDLER = new WebTransportHandler() {};

  @Override
  protected void channelRead0(@NonNull ChannelHandlerContext ctx, @NonNull WebTransportFrame msg) {
    Channel channel = ctx.channel();
    if (logger.isDebugEnabled()) {
      logger.debug("📦 [RAW PAYLOAD] {}", WebTransportUtils.formatHexBytes(msg.content()));
    }
    final long finalSessionId = msg.sessionId();
    ExecutorService executor;
    if (channel instanceof QuicStreamChannel) {
      executor =
          ((QuicStreamChannel) channel)
              .parent()
              .attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR)
              .get();
    } else {
      executor = channel.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).get();
    }

    if (executor == null) {
      try {
        tryDispatchToHandler(channel, finalSessionId, msg);
      } catch (Throwable t) {
        logger.error("Uncaught exception/error during business logic execution", t);
      }
    } else {
      if (channel instanceof QuicStreamChannel) {
        QuicStreamChannel streamChannel = (QuicStreamChannel) channel;
        StreamMailbox mailbox =
            streamChannel.attr(WebTransportAttributeKeys.STREAM_MAILBOX_KEY).get();
        if (mailbox == null) {
          mailbox =
              new StreamMailbox(
                  streamChannel, executor, this::tryDispatchToHandler, finalSessionId);
          StreamMailbox oldMailbox =
              streamChannel.attr(WebTransportAttributeKeys.STREAM_MAILBOX_KEY).setIfAbsent(mailbox);
          if (oldMailbox != null) {
            mailbox = oldMailbox;
          }
        }
        mailbox.enqueue(msg);
      } else {
        DatagramMailbox mailbox =
            channel.attr(WebTransportAttributeKeys.DATAGRAM_MAILBOX_KEY).get();
        if (mailbox == null) {
          mailbox = new DatagramMailbox(channel, executor, this::tryDispatchToHandler);
          DatagramMailbox oldMailbox =
              channel.attr(WebTransportAttributeKeys.DATAGRAM_MAILBOX_KEY).setIfAbsent(mailbox);
          if (oldMailbox != null) {
            mailbox = oldMailbox;
          }
        }
        mailbox.enqueue(msg);
      }
    }
  }

  @Override
  public void exceptionCaught(@NonNull ChannelHandlerContext ctx, @NonNull Throwable cause) {
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
    if (cause instanceof QuicStreamResetException) {
      QuicStreamResetException reset = (QuicStreamResetException) cause;
      long httpErrorCode = reset.applicationProtocolCode();
      if (WebTransportUtils.isWebTransportApplicationError(httpErrorCode)) {
        long wtErrorCode = WebTransportUtils.httpCodeToWebTransportCode(httpErrorCode);
        logger.info(
            "🌊 Stream reset by peer with WebTransport application error code: 0x{} ({})",
            Long.toHexString(wtErrorCode),
            wtErrorCode);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "🌊 Stream reset by peer with HTTP/3 error code: 0x{}",
              Long.toHexString(httpErrorCode));
        }
      }
    } else {
      logger.error("❌ Pipeline error: ", cause);
    }
    ctx.close();
  }

  private void tryDispatchToHandler(
      @NonNull Channel channel, long sessionId, @NonNull WebTransportFrame frame) {
    WebTransportSessionManager mgr;
    if (channel instanceof QuicStreamChannel) {
      mgr =
          ((QuicStreamChannel) channel)
              .parent()
              .attr(WebTransportAttributeKeys.WT_SESSION_MGR)
              .get();
    } else {
      mgr = channel.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    }
    if (mgr == null) {
      return;
    }
    WebTransportSession session = mgr.get(sessionId);
    if (session == null) {
      // Fire metrics: discard — session not found at dispatch time
      WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
      if (metrics != null) {
        metrics.onDatagramDiscarded(sessionId, "session_not_found_at_dispatch");
      }
      return;
    }
    WebTransportServer server;
    Attribute<WebTransportServer> attr;
    if (channel instanceof QuicStreamChannel) {
      attr = ((QuicStreamChannel) channel).parent().attr(WebTransportAttributeKeys.SERVER_KEY);
    } else {
      attr = channel.attr(WebTransportAttributeKeys.SERVER_KEY);
    }
    server = attr != null ? attr.get() : null;
    WebTransportHandler handler =
        (server != null) ? server.getHandler(session.path()) : NOOP_HANDLER;
    try {
      if (frame instanceof WebTransportStreamFrame) {
        if (!(channel instanceof QuicStreamChannel)) {
          throw new RuntimeException("Implemented only for QuicStreamChannel");
        }
        QuicStreamChannel streamChannel = (QuicStreamChannel) channel;
        WebTransportStream stream =
            streamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
        if (stream == null) {
          stream = new DefaultNettyWebTransportStream(streamChannel, sessionId);
          streamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
          final WebTransportStream finalStream = stream;
          streamChannel
              .closeFuture()
              .addListener(
                  f -> {
                    if (finalStream.getCloseHandler() != null) {
                      try {
                        finalStream.getCloseHandler().onClose();
                      } catch (Exception e) {
                        logger.error("Error in stream onClose handler", e);
                      }
                    }
                  });
        }
        // Notify incoming stream if client-initiated and not yet notified
        Boolean serverInitiated =
            streamChannel.attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).get();
        if (!Boolean.TRUE.equals(serverInitiated)) {
          if (!Boolean.TRUE.equals(
              streamChannel.attr(WebTransportAttributeKeys.STREAM_NOTIFIED).get())) {
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
          ByteBuf slice = frame.content().retainedSlice();
          try {
            DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(slice);
            slice = null; // ownership transferred to buffer
            try {
              stream.getDataConsumer().accept(buffer);
            } finally {
              buffer.release();
            }
          } catch (Exception e) {
            logger.error("Error in stream onData callback", e);
          } finally {
            if (slice != null) {
              slice.release();
            }
          }
        }
      } else if (frame instanceof WebTransportDatagramFrame) {
        // Fire metrics: datagram received
        WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
        if (metrics != null) {
          metrics.onDatagramReceived(sessionId, frame.content().readableBytes());
        }
        ByteBuf slice = frame.content().retainedSlice();
        try {
          DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(slice);
          slice = null; // ownership transferred to buffer
          try {
            handler.onDatagramReceived(session, buffer);
          } finally {
            buffer.release();
          }
        } catch (Exception e) {
          logger.error("Error in onDatagramReceived callback", e);
        } finally {
          if (slice != null) {
            slice.release();
          }
        }
      }
    } catch (Exception e) {
      logger.error("Exception in tryDispatchToHandler", e);
    }
  }
}
