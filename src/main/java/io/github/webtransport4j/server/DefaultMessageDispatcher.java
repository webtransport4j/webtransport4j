package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.util.Attribute;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default dispatcher routing WebTransport frames and buffers directly to handlers. */
@ChannelHandler.Sharable
public class DefaultMessageDispatcher extends ChannelInboundHandlerAdapter
    implements MessageDispatcher {

  public static final DefaultMessageDispatcher INSTANCE = new DefaultMessageDispatcher();

  /**
   * Config key: milliseconds a user handler callback may run before it is reported as slow.
   * Defaults to 5000 ms; it is a correctness tripwire, not a performance alarm, because every
   * callback runs on the connection event loop and a blocking one stalls all streams, datagrams,
   * and capsules on that connection, including its own flow control.
   */
  public static final String SLOW_HANDLER_WARN_MS_KEY =
      "webtransport4j.dispatch.slow_handler.warn_ms";

  private static final long DEFAULT_SLOW_HANDLER_WARN_MS = 5000L;

  private static final Logger logger = LoggerFactory.getLogger(DefaultMessageDispatcher.class);

  private static final WebTransportHandler NOOP_HANDLER = new WebTransportHandler() {};

  /**
   * One thread-confined flyweight buffer per dispatching thread. Dispatch always runs on the
   * channel's {@code EventLoop}, so a single instance per thread is safe.
   */
  private static final ThreadLocal<FlyweightWebTransportBuffer> DISPATCH_BUFFER =
      ThreadLocal.withInitial(FlyweightWebTransportBuffer::createFlyweight);

  @Override
  public void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Object msg)
      throws Exception {
    Channel channel = ctx.channel();
    try {
      if (msg instanceof ByteBuf) {
        ByteBuf content = (ByteBuf) msg;
        if (channel instanceof QuicStreamChannel) {
          QuicStreamChannel streamChannel = (QuicStreamChannel) channel;
          Long sessionId = streamChannel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();
          if (sessionId == null) {
            content.release();
            WebTransportUtils.rejectClientStream(ctx, WebTransportUtils.WT_SESSION_GONE);
            return;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("📦 [STREAM PAYLOAD] {}", WebTransportUtils.formatHexBytes(content));
          }
          tryDispatchStreamToHandler(streamChannel, sessionId, content);
        } else {
          long quarterSessionId = WebTransportUtils.readVariableLengthInt(content);
          if (quarterSessionId == -1) {
            content.release();
            return;
          }
          long sessionId = quarterSessionId << 2;
          if (logger.isDebugEnabled()) {
            logger.debug("📦 [DATAGRAM PAYLOAD] {}", WebTransportUtils.formatHexBytes(content));
          }
          tryDispatchDatagramToHandler(channel, sessionId, content);
        }
        return;
      }
      if (msg instanceof WebTransportStreamFrame) {
        WebTransportStreamFrame frame = (WebTransportStreamFrame) msg;
        try {
          if (channel instanceof QuicStreamChannel) {
            tryDispatchStreamToHandler(
                (QuicStreamChannel) channel, frame.sessionId(), frame.content().retain());
          }
        } finally {
          frame.release();
        }
        return;
      }
      if (msg instanceof WebTransportDatagramFrame) {
        WebTransportDatagramFrame frame = (WebTransportDatagramFrame) msg;
        try {
          tryDispatchDatagramToHandler(channel, frame.sessionId(), frame.content().retain());
        } finally {
          frame.release();
        }
        return;
      }
      ctx.fireChannelRead(msg);
    } catch (Throwable t) {
      logger.error("Uncaught exception/error during message dispatch", t);
      notifyHandlerError(channel, t);
    }
  }

  @Override
  public void exceptionCaught(@NonNull ChannelHandlerContext ctx, @NonNull Throwable cause) {
    if (ctx.channel() instanceof QuicStreamChannel) {
      QuicStreamChannel streamChannel = (QuicStreamChannel) ctx.channel();
      WebTransportStream stream = streamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
      if (stream != null && stream.getErrorHandler() != null) {
        long errorStart = System.nanoTime();
        try {
          stream.getErrorHandler().accept(cause);
        } catch (Exception e) {
          logger.error("Error in stream onError handler", e);
        }
        warnIfSlowHandler(streamChannel, "onError", errorStart);
      }
      notifyHandlerError(streamChannel, cause);
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

  private void notifyHandlerError(
      @NonNull Channel channel, long sessionId, @NonNull Throwable cause) {
    try {
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
        return;
      }
      WebTransportServer server;
      if (channel instanceof QuicStreamChannel) {
        Attribute<WebTransportServer> attr =
            ((QuicStreamChannel) channel).parent().attr(WebTransportAttributeKeys.SERVER_KEY);
        server = attr != null ? attr.get() : null;
      } else {
        Attribute<WebTransportServer> attr = channel.attr(WebTransportAttributeKeys.SERVER_KEY);
        server = attr != null ? attr.get() : null;
      }
      WebTransportHandler handler = (server != null) ? server.getHandler(session.path()) : null;
      if (handler != null) {
        long errorStart = System.nanoTime();
        try {
          handler.onError(session, cause);
        } catch (Exception e) {
          logger.error("Error in handler onError callback", e);
        }
        warnIfSlowHandler(channel, "onError", errorStart);
      }
    } catch (Exception ex) {
      logger.error("Error dispatching onError to handler", ex);
    }
  }

  private void notifyHandlerError(@NonNull Channel channel, @NonNull Throwable cause) {
    if (channel instanceof QuicStreamChannel) {
      Long sessionId = channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get();
      if (sessionId != null) {
        notifyHandlerError(channel, sessionId, cause);
      }
    }
  }

  protected void tryDispatchStreamToHandler(
      @NonNull QuicStreamChannel streamChannel, long sessionId, @NonNull ByteBuf content) {
    WebTransportSessionManager mgr =
        streamChannel.parent().attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    if (mgr == null) {
      content.release();
      return;
    }
    WebTransportSession session = mgr.get(sessionId);
    if (session == null || !session.isOpen()) {
      content.release();
      return;
    }
    Attribute<WebTransportServer> attr =
        streamChannel.parent().attr(WebTransportAttributeKeys.SERVER_KEY);
    WebTransportServer server = (attr != null) ? attr.get() : null;
    WebTransportHandler handler =
        (server != null) ? server.getHandler(session.path()) : NOOP_HANDLER;
    if (handler == null) {
      content.release();
      return;
    }
    try {
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
          long notifyStart = System.nanoTime();
          try {
            handler.onIncomingStream(session, stream);
          } catch (Exception e) {
            logger.error("Error in onIncomingStream callback", e);
          }
          warnIfSlowHandler(streamChannel, "onIncomingStream", notifyStart);
        }
      }

      // Dispatch data
      Consumer<ByteBuf> rawConsumer = null;
      if (stream instanceof NettyWebTransportStream) {
        rawConsumer = ((NettyWebTransportStream) stream).getRawByteBufConsumer();
      }
      long dispatchStart = System.nanoTime();
      String dataCallback;
      if (rawConsumer != null) {
        dataCallback = "onRawByteBuf";
        dispatchRawStreamData(rawConsumer, content);
      } else if (stream.getDataConsumer() != null) {
        dataCallback = "onData";
        dispatchStreamData(stream, content);
      } else {
        dataCallback = null;
        content.release();
      }
      if (dataCallback != null) {
        warnIfSlowHandler(streamChannel, dataCallback, dispatchStart);
      }
    } catch (Throwable t) {
      logger.error("Exception in tryDispatchStreamToHandler", t);
      if (content.refCnt() > 0) {
        content.release();
      }
      long errorStart = System.nanoTime();
      try {
        handler.onError(session, t);
      } catch (Exception ex) {
        logger.error("Error in handler onError callback", ex);
      }
      warnIfSlowHandler(streamChannel, "onError", errorStart);
    }
  }

  protected void tryDispatchDatagramToHandler(
      @NonNull Channel channel, long sessionId, @NonNull ByteBuf content) {
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
      content.release();
      return;
    }
    WebTransportSession session = mgr.get(sessionId);
    if (session == null || !session.isOpen()) {
      content.release();
      WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
      if (metrics != null) {
        metrics.onDatagramDiscarded(sessionId, "session_not_found_at_dispatch");
      }
      return;
    }
    Attribute<WebTransportServer> attr;
    if (channel instanceof QuicStreamChannel) {
      attr = ((QuicStreamChannel) channel).parent().attr(WebTransportAttributeKeys.SERVER_KEY);
    } else {
      attr = channel.attr(WebTransportAttributeKeys.SERVER_KEY);
    }
    WebTransportServer server = (attr != null) ? attr.get() : null;
    WebTransportHandler handler =
        (server != null) ? server.getHandler(session.path()) : NOOP_HANDLER;
    if (handler == null) {
      content.release();
      return;
    }
    try {
      WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
      if (metrics != null) {
        metrics.onDatagramReceived(sessionId, content.readableBytes());
      }
      Consumer<ByteBuf> rawDatagramConsumer = null;
      if (session instanceof NettyWebTransportSession) {
        rawDatagramConsumer = ((NettyWebTransportSession) session).getRawDatagramConsumer();
      }
      long dispatchStart = System.nanoTime();
      if (rawDatagramConsumer != null) {
        dispatchRawDatagramData(rawDatagramConsumer, content);
        warnIfSlowHandler(channel, "onRawDatagram", dispatchStart);
      } else {
        dispatchDatagramData(handler, session, content);
        warnIfSlowHandler(channel, "onDatagramReceived", dispatchStart);
      }
    } catch (Throwable t) {
      logger.error("Exception in tryDispatchDatagramToHandler", t);
      if (content.refCnt() > 0) {
        content.release();
      }
      long errorStart = System.nanoTime();
      try {
        handler.onError(session, t);
      } catch (Exception ex) {
        logger.error("Error in handler onError callback", ex);
      }
      warnIfSlowHandler(channel, "onError", errorStart);
    }
  }

  protected void tryDispatchToHandler(
      @NonNull Channel channel, long sessionId, @NonNull WebTransportFrame frame) {
    if (frame instanceof WebTransportStreamFrame) {
      if (channel instanceof QuicStreamChannel) {
        tryDispatchStreamToHandler(
            (QuicStreamChannel) channel, sessionId, frame.content().retain());
      }
    } else if (frame instanceof WebTransportDatagramFrame) {
      tryDispatchDatagramToHandler(channel, sessionId, frame.content().retain());
    }
  }

  /**
   * Borrows the thread-confined flyweight buffer for this dispatch. A buffer that a consumer
   * promoted via {@code retain()} stays live for that consumer, so the shared slot is replaced
   * with a fresh instance. A buffer still attached to an outer dispatch on this thread
   * (re-entrant callbacks) is left in the slot and a fresh instance is used instead.
   */
  private static FlyweightWebTransportBuffer borrowDispatchBuffer() {
    FlyweightWebTransportBuffer buffer = DISPATCH_BUFFER.get();
    if (buffer.isReusable()) {
      return buffer;
    }
    FlyweightWebTransportBuffer fresh = FlyweightWebTransportBuffer.createFlyweight();
    if (buffer.isPromoted()) {
      DISPATCH_BUFFER.set(fresh);
    }
    return fresh;
  }

  /**
   * Dispatches incoming stream data to the stream consumer.
   */
  protected void dispatchStreamData(
      @NonNull WebTransportStream stream, @NonNull ByteBuf content) {
    FlyweightWebTransportBuffer buffer = borrowDispatchBuffer().attach(content);
    try {
      stream.getDataConsumer().accept(buffer);
    } catch (Exception e) {
      logger.error("Error in stream onData callback", e);
    } finally {
      buffer.detach();
      content.release();
    }
  }

  /**
   * Dispatches incoming raw ByteBuf stream data to the stream consumer.
   */
  protected void dispatchRawStreamData(
      @NonNull Consumer<ByteBuf> rawConsumer, @NonNull ByteBuf content) {
    try {
      rawConsumer.accept(content);
    } catch (Exception e) {
      logger.error("Error in stream onRawByteBuf callback", e);
    } finally {
      content.release();
    }
  }

  /**
   * Dispatches incoming raw ByteBuf datagram data to the raw datagram consumer.
   */
  protected void dispatchRawDatagramData(
      @NonNull Consumer<ByteBuf> rawDatagramConsumer, @NonNull ByteBuf content) {
    try {
      rawDatagramConsumer.accept(content);
    } catch (Exception e) {
      logger.error("Error in onRawDatagram callback", e);
    } finally {
      content.release();
    }
  }

  /**
   * Dispatches incoming datagram data to the handler.
   */
  protected void dispatchDatagramData(
      @NonNull WebTransportHandler handler,
      @NonNull WebTransportSession session,
      @NonNull ByteBuf content) {
    FlyweightWebTransportBuffer buffer = borrowDispatchBuffer().attach(content);
    try {
      handler.onDatagramReceived(session, buffer);
    } catch (Exception e) {
      logger.error("Error in onDatagramReceived callback", e);
    } finally {
      buffer.detach();
      content.release();
    }
  }

  /**
   * Reports a user handler callback that ran longer than {@link #SLOW_HANDLER_WARN_MS_KEY}.
   * Callbacks run directly on the channel event loop, so a slow or blocking one stalls every
   * stream, datagram, and capsule on the connection, including its own flow control. The warning
   * is emitted once per channel; subsequent overruns stay silent until the channel is replaced.
   */
  private void warnIfSlowHandler(
      @NonNull Channel channel, @NonNull String callbackName, long startNanos) {
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    long warnMs =
        WebTransportConfig.getLong(SLOW_HANDLER_WARN_MS_KEY, DEFAULT_SLOW_HANDLER_WARN_MS);
    if (elapsedMs <= warnMs) {
      return;
    }
    if (channel.attr(WebTransportAttributeKeys.SLOW_HANDLER_WARNED).setIfAbsent(Boolean.TRUE)
        == null) {
      logger.warn(
          "🐢 Slow WebTransport handler callback '{}' took {} ms on channel {} (threshold {}"
              + " ms). Handler callbacks run on the connection event loop; blocking them stalls"
              + " all streams and datagrams on this connection. Offload blocking work to an"
              + " executor, e.g. via ExecutorOffloadDispatcher.",
          callbackName,
          elapsedMs,
          channel.id(),
          warnMs);
    }
  }
}
