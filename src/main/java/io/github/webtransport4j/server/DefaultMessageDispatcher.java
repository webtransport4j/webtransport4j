package io.github.webtransport4j.server;

import io.github.webtransport4j.api.DefaultNettyWebTransportBuffer;
import io.github.webtransport4j.api.DefaultNettyWebTransportStream;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.concurrent.RejectedExecutionException;

import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

public class DefaultMessageDispatcher
        extends SimpleChannelInboundHandler<WebTransportFrame>
        implements MessageDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageDispatcher.class);

    @Override
    protected void channelRead0(@NonNull ChannelHandlerContext ctx, @NonNull WebTransportFrame msg) {
        Channel channel = ctx.channel();
        // 1. Debug: Log the raw hex to see invisible bytes (like 0x00)
        if (logger.isDebugEnabled()) {
            logger.debug("📦 [RAW PAYLOAD] {}", WebTransportUtils.formatHexBytes(msg.content()));
        }
        long sessionId = msg.sessionId();
        // 2. Offload to Business Logic
        msg.retain();
        final long finalSessionId = sessionId;
        java.util.concurrent.ExecutorService executor;
        if (channel instanceof QuicStreamChannel) {
            executor = ((QuicStreamChannel) channel).parent().attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).get();
        } else {
            executor = channel.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).get();
        }
        if (executor == null) {
            try {
                tryDispatchToHandler(channel, finalSessionId, msg);
            } catch (Throwable t) {
                logger.error("Uncaught exception/error during business logic execution", t);
            } finally {
                msg.release();
            }
        } else {
            boolean submitted = false;
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("📤 Submitting task to executor: {}", executor.getClass().getSimpleName());
                }
                executor.execute(() -> {
                    try {
                        tryDispatchToHandler(channel, finalSessionId, msg);
                    } catch (Throwable t) {
                        logger.error("Uncaught exception/error during business logic execution", t);
                    } finally {
                        msg.release();
                    }
                });
                submitted = true;
            } catch (RejectedExecutionException e) {
                logger.error("❌ REJECTED: Task submission rejected by business executor (queue full?). " +
                        "Executor: {} | SessionID: {} | Shutting down stream with WT_SESSION_GONE",
                        executor.getClass().getSimpleName(), finalSessionId, e);
                if (channel instanceof QuicStreamChannel) {
                    ((QuicStreamChannel) channel).shutdown(WebTransportUtils.WT_SESSION_GONE, channel.newPromise());
                } else {
                    channel.close();
                }
            } catch (Throwable t) {
                logger.error("❌ FAILED: Failed to submit task to business executor. " +
                        "Executor: {} | SessionID: {} | Cause: {}",
                        executor.getClass().getSimpleName(), finalSessionId, t.getMessage(), t);
                if (channel instanceof QuicStreamChannel) {
                    ((QuicStreamChannel) channel).shutdown(WebTransportUtils.WT_SESSION_GONE, channel.newPromise());
                } else {
                    channel.close();
                }
            } finally {
                if (!submitted) {
                    msg.release();
                }
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
                logger.info("🌊 Stream reset by peer with WebTransport application error code: 0x{} ({})",
                        Long.toHexString(wtErrorCode), wtErrorCode);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("🌊 Stream reset by peer with HTTP/3 error code: 0x{}",
                            Long.toHexString(httpErrorCode));
                }
            }
        } else {
            logger.error("❌ Pipeline error: ", cause);
        }
        ctx.close();
    }

    private void tryDispatchToHandler(@NonNull Channel channel, long sessionId, @NonNull WebTransportFrame frame) {
        WebTransportSessionManager mgr;
        if (channel instanceof QuicStreamChannel) {
            mgr = ((QuicStreamChannel) channel).parent().attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
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
        WebTransportHandler handler = (server != null) ? server.getHandler(session.path()) : new WebTransportHandler() {
        };
        try {
            if (frame instanceof WebTransportStreamFrame) {
                if (!(channel instanceof QuicStreamChannel)) {
                    throw new RuntimeException("Implemented only for QuicStreamChannel");
                }
                QuicStreamChannel streamChannel = (QuicStreamChannel) channel;
                WebTransportStream stream = streamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY).get();
                if (stream == null) {
                    stream = new DefaultNettyWebTransportStream(streamChannel, sessionId);
                    streamChannel.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(stream);
                    final WebTransportStream finalStream = stream;
                    streamChannel.closeFuture().addListener(f -> {
                        if (finalStream.getCloseHandler() != null) {
                            try {
                                finalStream.getCloseHandler();
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
                        stream.getDataConsumer().accept(new DefaultNettyWebTransportBuffer(frame.content()));
                    } catch (Exception e) {
                        logger.error("Error in stream onData callback", e);
                    }
                }
            } else if (frame instanceof WebTransportDatagramFrame) {
                // Fire metrics: datagram received
                WebTransportMetricsListener metrics = WebTransportUtils.getMetrics(channel);
                if (metrics != null) {
                    metrics.onDatagramReceived(sessionId, frame.content().readableBytes());
                }
                try {
                    handler.onDatagramReceived(session, new DefaultNettyWebTransportBuffer(frame.content()));
                } catch (Exception e) {
                    logger.error("Error in onDatagramReceived callback", e);
                }
            }
        } catch (Exception e) {
            logger.error("Exception in tryDispatchToHandler", e);
        }
    }
}
