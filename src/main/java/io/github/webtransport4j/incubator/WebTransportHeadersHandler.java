package io.github.webtransport4j.incubator;

import io.github.webtransport4j.incubator.applayer.ServerPushService;
import io.github.webtransport4j.incubator.applayer.StreamSender;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class WebTransportHeadersHandler extends Http3RequestStreamInboundHandler {
    private static final Logger logger = Logger.getLogger(WebTransportHeadersHandler.class.getName());

    @Override
    protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame) {
        logger.debug("=== [DEBUG] Received HTTP/3 Headers ===");

        // Loop through all headers and print them
        for (Map.Entry<CharSequence, CharSequence> header : frame.headers()) {
            logger.debug(header.getKey() + ": " + header.getValue());
        }

        logger.debug("=======================================");
        logger.debug("📜 HTTP/3 Headers Received: " + frame.headers().path());
        CharSequence scheme = frame.headers().scheme();
        CharSequence authority = frame.headers().authority();
        CharSequence path = frame.headers().path();
        CharSequence method = frame.headers().method();
        CharSequence protocol = frame.headers().get(":protocol");

        // TEST the server GET request
        if ("GET".contentEquals(method)) {
            Http3Headers responseHeaders = new DefaultHttp3Headers();
            responseHeaders.status("200");

            ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
        }
        if ("CONNECT".contentEquals(method)
                && ("webtransport-h3".contentEquals(protocol) || "webtransport".contentEquals(protocol))) {
            QuicChannel quic = (QuicChannel) ctx.channel().parent();
            QuicStreamChannel connectStream = (QuicStreamChannel) ctx.channel();
            long sessionId = connectStream.streamId();
            //verify it is client-iniated bi directional stream as per below RFC
            //https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-15#section-4.4
            // Client-Initiated Bi-Directional: 0x0, 0x4, 0x8, ... → type=0 mod 4
            if (sessionId % 4L != 0L) {
                logger.warn("❌ Rejecting connection from invalid session id: " + sessionId);
                if (quic != null) {
                    quic.close(true, (int) Http3ErrorCode.H3_ID_ERROR.code(), io.netty.buffer.Unpooled.EMPTY_BUFFER);
                } else {
                    ctx.close();
                }
                ReferenceCountUtil.release(frame);
                return;
            }

            // Validate CORS allowed origins
            CharSequence origin = frame.headers().get("origin");
            java.util.List<String> allowed = quic.attr(WebTransportServer.ALLOWED_ORIGINS).get();
            if (!isOriginAllowed(allowed, origin)) {
                logger.warn("❌ Rejecting connection from unauthorized origin: " + origin);
                Http3Headers responseHeaders = new DefaultHttp3Headers();
                responseHeaders.status(HttpResponseStatus.FORBIDDEN.codeAsText());
                ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
                ctx.close();
                ReferenceCountUtil.release(frame);
                return;
            }

            String pathStr = path.toString();
            quic.attr(WebTransportServer.SESSION_PATH_KEY).set(pathStr);

            logger.info("✅ Handshake Success for Path: " + pathStr);
            Http3Headers responseHeaders = new DefaultHttp3Headers();
            responseHeaders.status(HttpResponseStatus.OK.codeAsText());

            ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
            logger.debug("🌊 Stream 0 AutoRead: " + ctx.channel().config().isAutoRead());
            logger.debug("🌊 Stream 0 Pipeline post-handshake: " + ctx.pipeline().names());

            WebTransportSessionManager mgr = quic.attr(WebTransportSessionManager.WT_SESSION_MGR).get();

            
            connectStream.closeFuture().addListener(f -> {
                mgr.unregister(connectStream);
            });
            mgr.register(connectStream);
            
            boolean isConnectSocketIo = pathStr.contains("socket.io");
            boolean enableServerPush = WebTransportConfig.getBoolean("webtransport4j.webtransport.enable_server_push", true);
            if (!isConnectSocketIo && enableServerPush) {
                // Trigger server initiated uni-stream - test code
                logger.debug(
                        "⏰ Creating Server-Push Stream for Session "
                                + sessionId);
                logger.debug("⏳ Creating Push Stream...");
                WebTransportUtils.createUniStream(connectStream, Optional.of(true), new ChannelInboundHandlerAdapter())
                        .addListener(future -> {
                            if (future.isSuccess()) {
                                QuicStreamChannel stream = (QuicStreamChannel) future.getNow();
                                StreamSender sender = new StreamSender(stream);
                                String key = "stream-" + stream.id().asShortText();
                                ServerPushService.INSTANCE.register(key, sender);
                                logger.debug("🚀 Push Stream Ready! ID: " + stream.id());

                                final io.netty.util.concurrent.ScheduledFuture<?>[] pushFuture = new io.netty.util.concurrent.ScheduledFuture<?>[1];
                                pushFuture[0] = quic.eventLoop()
                                        .scheduleAtFixedRate(() -> {
                                            if (stream.isActive() && connectStream.isActive()) {
                                                ServerPushService.INSTANCE.sendTo(key, "Continuous Uni Stream Message: " + System.nanoTime());
                                            } else {
                                                if (pushFuture[0] != null) {
                                                    pushFuture[0].cancel(false);
                                                }
                                            }
                                        }, 0, 1, TimeUnit.SECONDS);

                                connectStream.closeFuture().addListener(f -> {
                                    if (pushFuture[0] != null) {
                                        pushFuture[0].cancel(false);
                                    }
                                    ServerPushService.INSTANCE.unregister(key);
                                    sender.close();
                                });
                            } else {
                                System.err
                                        .println("❌ Failed: " + future.cause());
                            }
                        });
                // TEST: Create a Bi-Directional Stream
                logger.info("⏳ Creating Bi-Directional Stream...");
                ChannelHandler streamInitializer = new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel ch) {
                        ch.pipeline().addFirst(new QuicGlobalSniffer("STREAM-" + ch.streamId()));
                        if (isConnectSocketIo) {
                            ch.pipeline().addLast(new EngineIoFrameDecoder());
                        }
                        ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
                        ch.pipeline().addLast(new WebTransportCapsuleHandler());
                        ch.pipeline().addLast(new MessageDispatcher());
                    }
                };
                WebTransportUtils.createBiStream(connectStream, Optional.of(true), streamInitializer)
                        .addListener(future -> {
                            if (future.isSuccess()) {
                                QuicStreamChannel stream = (QuicStreamChannel) future.getNow();
                                StreamSender sender = new StreamSender(stream);
                                String biKey = "bi-stream-" + stream.id().asShortText();
                                ServerPushService.INSTANCE.register(biKey, sender);
                                logger.info("✅ Bi-Directional Stream Ready! ID: " + stream.id());
                                
                                final io.netty.util.concurrent.ScheduledFuture<?>[] biFuture = new io.netty.util.concurrent.ScheduledFuture<?>[1];
                                biFuture[0] = quic.eventLoop()
                                        .scheduleAtFixedRate(() -> {
                                            if (stream.isActive() && connectStream.isActive()) {
                                                ServerPushService.INSTANCE.sendTo(biKey, "Continuous Bi Stream Message: " + System.nanoTime());
                                            } else {
                                                if (biFuture[0] != null) {
                                                    biFuture[0].cancel(false);
                                                }
                                            }
                                        }, 0, 1, TimeUnit.SECONDS);

                                connectStream.closeFuture().addListener(f -> {
                                    if (biFuture[0] != null) {
                                        biFuture[0].cancel(false);
                                    }
                                    ServerPushService.INSTANCE.unregister(biKey);
                                    sender.close();
                                });
                            } else {
                                logger.error("❌ Failed to create Bi Stream", future.cause());
                            }
                        });
                      
            }

            
        }
       
        ReferenceCountUtil.release(frame);
    }

    private boolean isOriginAllowed(java.util.List<String> allowedOrigins, CharSequence origin) {
        if (allowedOrigins == null || allowedOrigins.isEmpty()) {
            return true;
        }
        if (origin == null) {
            return allowedOrigins.contains("*");
        }
        String originStr = origin.toString();
        if (allowedOrigins.contains("*")) {
            return true;
        }
        return allowedOrigins.contains(originStr);
    }

    @Override
    protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame) {
        ctx.fireChannelRead(frame);
    }

    @Override
    protected void channelInputClosed(ChannelHandlerContext ctx) {
        logger.debug("🔒 Stream Closed: " + ctx.channel().id());
        ctx.close();
    }
}
