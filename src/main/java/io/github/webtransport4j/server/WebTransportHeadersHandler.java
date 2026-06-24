package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

class WebTransportHeadersHandler extends Http3RequestStreamInboundHandler {
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
      // Validate scheme: MUST be "https" as per draft-15 section 4.4
      if (scheme == null || !"https".contentEquals(scheme)) {
        logger.warn("❌ Rejecting connection from invalid scheme: " + scheme);
        Http3Headers responseHeaders = new DefaultHttp3Headers();
        responseHeaders.status(HttpResponseStatus.BAD_REQUEST.codeAsText());
        ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
        ctx.close();
        ReferenceCountUtil.release(frame);
        return;
      }

      // Validate authority: MUST be present as per draft-15 section 4.4
      if (authority == null || authority.length() == 0) {
        logger.warn("❌ Rejecting connection due to missing :authority");
        Http3Headers responseHeaders = new DefaultHttp3Headers();
        responseHeaders.status(HttpResponseStatus.BAD_REQUEST.codeAsText());
        ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
        ctx.close();
        ReferenceCountUtil.release(frame);
        return;
      }

      QuicChannel quic = (QuicChannel) ctx.channel().parent();
      QuicStreamChannel connectStream = (QuicStreamChannel) ctx.channel();

      if (quic != null) {
        io.netty.util.Attribute<Boolean> receivedAttr = quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED);
        io.netty.util.Attribute<Boolean> validAttr = quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID);
        Boolean settingsReceived = receivedAttr != null ? receivedAttr.get() : null;
        Boolean settingsValid = validAttr != null ? validAttr.get() : null;
        if (Boolean.TRUE.equals(settingsReceived) && !Boolean.TRUE.equals(settingsValid)) {
          logger.warn(
              "❌ WebTransport peer settings are invalid: Client does not support H3 Datagrams."
                  + " Treating incoming session CONNECT stream as malformed and resetting with"
                  + " H3_MESSAGE_ERROR (0x010e).");
          connectStream.shutdown(0x010e, connectStream.newPromise());
          ReferenceCountUtil.release(frame);
          return;
        }
      }

      long sessionId = connectStream.streamId();
      // verify it is client-iniated bi directional stream as per below RFC
      // https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-15#section-4.4
      // Client-Initiated Bi-Directional: 0x0, 0x4, 0x8, ... → type=0 mod 4
      if (sessionId % 4L != 0L) {
        logger.warn("❌ Rejecting connection from invalid session id: " + sessionId);
        if (quic != null) {
          quic.close(
              true, Http3ErrorCode.H3_ID_ERROR.code(), io.netty.buffer.Unpooled.EMPTY_BUFFER);
        } else {
          ctx.close();
        }
        ReferenceCountUtil.release(frame);
        return;
      }

      // Validate CORS allowed origins and authority host
      CharSequence origin = frame.headers().get("origin");
      java.util.List<String> allowed = quic.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).get();
      if (!isAllowed(allowed, origin, authority)) {
        logger.warn(
            "❌ Rejecting connection from unauthorized origin: "
                + origin
                + " (authority: "
                + authority
                + ")");
        Http3Headers responseHeaders = new DefaultHttp3Headers();
        responseHeaders.status(HttpResponseStatus.FORBIDDEN.codeAsText());
        ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
        ctx.close();
        ReferenceCountUtil.release(frame);
        return;
      }

      WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
      int maxSessions = WebTransportConfig.getInt("webtransport4j.webtransport.max_sessions_per_connection", 1);
      if (mgr != null && mgr.sessionsSize() >= maxSessions) {
        logger.warn(
            "❌ Rejecting connection: Max simultaneous sessions per connection reached ("
                + maxSessions
                + ")");
        Http3Headers responseHeaders = new DefaultHttp3Headers();
        responseHeaders.status(HttpResponseStatus.TOO_MANY_REQUESTS.codeAsText());
        ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
        ctx.close();
        ReferenceCountUtil.release(frame);
        return;
      }

      String pathStr = path.toString();
      quic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set(pathStr);

      logger.info("✅ Handshake Success for Path: " + pathStr);
      Http3Headers responseHeaders = new DefaultHttp3Headers();
      responseHeaders.status(HttpResponseStatus.OK.codeAsText());

      ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
      logger.debug("🌊 Stream 0 AutoRead: " + ctx.channel().config().isAutoRead());
      logger.debug("🌊 Stream 0 Pipeline post-handshake: " + ctx.pipeline().names());

      if (mgr != null) {
        connectStream
            .closeFuture()
            .addListener(
                f -> {
                  mgr.unregister(connectStream);
                });
        mgr.register(connectStream);
      }


      // --- DEV ENGINE CONTINUOUS SERVER PUSH IMPLEMENTATION (JAVA 8) ---
// REMOVE when production ready
      boolean pushEnabled = WebTransportConfig.getBoolean("webtransport4j.dev.server.push.enabled", false);

      if (pushEnabled && mgr != null) {
        long intervalMs = WebTransportConfig.getLong("webtransport4j.dev.server.push.interval.ms", 1000L);
        final boolean bidiEnabled = WebTransportConfig.getBoolean("webtransport4j.dev.server.push.bidi.enabled", true);
        final boolean uniEnabled = WebTransportConfig.getBoolean("webtransport4j.dev.server.push.uni.enabled", true);
        final boolean datagramEnabled = WebTransportConfig.getBoolean("webtransport4j.dev.server.push.datagram.enabled", true);
        final int paddingBytes = WebTransportConfig.getInt("webtransport4j.dev.server.push.payload.padding.bytes", 0);

        logger.info("🧪 [DEV ENGINE] Spawning continuous telemetry scheduler loop every " + intervalMs + "ms");

        // Track active streams globally to reuse them across scheduler ticks
        // Using Object as the key type to represent the Session, adjust to your specific WebTransportSession class
        final java.util.concurrent.ConcurrentHashMap<Object, WebTransportStream> activeBidiStreams = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.concurrent.ConcurrentHashMap<Object, WebTransportStream> activeUniStreams = new java.util.concurrent.ConcurrentHashMap<>();

        io.netty.util.concurrent.ScheduledFuture<?> pushTask = ctx.executor().scheduleAtFixedRate(() -> {

          long timestamp = System.currentTimeMillis();

          StringBuilder padBuilder = new StringBuilder();
          if (paddingBytes > 0) {
            for (int i = 0; i < paddingBytes; i++) {
              padBuilder.append("X");
            }
          }
          final String padding = padBuilder.toString();
          final String basePayload = timestamp + "_SERVER_PUSH_";
          final String fullPayload = basePayload + padding;

          mgr.getSessions().forEach(session -> {
            if (session == null) return;
            if (bidiEnabled) {
              WebTransportStream existingBidi = activeBidiStreams.get(session);

              if (existingBidi != null) {
                // Reuse existing stream
                existingBidi.writeText(fullPayload + "_Bi").addListener(writeResult -> {
                  if (!writeResult.isSuccess()) {
                    logger.warn("⚠️ [DEV BIDI] Write failed, dropping cached stream ID: " + existingBidi.streamId());
                    activeBidiStreams.remove(session); // Purge dead stream so a new one is created next tick
                  }
                    logger.info("📤 [DEV BIDI OUT] Pushed to existing stream ID: " + existingBidi.streamId());

                });
              } else {
                // Create and cache new stream
                session.createBiStream().addListener((Future<WebTransportStream> f) -> {
                  if (!f.isSuccess()) {
                     logger.warn("⚠️ [DEV BIDI] Stream creation denied. Max allocations exhausted.");
                    return;
                  }
                  WebTransportStream stream = f.getNow();
                  activeBidiStreams.put(session, stream); // Cache for future ticks

                  stream.writeText(fullPayload + "_Bi").addListener(writeResult -> {
                    if (writeResult.isSuccess()) {
                      logger.info("📤 [DEV BIDI OUT] Allocated NEW stream frame ID: " + stream.streamId());
                    }
                  });
                });
              }
            }

            if (uniEnabled) {
              WebTransportStream existingUni = activeUniStreams.get(session);

              if (existingUni != null) {
                // Reuse existing stream
                existingUni.writeText(fullPayload + "_Uni").addListener(writeResult -> {
                  if (!writeResult.isSuccess()) {
                    logger.warn("⚠️ [DEV UNI] Write failed, dropping cached stream ID: " + existingUni.streamId());
                    activeUniStreams.remove(session);
                  }
                  logger.info("📤 [DEV UNI OUT] Pushed to existing stream ID: " + existingUni.streamId());
                });
              } else {
                // Create and cache new stream
                session.createUniStream().addListener((Future<WebTransportStream> f) -> {
                  if (!f.isSuccess()) {
                    logger.warn("⚠️ [DEV UNI] Stream creation denied. Flow control choked.");
                    return;
                  }
                  WebTransportStream stream = f.getNow();
                  activeUniStreams.put(session, stream); // Cache for future ticks

                  stream.writeText(fullPayload + "_Uni").addListener(writeResult -> {
                    if (writeResult.isSuccess()) {
                      logger.info("📤 [DEV UNI OUT] Allocated NEW stream frame ID: " + stream.streamId());
                    }
                  });
                });
              }
            }

            // 3. Continuous Unreliable Datagram Frames (UNCHANGED)
            if (datagramEnabled) {
              byte[] rawDatagram = (fullPayload + "_Datagram").getBytes(java.nio.charset.StandardCharsets.UTF_8);
              session.sendDatagram(rawDatagram).addListener(writeResult -> {
                if (writeResult.isSuccess()) {
                  logger.info("⚡ [DEV DATAGRAM OUT] packet flushed directly to connection pipe.");
                }
              });
            }
          });

        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        // Terminate scheduling task immediately if the parent control stream dies
        connectStream.closeFuture().addListener(f -> {
          logger.info("🛑 [DEV ENGINE] Control stream terminated. Cleaning up loop engine context.");
          pushTask.cancel(false);
          activeBidiStreams.clear();
          activeUniStreams.clear();
        });
      }
    }

    
    ReferenceCountUtil.release(frame);
  }

  private boolean isAllowed(
      java.util.List<String> allowedOrigins, CharSequence origin, CharSequence authority) {
    if (allowedOrigins == null || allowedOrigins.isEmpty() || allowedOrigins.contains("*")) {
      return true;
    }

    // If origin is present, we MUST validate it (no fallback to authority if it
    // fails validation)
    if (origin != null) {
      String originHost = extractHost(origin.toString());
      return originHost != null && allowedOrigins.contains(originHost);
    }

    // If origin is absent (non-browser clients), fall back to checking host
    // extracted from
    // authority
    if (authority != null) {
      String authorityHost = extractHost(authority.toString());
      return authorityHost != null && allowedOrigins.contains(authorityHost);
    }

    return false;
  }

  private String extractHost(String value) {
    if (value == null) {
      return null;
    }
    try {
      String uriStr = value.trim();
      if (!uriStr.contains("://")) {
        uriStr = "https://" + uriStr;
      }
      java.net.URI uri = new java.net.URI(uriStr);
      String host = uri.getHost();
      if (host == null) {
        // In case URI host is null (e.g. for "*"), fallback to value
        return uriStr.substring(uriStr.indexOf("://") + 3);
      }
      return host;
    } catch (Exception e) {
      return null;
    }
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
