package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.resilience.OverloadProtectionPolicy;
import io.github.webtransport4j.security.OriginValidator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Headers;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLEngine;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Inbound handler for HTTP/3 request streams and WebTransport extended CONNECT requests. */
@ChannelHandler.Sharable
public class WebTransportHeadersHandler extends Http3RequestStreamInboundHandler {

  public static final WebTransportHeadersHandler INSTANCE = new WebTransportHeadersHandler();

  public static final String UPGRADE_TOKEN_H3 = "webtransport-h3";
  public static final String UPGRADE_TOKEN_LEGACY = "webtransport";
  public static final String HEADER_WT_AVAILABLE_PROTOCOLS = "wt-available-protocols";
  public static final String HEADER_WT_PROTOCOL = "wt-protocol";

  private static final Logger logger = LoggerFactory.getLogger(WebTransportHeadersHandler.class);

  public WebTransportHeadersHandler() {}

  @Override
  protected void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Http3HeadersFrame frame) {
    if (logger.isDebugEnabled()) {
      logger.debug("=== [DEBUG] Received HTTP/3 Headers ===");
    }
    // Loop through all headers and print them
    for (Map.Entry<CharSequence, CharSequence> header : frame.headers()) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {}", header.getKey(), header.getValue());
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("=======================================");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("📜 HTTP/3 Headers Received: {}", frame.headers().path());
    }
    CharSequence scheme = frame.headers().scheme();
    CharSequence authority = frame.headers().authority();
    CharSequence path = frame.headers().path();
    CharSequence method = frame.headers().method();
    CharSequence protocol = frame.headers().get(":protocol");
    CharSequence origin = frame.headers().get("origin");
    if (method == null || scheme == null || authority == null || path == null) {
      Http3Headers headers = new DefaultHttp3Headers();
      headers.status(HttpResponseStatus.BAD_REQUEST.codeAsText());
      ctx.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
      return;
    }

    // TEST the server GET request
    if ("GET".contentEquals(method)) {
      Http3Headers responseHeaders = new DefaultHttp3Headers();
      responseHeaders.status(HttpResponseStatus.OK.codeAsText());
      ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
      ByteBuf body = ctx.alloc().buffer();
      body.writeCharSequence("Hello HTTP/3", StandardCharsets.UTF_8);

      ctx.writeAndFlush(new DefaultHttp3DataFrame(body))
          .addListener(
              f -> {
                if (f.isSuccess()) {
                  ((QuicStreamChannel) ctx.channel()).shutdownOutput();
                } else {
                  logger.error("❌ Failed to send response body", f.cause());
                  ctx.close();
                }
              });

      return;
    }
    if ("CONNECT".contentEquals(method)
        && (UPGRADE_TOKEN_H3.contentEquals(protocol)
            || UPGRADE_TOKEN_LEGACY.contentEquals(protocol))) {
      QuicChannel quic = (QuicChannel) ctx.channel().parent();
      QuicStreamChannel connectStream = (QuicStreamChannel) ctx.channel();
      WebTransportMetricsListener metricsListener =
          quic != null && quic.attr(WebTransportAttributeKeys.METRICS_LISTENER) != null
              ? quic.attr(WebTransportAttributeKeys.METRICS_LISTENER).get()
              : null;

      // Validate scheme: MUST be "https" as per draft-15 section 4.4
      if (!"https".contentEquals(scheme)) {
        if (metricsListener != null) {
          metricsListener.onSessionRejected("invalid_scheme");
        }
        logger.warn("❌ Rejecting connection from invalid scheme: {}", scheme);
        Http3Headers responseHeaders = new DefaultHttp3Headers();
        responseHeaders.status(HttpResponseStatus.BAD_REQUEST.codeAsText());
        ChannelFuture f = ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
        if (f != null) {
          f.addListener(ChannelFutureListener.CLOSE);
        }
        return;
      }
      // Validate authority: MUST be present as per draft-15 section 4.4
      if (authority.length() == 0) {
        if (metricsListener != null) {
          metricsListener.onSessionRejected("missing_authority");
        }
        logger.warn("❌ Rejecting connection due to missing :authority");
        Http3Headers responseHeaders = new DefaultHttp3Headers();
        responseHeaders.status(HttpResponseStatus.BAD_REQUEST.codeAsText());
        ChannelFuture f = ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
        if (f != null) {
          f.addListener(ChannelFutureListener.CLOSE);
        }
        return;
      }
      if (quic != null) {
        Attribute<WebTransportServer> serverAttribute = quic.attr(WebTransportAttributeKeys.SERVER_KEY);
        WebTransportServer admissionServer = serverAttribute == null ? null : serverAttribute.get();
        Attribute<Boolean> connectionDrain = quic.attr(WebTransportAttributeKeys.CONNECTION_DRAINING);
        if ((admissionServer != null && !admissionServer.isAcceptingSessions())
            || (connectionDrain != null && Boolean.TRUE.equals(connectionDrain.get()))) {
          if (metricsListener != null) {
            metricsListener.onSessionRejected("server_draining");
          }
          ctx.writeAndFlush(new DefaultHttp3HeadersFrame(
              new DefaultHttp3Headers().status(HttpResponseStatus.SERVICE_UNAVAILABLE.codeAsText())))
              .addListener(ChannelFutureListener.CLOSE);
          return;
        }
        Attribute<Boolean> receivedAttr =
            quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED);
        Attribute<Boolean> validAttr = quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID);
        Boolean settingsReceived = receivedAttr != null ? receivedAttr.get() : null;
        Boolean settingsValid = validAttr != null ? validAttr.get() : null;
        if (Boolean.TRUE.equals(settingsReceived) && !Boolean.TRUE.equals(settingsValid)) {
          if (metricsListener != null) {
            metricsListener.onSessionRejected("invalid_peer_settings");
          }
          logger.warn(
              "❌ WebTransport peer settings are invalid: Client does not support H3 Datagrams."
                  + " Treating incoming session CONNECT stream as malformed and resetting with"
                  + " H3_MESSAGE_ERROR (0x010e).");
          connectStream.shutdown(0x010e, connectStream.newPromise());
          return;
        }
        long sessionId = connectStream.streamId();
        // verify it is client-initiated bi directional stream as per RFC 9000 section 2.1
        // and https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-15#section-4.4
        if (!WebTransportUtils.isClientInitiatedBidirectionalStream(sessionId)) {
          if (metricsListener != null) {
            metricsListener.onSessionRejected("invalid_stream_id");
          }
          logger.warn("❌ Rejecting connection from invalid session id: {}", sessionId);
          quic.close(true, Http3ErrorCode.H3_ID_ERROR.code(), Unpooled.EMPTY_BUFFER);
          return;
        }
        // Validate CORS allowed origins and authority host
        List<String> allowed = quic.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).get();
        OriginValidator originValidator = quic.attr(WebTransportAttributeKeys.ORIGIN_VALIDATOR).get();
        Boolean strictOrigin = quic.attr(WebTransportAttributeKeys.STRICT_ORIGIN_VALIDATION).get();
        if (!isAllowed(allowed, originValidator, strictOrigin, origin, authority)) {
          if (metricsListener != null) {
            metricsListener.onSessionRejected("unauthorized_origin");
          }
          logger.warn(
              "❌ Rejecting connection from unauthorized origin: {} (authority: {})",
              origin,
              authority);
          Http3Headers responseHeaders = new DefaultHttp3Headers();
          responseHeaders.status(HttpResponseStatus.FORBIDDEN.codeAsText());
          ChannelFuture f = ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
          if (f != null) {
            f.addListener(ChannelFutureListener.CLOSE);
          }
          return;
        }
        WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
        int maxSessions =
            WebTransportConfig.getInt("webtransport4j.webtransport.max_sessions_per_connection", 1);
        if (mgr == null || !mgr.reserveSession(quic, maxSessions)) {
          if (metricsListener != null) {
            metricsListener.onSessionRejected("max_sessions_per_connection");
          }
          logger.warn(
              "❌ Rejecting connection: Max simultaneous sessions per connection reached ({})",
              maxSessions);
          Http3Headers responseHeaders = new DefaultHttp3Headers();
          responseHeaders.status(HttpResponseStatus.TOO_MANY_REQUESTS.codeAsText());
          ChannelFuture f = ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
          if (f != null) {
            f.addListener(ChannelFutureListener.CLOSE);
          }
          return;
        }

        Attribute<AtomicInteger> slotsAttr =
            quic.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS);
        AtomicInteger globalSlots = slotsAttr != null ? slotsAttr.get() : null;
        int globalMaxSessions =
            WebTransportConfig.getInt(
                "webtransport4j.server.max_concurrent_sessions", Integer.MAX_VALUE);
        if (globalMaxSessions <= 0) {
          globalMaxSessions = Integer.MAX_VALUE;
        }
        if (globalSlots != null && !reserveGlobalSlot(globalSlots, globalMaxSessions)) {
          mgr.releaseReservation();
          if (metricsListener != null) {
            metricsListener.onSessionRejected("global_limit_reached");
          }
          logger.warn(
              "❌ Rejecting connection: GLOBAL Max simultaneous sessions reached ({})",
              globalMaxSessions);
          Http3Headers responseHeaders = new DefaultHttp3Headers();
          responseHeaders.status(HttpResponseStatus.TOO_MANY_REQUESTS.codeAsText());
          ChannelFuture f = ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
          if (f != null) {
            f.addListener(ChannelFutureListener.CLOSE);
          }
          return;
        }

        Attribute<OverloadProtectionPolicy> policyAttr =
            quic.attr(WebTransportAttributeKeys.OVERLOAD_POLICY);
        OverloadProtectionPolicy overloadPolicy = policyAttr != null ? policyAttr.get() : null;
        if (overloadPolicy != null) {
          // The policy receives other active/pending sessions, excluding this reservation.
          int activeSessions = globalSlots != null ? Math.max(0, globalSlots.get() - 1) : 0;
          OverloadProtectionPolicy.AdmissionResult decision = overloadPolicy.tryAcquire(activeSessions);
          if (!decision.isAdmitted()) {
            mgr.releaseReservation();
            if (globalSlots != null) {
              globalSlots.decrementAndGet();
            }
            if (metricsListener != null) {
              metricsListener.onSessionRejected(
                  decision.getReason() != null ? decision.getReason() : "overload_shed");
            }
            logger.warn("⚠️ Rejecting session: Overload policy shed load ({})", decision.getReason());
            Http3Headers responseHeaders = new DefaultHttp3Headers();
            responseHeaders.status(HttpResponseStatus.SERVICE_UNAVAILABLE.codeAsText());
            if (decision.getRetryAfterSeconds() > 0) {
              responseHeaders.set("retry-after", String.valueOf(decision.getRetryAfterSeconds()));
            }
            ChannelFuture f = ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders));
            if (f != null) {
              f.addListener(ChannelFutureListener.CLOSE);
            }
            return;
          }
        }

        String pathStr = path.toString();
        AtomicBoolean pending = new AtomicBoolean(true);
        connectStream
            .closeFuture()
            .addListener(
                f -> {
                  if (overloadPolicy != null) {
                    overloadPolicy.release();
                  }
                  if (pending.compareAndSet(true, false)) {
                    mgr.releaseReservation();
                    if (globalSlots != null) {
                      globalSlots.decrementAndGet();
                    }
                  } else {
                    mgr.unregister(connectStream);
                  }
                });
        if (quic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY) != null) {
          quic.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set(pathStr);
        }
        if (connectStream.attr(WebTransportAttributeKeys.SESSION_PATH_KEY) != null) {
          connectStream.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set(pathStr);
        }

        String cipherSuite = "TLS_AES_128_GCM_SHA256";
        String tlsVersion = "TLSv1.3";
        try {
          SSLEngine sslEngine = quic.sslEngine();
          if (sslEngine != null && sslEngine.getSession() != null) {
            String c = sslEngine.getSession().getCipherSuite();
            if (c != null && !c.isEmpty() && !"SSL_NULL_WITH_NULL_NULL".equals(c)) {
              cipherSuite = c;
            }
            String p = sslEngine.getSession().getProtocol();
            if (p != null && !p.isEmpty()) {
              tlsVersion = p;
            }
          }
        } catch (Exception ignored) {
          // Ignored.
        }

        if (logger.isDebugEnabled()) {
          logger.debug(
              "⚡ [WebTransport Session Established] Peer: {} | Path: {} | TLS: {} | Negotiated"
                  + " Cipher: {}",
              quic.remoteSocketAddress(),
              pathStr,
              tlsVersion,
              cipherSuite);
        }
        CharSequence availableProtocolsHeader = frame.headers().get(HEADER_WT_AVAILABLE_PROTOCOLS);
        String selectedProtocol = null;
        if (availableProtocolsHeader != null) {
          List<String> availableProtocols =
              WebTransportUtils.parseAvailableProtocols(availableProtocolsHeader);
          WebTransportServer server = quic.attr(WebTransportAttributeKeys.SERVER_KEY).get();
          WebTransportHandler handler = (server != null) ? server.getHandler(pathStr) : null;
          if (handler != null && !availableProtocols.isEmpty()) {
            selectedProtocol = handler.selectSubprotocol(availableProtocols);
          }
        }
        if (selectedProtocol != null) {
          connectStream.attr(WebTransportAttributeKeys.SELECTED_SUBPROTOCOL).set(selectedProtocol);
        }

        Http3Headers responseHeaders = new DefaultHttp3Headers();
        responseHeaders.status(HttpResponseStatus.OK.codeAsText());
        if (selectedProtocol != null) {
          responseHeaders.add(
              HEADER_WT_PROTOCOL, WebTransportUtils.formatProtocolHeader(selectedProtocol));
        }

        ctx.writeAndFlush(new DefaultHttp3HeadersFrame(responseHeaders))
            .addListener(
                f -> {
                  if (f.isSuccess() && pending.compareAndSet(true, false)) {
                    mgr.registerReserved(connectStream);
                  } else if (!f.isSuccess() && pending.compareAndSet(true, false)) {
                    mgr.releaseReservation();
                    if (globalSlots != null) {
                      globalSlots.decrementAndGet();
                    }
                    connectStream.close();
                  }
                });
        if (logger.isDebugEnabled()) {
          logger.debug("🌊 Stream 0 AutoRead: {}", ctx.channel().config().isAutoRead());
        }
        if (logger.isDebugEnabled()) {
          logger.debug("🌊 Stream 0 Pipeline post-handshake: {}", ctx.pipeline().names());
        }
      }
      return;
    }
  }

  @Override
  protected void channelRead(@NonNull ChannelHandlerContext ctx, @NonNull Http3DataFrame frame) {
    ctx.fireChannelRead(frame);
  }

  private static boolean reserveGlobalSlot(AtomicInteger slots, int limit) {
    for (; ; ) {
      int current = slots.get();
      if (current >= limit || current == Integer.MAX_VALUE) {
        return false;
      }
      if (slots.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  private boolean isAllowed(
      @Nullable List<String> allowedOrigins,
      @Nullable OriginValidator originValidator,
      @Nullable Boolean strictOrigin,
      @Nullable CharSequence origin,
      @Nullable CharSequence authority) {
    String originStr = origin != null ? origin.toString() : null;
    String authorityStr = authority != null ? authority.toString() : null;

    if (strictOrigin != null && strictOrigin && (originStr == null || originStr.trim().isEmpty())) {
      logger.warn("Strict origin validation failed: Origin header is missing or empty");
      return false;
    }

    if (originValidator != null) {
      return originValidator.validate(originStr, authorityStr);
    }

    if (allowedOrigins == null || allowedOrigins.isEmpty() || allowedOrigins.contains("*")) {
      return true;
    }

    if (originStr != null) {
      if (allowedOrigins.contains(originStr)) {
        return true;
      }
      String originHost = extractHost(originStr);
      return originHost != null && matchesOriginList(allowedOrigins, originHost);
    }

    if (authorityStr != null) {
      if (allowedOrigins.contains(authorityStr)) {
        return true;
      }
      String authorityHost = extractHost(authorityStr);
      return authorityHost != null && matchesOriginList(allowedOrigins, authorityHost);
    }

    return false;
  }

  private boolean matchesOriginList(@NonNull List<String> allowedOrigins, @NonNull String host) {
    if (allowedOrigins.contains(host)) {
      return true;
    }
    for (String allowed : allowedOrigins) {
      if (allowed != null && allowed.startsWith("*.")) {
        String baseDomain = allowed.substring(2);
        if (host.equalsIgnoreCase(baseDomain)
            || host.toLowerCase(Locale.ROOT).endsWith("." + baseDomain.toLowerCase(Locale.ROOT))) {
          return true;
        }
      }
    }
    return false;
  }

  private @Nullable String extractHost(@Nullable String value) {
    if (value == null) {
      return null;
    }
    try {
      String uriStr = value.trim();
      if (!uriStr.contains("://")) {
        uriStr = "https://" + uriStr;
      }
      URI uri = new URI(uriStr);
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
  protected void channelInputClosed(@NonNull ChannelHandlerContext ctx) {
    if (logger.isDebugEnabled()) {
      logger.debug("🔒 Stream Closed: {}", ctx.channel().id());
    }
    ctx.close();
  }
}
