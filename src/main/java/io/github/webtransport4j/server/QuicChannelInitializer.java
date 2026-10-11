package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicPathEvent;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes QUIC channels with the WebTransport pipeline.
 *
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:32 pm
 */
public class QuicChannelInitializer extends ChannelInitializer<QuicChannel> {

  private static final Logger logger = LoggerFactory.getLogger(QuicChannelInitializer.class);

  private final Http3Settings settings;

  private final WebTransportServer server;

  private final List<String> allowedOrigins;

  private final AtomicInteger globalActiveSessions;
  private final AtomicInteger globalSessionSlots;

  /** Quic Channel Initializer. */
  public QuicChannelInitializer(
      WebTransportServer server,
      Http3Settings settings,
      List<String> allowedOrigins,
      AtomicInteger globalActiveSessions,
      AtomicInteger globalSessionSlots) {
    this.server = server;
    this.settings = settings;
    this.allowedOrigins = allowedOrigins;
    this.globalActiveSessions = globalActiveSessions;
    this.globalSessionSlots = globalSessionSlots;
  }

  @Override
  protected void initChannel(@NonNull QuicChannel ch) {
    if (logger.isDebugEnabled()) {
      logger.debug("Opening Quic connection : {}", ch.id());
    }
    Long defUni = settings.get(0x2b64L) == null ? Long.valueOf(0L) : settings.get(0x2b64L);
    Long defBidi = settings.get(0x2b65L) == null ? Long.valueOf(0L) : settings.get(0x2b65L);
    Long defData = settings.get(0x2b61L) == null ? Long.valueOf(0L) : settings.get(0x2b61L);
    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(defUni);
    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(defBidi);
    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(defData);
    ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
    ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(false);

    long connWriteLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.connection.write.limit", 0L);
    long connReadLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.connection.read.limit", 0L);
    if (connWriteLimit > 0 || connReadLimit > 0) {
      GlobalTrafficShapingHandler connShaper =
          new GlobalTrafficShapingHandler(ch.eventLoop(), connWriteLimit, connReadLimit);
      ch.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER).set(connShaper);
      ch.closeFuture().addListener(f -> connShaper.release());
    }

    ch.pipeline().addFirst(IpRateLimitingHandler.INSTANCE);
    if (logger.isDebugEnabled() || logger.isTraceEnabled()) {
      ch.pipeline().addFirst(QuicGlobalSniffer.GLOBAL);
    }

    // Intercept connection migration events to fire metrics and notify handler
    ch.pipeline().addLast(createMigrationHandler(ch));

    InetSocketAddress remote = (InetSocketAddress) ch.remoteSocketAddress();
    if (remote == null || remote.getAddress() == null) {
      logger.debug("⚠️ Remote socket address is null during channel initialization");
      ch.close();
      return;
    }
    String ip = remote.getAddress().getHostAddress();
    int port = remote.getPort();
    String nettyId = ch.id().asShortText();
    if (logger.isDebugEnabled()) {
      logger.debug("\n🔌 NEW QUIC CONNECTION ESTABLISHED");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("    ├── 🌍 Remote IP:   {}", ip);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("    ├── 🚪 Remote Port: {}", port);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("    └── 🆔 Channel ID:  {}", nettyId);
    }
    ch.attr(WebTransportAttributeKeys.SERVER_KEY).set(this.server);
    this.server.registerConnection(ch);
    ch.attr(WebTransportAttributeKeys.GLOBAL_SESSION_COUNT).set(this.globalActiveSessions);
    ch.attr(WebTransportAttributeKeys.GLOBAL_SESSION_SLOTS).set(this.globalSessionSlots);
    ch.attr(WebTransportAttributeKeys.OVERLOAD_POLICY).set(this.server.getOverloadProtectionPolicy());
    WebTransportSessionManager sessionManager = new WebTransportSessionManager();
    ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(sessionManager);
    this.server.registerQuicChannel(ch);
    ch.closeFuture()
        .addListener(
            f -> {
              this.server.unregisterQuicChannel(ch);
              sessionManager.closeAll(ch);
            });
    ch.attr(WebTransportAttributeKeys.MESSAGE_DISPATCHER_SUPPLIER)
        .set(this.server.getMessageDispatcherSupplier());
    ch.attr(WebTransportAttributeKeys.METRICS_LISTENER).set(this.server.getMetricsListener());
    ch.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).set(allowedOrigins);
    ch.attr(WebTransportAttributeKeys.ORIGIN_VALIDATOR).set(this.server.getOriginValidator());
    ch.attr(WebTransportAttributeKeys.STRICT_ORIGIN_VALIDATION).set(this.server.isStrictOriginValidation());
    ch.pipeline().addLast(this.server.getMessageDispatcherSupplier().get());
    if (logger.isDebugEnabled()) {
      logger.debug("🔧 Added MessageDispatcher. Pipeline now: {}", ch.pipeline().names());
    }
    ch.pipeline()
        .addLast(
            new Http3ServerConnectionHandler(
                new WebTransportStreamChannelInitializer(),
                new Http3InboundControlStreamHandler(),
                new UnknownStreamHandlerFactory(),
                new DefaultHttp3SettingsFrame(settings),
                WebTransportConfig.getBoolean(
                    "webtransport4j.http3.qpack.dynamic.table.disabled", true),
                (id, value) -> true));
  }

  /**
   * Creates an inbound channel handler that intercepts QUIC path events to fire metrics and notify
   * registered handlers of connection migration.
   *
   * @param ch the QUIC channel
   * @return migration channel handler
   */
  public static ChannelInboundHandlerAdapter createMigrationHandler(@NonNull QuicChannel ch) {
    return new ChannelInboundHandlerAdapter() {
      private SocketAddress currentRemoteSocketAddress = ch.remoteSocketAddress();
      private String currentRemoteAddress =
          ch.remoteSocketAddress() != null
              ? Objects.requireNonNull(ch.remoteSocketAddress()).toString()
              : "unknown";

      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof QuicPathEvent.PeerMigrated) {
          QuicPathEvent.PeerMigrated event = (QuicPathEvent.PeerMigrated) evt;
          SocketAddress newSocketAddress = event.remote();
          String newRemoteAddress =
              newSocketAddress != null ? newSocketAddress.toString() : "unknown";
          SocketAddress oldSocketAddress = currentRemoteSocketAddress;

          WebTransportMetricsListener metrics =
              ctx.channel().attr(WebTransportAttributeKeys.METRICS_LISTENER).get();
          WebTransportSessionManager mgr =
              ctx.channel().attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
          WebTransportServer server =
              ctx.channel().attr(WebTransportAttributeKeys.SERVER_KEY).get();

          if (mgr != null) {
            for (WebTransportSession session : mgr.getSessions()) {
              if (metrics != null) {
                metrics.onConnectionMigration(
                    session.getSessionStreamId(), currentRemoteAddress, newRemoteAddress);
              }
              if (server != null && oldSocketAddress != null && newSocketAddress != null) {
                WebTransportHandler handler = server.getHandler(session.path());
                if (handler != null) {
                  try {
                    handler.onConnectionMigration(session, oldSocketAddress, newSocketAddress);
                  } catch (Exception e) {
                    logger.error("Error in handler onConnectionMigration callback", e);
                  }
                }
              }
            }
          }
          currentRemoteSocketAddress = newSocketAddress;
          currentRemoteAddress = newRemoteAddress;
        }
        if (evt instanceof SslHandshakeCompletionEvent) {
          SslHandshakeCompletionEvent event = (SslHandshakeCompletionEvent) evt;
          if (event.isSuccess()) {
            logger.info("Handshake successful");
          } else {
            logger.warn("Handshake failed", event.cause());
          }
        }
        super.userEventTriggered(ctx, evt);
      }
    };
  }
}
