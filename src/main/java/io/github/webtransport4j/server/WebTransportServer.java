package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;


import static io.github.webtransport4j.server.WebTransportUtils.readVariableLengthInt;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.SslSessionTicketKey;
import io.netty.handler.codec.quic.QuicSslSessionContext;
import io.netty.handler.codec.quic.QuicTokenHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class WebTransportServer {
  private static final Logger logger = Logger.getLogger(WebTransportServer.class.getName());
  private int port;
  
  private final Map<String, WebTransportHandler> handlers = new ConcurrentHashMap<>();
  private WebTransportHandler defaultHandler;

  public WebTransportServer(WebTransportHandler defaultHandler) {
    if (defaultHandler == null) {
      throw new IllegalArgumentException("defaultHandler cannot be null");
    }
    this.defaultHandler = defaultHandler;
    this.businessExecutor = BusinessExecutorFactory.create();
  }

  public WebTransportServer(){
    this.defaultHandler = new WebTransportHandler(){};
    this.businessExecutor = BusinessExecutorFactory.create();
  }

  public WebTransportServer(WebTransportHandler defaultHandler, ExecutorService businessExecutor) {
    if (defaultHandler == null) {
      throw new IllegalArgumentException("defaultHandler cannot be null");
    }
    this.defaultHandler = defaultHandler;
    this.businessExecutor = businessExecutor;
  }

  private static String normalizePath(String path) {
    if (path == null) {
      return null;
    }
    String trimmed = path.trim();
    if (trimmed.length() > 1 && trimmed.endsWith("/")) {
      return trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  public void registerHandler(String path, WebTransportHandler handler) {
    if (path == null) throw new IllegalArgumentException("path cannot be null");
    String normalized = normalizePath(path);
    if (normalized.isEmpty() || !normalized.startsWith("/")) {
      throw new IllegalArgumentException("path must not be empty and must start with '/'");
    }
    if (handler == null) {
      handlers.remove(normalized);
    } else {
      handlers.put(normalized, handler);
    }
  }

  public WebTransportHandler getHandler(String path) {
    if (path == null) return defaultHandler;
    String normalized = normalizePath(path);
    WebTransportHandler handler = handlers.get(normalized);
    return (handler != null) ? handler : this.defaultHandler;
  }

  public int getPort() {
    return port;
  }



  public static GlobalTrafficShapingHandler globalTrafficShaper;

  
  private final ExecutorService businessExecutor;

  private List<String> allowedOrigins;

  private EventLoopGroup group;
  private Channel channel;



  public void start() throws Exception {
    if (defaultHandler == null) {
      throw new IllegalStateException("Server cannot start without a registered default path handler.");
    }
    port = WebTransportConfig.getInt("webtransport4j.server.port", 4433);
    String originsProp = WebTransportConfig.get("webtransport4j.allowed.origins", "*");
    allowedOrigins = Arrays.asList(originsProp.split(","));

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Shutdown hook triggered. Stopping server...");
                  stop();
                }));

    logger.debug("🚀 STARTING DEBUG SERVER...");
    this.group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    long globalWriteLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.global.write.limit", 0L);
    long globalReadLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.global.read.limit", 0L);
    if (globalWriteLimit > 0 || globalReadLimit > 0) {
      globalTrafficShaper =
          new GlobalTrafficShapingHandler(group, globalWriteLimit, globalReadLimit);
    }
    String keyPath = WebTransportConfig.get("webtransport4j.ssl.key.path", null);
    String certPath = WebTransportConfig.get("webtransport4j.ssl.cert.path", null);

    if (keyPath == null && certPath == null) {
      File keyFile = new File("localhost-key.pem");
      File certFile = new File("localhost.pem");
      if (keyFile.exists() && certFile.exists()) {
        keyPath = keyFile.getAbsolutePath();
        certPath = certFile.getAbsolutePath();
      }
    }



    long sessionTimeout = WebTransportConfig.getLong("webtransport4j.ssl.session.timeout.seconds", -1L);
    long sessionCacheSize = WebTransportConfig.getLong("webtransport4j.ssl.session.cache.size", -1L);

    QuicSslContext sslContext;
    if (keyPath != null && certPath != null) {
      QuicSslContextBuilder builder = QuicSslContextBuilder.forServer(new File(keyPath), null, new File(certPath))
              .applicationProtocols(Http3.supportedApplicationProtocols());
      if (sessionTimeout > 0) {
        builder.sessionTimeout(sessionTimeout);
      }
      if (sessionCacheSize > 0) {
        builder.sessionCacheSize(sessionCacheSize);
      }
      sslContext = builder.build();
    } else {
      throw new IllegalStateException("SSL key path and certificate path must be configured. "
          + "Set webtransport4j.ssl.key.path and webtransport4j.ssl.cert.path in configuration.");
    }

    String ticketKeysStr = WebTransportConfig.get("webtransport4j.ssl.session.ticket.keys", null);
    if (ticketKeysStr != null && !ticketKeysStr.trim().isEmpty()) {
      try {
        String[] keysList = ticketKeysStr.split(",");
        SslSessionTicketKey[] ticketKeys = new SslSessionTicketKey[keysList.length];
        for (int i = 0; i < keysList.length; i++) {
          String hex = keysList[i].trim();
          if (hex.length() != 96) {
            throw new IllegalArgumentException("Session ticket key must be exactly 96 hex characters (16 byte name + 16 byte HMAC + 16 byte AES)");
          }
          byte[] keyBytes = ByteBufUtil.decodeHexDump(hex);
          byte[] name = new byte[16];
          byte[] hmacKey = new byte[16];
          byte[] aesKey = new byte[16];
          System.arraycopy(keyBytes, 0, name, 0, 16);
          System.arraycopy(keyBytes, 16, hmacKey, 0, 16);
          System.arraycopy(keyBytes, 32, aesKey, 0, 16);
          ticketKeys[i] = new SslSessionTicketKey(name, hmacKey, aesKey);
        }
        if (sslContext.sessionContext() instanceof QuicSslSessionContext) {
          ((QuicSslSessionContext) sslContext.sessionContext()).setTicketKeys(ticketKeys);
          logger.info("🔑 Explicit TLS Session Ticket Keys loaded. 1-RTT Session Resumption across servers is fully supported.");
        }
      } catch (Exception e) {
        logger.error("❌ Failed to parse webtransport4j.ssl.session.ticket.keys", e);
      }
    }
    String allowedProp =
        WebTransportConfig.get(
            "webtransport4j.webtransport.settings.nonstandardallowed",
            "0x2c7cf000,0x2b64,0x2b65,0x2b61");
    Set<Long> allowed = new HashSet<>();
    for (String val : allowedProp.split(",")) {
      allowed.add(Long.decode(val.trim()));
    }

    Http3Settings settings = new Http3Settings((id, value) -> allowed.contains(id));

    long wtMaxStreamsUni =
        WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.streams.uni", 0L);
    long wtMaxStreamsBidi =
        WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.streams.bidi", 0L);
    long wtInitialMaxData =
        WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.data", 0L);

    long quicMaxStreamsUni = WebTransportConfig.getLong("webtransport4j.quic.max.streams.uni", 0L);
    long quicMaxStreamsBidi =
        WebTransportConfig.getLong("webtransport4j.quic.max.streams.bidi", 0L);
    long quicInitialMaxData =
        WebTransportConfig.getLong("webtransport4j.quic.initial.max.data", 0L);

    // Validate that QUIC limits are not lesser than WebTransport initial session
    // limits
    validateConfig(
        quicMaxStreamsBidi,
        wtMaxStreamsBidi,
        quicMaxStreamsUni,
        wtMaxStreamsUni,
        quicInitialMaxData,
        wtInitialMaxData);

    settings.enableH3Datagram(
        WebTransportConfig.getBoolean(
            "webtransport4j.webtransport.settings.enable_h3_datagram", false));
    settings.enableConnectProtocol(
        WebTransportConfig.getBoolean(
            "webtransport4j.webtransport.settings.enable_connect_protocol", false));

    // SETTINGS_WT_ENABLED (0x2c7cf000) - draft-15
    settings.put(
        0x2c7cf000L,
        WebTransportConfig.getLong("webtransport4j.webtransport.settings.wt_enabled.value", 0L));
    // SETTINGS_WT_INITIAL_MAX_STREAMS_UNI (0x2b64) - draft-15
    settings.put(0x2b64L, wtMaxStreamsUni);
    // SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (0x2b65) - draft-15
    settings.put(0x2b65L, wtMaxStreamsBidi);
    // SETTINGS_WT_INITIAL_MAX_DATA (0x2b61) - draft-15
    settings.put(0x2b61L, wtInitialMaxData);
    logger.info("Server side settings : " + settings);
    ChannelHandler serverCodec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(
                WebTransportConfig.getInt("webtransport4j.quic.idle.timeout.seconds", 0),
                TimeUnit.SECONDS)
            .initialMaxData(quicInitialMaxData)
            .initialMaxStreamDataBidirectionalLocal(
                WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.local", 0L))
            .initialMaxStreamDataBidirectionalRemote(
                WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.remote", 0L))
            .initialMaxStreamsBidirectional(quicMaxStreamsBidi)
            .datagram(
                WebTransportConfig.getInt("webtransport4j.quic.datagram.recv.queue.len", 0),
                WebTransportConfig.getInt("webtransport4j.quic.datagram.send.queue.len", 0))
            .initialMaxStreamsUnidirectional(quicMaxStreamsUni)
            .initialMaxStreamDataUnidirectional(
                WebTransportConfig.getLong("webtransport4j.quic.stream.data.uni", 0L))
            .tokenHandler(getTokenHandler())
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    logger.debug("Opening quic connection " + ch.id());
                    long defUni = settings.get(0x2b64L) == null ? 0L : settings.get(0x2b64L);
                    long defBidi = settings.get(0x2b65L) == null ? 0L : settings.get(0x2b65L);
                    long defData = settings.get(0x2b61L) == null ? 0L : settings.get(0x2b61L);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(defUni);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(defBidi);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(defData);
                    ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
                    ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(false);
                    if (businessExecutor != null) {
                      ch.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).set(businessExecutor);
                    }

                    long connWriteLimit =
                        WebTransportConfig.getLong(
                            "webtransport4j.server.traffic.connection.write.limit", 0L);
                    long connReadLimit =
                        WebTransportConfig.getLong(
                            "webtransport4j.server.traffic.connection.read.limit", 0L);
                    if (connWriteLimit > 0 || connReadLimit > 0) {
                      GlobalTrafficShapingHandler connShaper =
                          new GlobalTrafficShapingHandler(
                              ch.eventLoop(), connWriteLimit, connReadLimit);
                      ch.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER).set(connShaper);
                      ch.closeFuture().addListener(f -> connShaper.release());
                    }

                    ch.pipeline().addFirst(new QuicGlobalSniffer("GLOBAL-CONN"));
                    
                    InetSocketAddress remote = (InetSocketAddress) ch.remoteSocketAddress();
                    String ip = remote.getAddress().getHostAddress();
                    int port = remote.getPort();
                    String nettyId = ch.id().asShortText();
                    // 2. PRINT NICE LOG
                    logger.debug("\n🔌 NEW QUIC CONNECTION ESTABLISHED");
                    logger.debug("    ├── 🌍 Remote IP:   " + ip);
                    logger.debug("    ├── 🚪 Remote Port: " + port);
                    logger.debug("    └── 🆔 Channel ID:  " + nettyId);
                    ch.attr(WebTransportAttributeKeys.SERVER_KEY).set(WebTransportServer.this);
                    ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR)
                        .set(new WebTransportSessionManager());
                    ch.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).set(businessExecutor);
                    ch.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).set(allowedOrigins);
                    ch.pipeline().addLast(new WebTransportDatagramHandler());
                    logger.debug(
                        "🔧 Added WebTransportDatagramHandler. Pipeline now: "
                            + ch.pipeline().names());
                    ch.pipeline().addLast(new WebTransportCapsuleHandler());
                    logger.debug(
                        "🔧 Added WebTransportCapsuleHandler. Pipeline now: "
                            + ch.pipeline().names());
                    ch.pipeline().addLast(new MessageDispatcher());
                    logger.debug(
                        "🔧 Added MessageDispatcher. Pipeline now: " + ch.pipeline().names());
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {
                                  
                                    addTrafficShapers(stream);

                                    stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                    logger.debug(
                                        "🔧 Added WebTransportDetectorHandler. Pipeline now: "
                                            + stream.pipeline().names());
                                    stream
                                        .pipeline()
                                        .addFirst(
                                            new QuicGlobalSniffer("STREAM-" + stream.streamId()));
                                    logger.debug(
                                        "🔧 Added QuicGlobalSniffer (per‑stream). Pipeline now: "
                                            + stream.pipeline().names());
                                    stream.pipeline().addLast(new RawWebTransportHandler());
                                    logger.debug(
                                        "🔧 Added RawWebTransportHandler. Pipeline now: "
                                            + stream.pipeline().names());
                                    stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                    logger.debug(
                                        "🔧 Added WebTransportStreamFrameDecoder. Pipeline now: "
                                            + stream.pipeline().names());
                                    stream.pipeline().addLast(new WebTransportHeadersHandler());
                                    logger.debug(
                                        "🔧 Added WebTransportHeadersHandler. Pipeline now: "
                                            + stream.pipeline().names());
                                    stream.pipeline().addLast(new WebTransportDataHandler());
                                    logger.debug(
                                        "🔧 Added WebTransportDataHandler. Pipeline now: "
                                            + stream.pipeline().names());
                                    stream.pipeline().addLast(new WebTransportCapsuleHandler());
                                    logger.debug(
                                        "🔧 Added WebTransportCapsuleHandler. Pipeline now: "
                                            + stream.pipeline().names());
                                    stream.pipeline().addLast(new MessageDispatcher());
                                    logger.debug(
                                        "🔧 Added MessageDispatcher. Pipeline now: "
                                            + stream.pipeline().names());
                                    // DEBUG: Catch-all exception handler
                                    stream
                                        .pipeline()
                                        .addLast(
                                            new ChannelInboundHandlerAdapter() {
                                              @Override
                                              public void exceptionCaught(
                                                  ChannelHandlerContext ctx, Throwable cause) {
                                                System.err.println(
                                                    "❌ PIPELINE ERROR: " + cause.getMessage());
                                                cause.printStackTrace();
                                              }
                                            });
                                  }
                                },
                                new ChannelInboundHandlerAdapter() {
                                  @Override
                                  public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    if (!(msg instanceof Http3SettingsFrame)) {
                                      ctx.fireChannelRead(msg);
                                      return;
                                    }
                                    if (msg instanceof Http3SettingsFrame) {
                                      Http3SettingsFrame settingsFrame = (Http3SettingsFrame) msg;
                                      logger.debug("PEER SETTINGS: " + settingsFrame);
                                      io.netty.handler.codec.http3.Http3Settings settings =
                                          settingsFrame.settings();
                                      if (settings != null) {
                                        QuicChannel quic = null;
                                        if (ctx.channel() instanceof QuicStreamChannel) {
                                          quic = ((QuicStreamChannel) ctx.channel()).parent();
                                        } else if (ctx.channel() instanceof QuicChannel) {
                                          quic = (QuicChannel) ctx.channel();
                                        }

                                        boolean valid = settings.h3DatagramEnabled();
                                        if (quic != null) {
                                          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
                                          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(valid);
                                        }

                                        // Section 5.1: Verify required setting SETTINGS_H3_DATAGRAM
                                        // (0x33) is
                                        // enabled (1)
                                        // NOTE: Do NOT close the connection immediately here.
                                        // Per RFC, CONNECT requests can arrive before or after
                                        // SETTINGS
                                        // (out of order on different streams). If we close
                                        // immediately,
                                        // a late-arriving CONNECT never gets a proper
                                        // H3_MESSAGE_ERROR
                                        // reset. Instead, we mark the connection invalid via
                                        // attributes
                                        // and let WebTransportHeadersHandler reject CONNECT
                                        // requests.
                                        if (!valid) {
                                          logger.warn(
                                              "❌ WebTransport requirements not met: Client does not"
                                                  + " support H3 Datagrams. Treating all"
                                                  + " established sessions as malformed.");
                                          if (quic != null) {
                                            WebTransportSessionManager mgr =
                                                quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR)
                                                    .get();
                                            if (mgr != null) {
                                              for (WebTransportSession session :
                                                  new ArrayList<>(mgr.getSessions())) {
                                                logger.warn(
                                                    "⚡️ Resetting established session ID "
                                                        + session.getSessionStreamId()
                                                        + " with H3_MESSAGE_ERROR");
                                                session
                                                    .getConnectStream()
                                                    .shutdown(
                                                        0x010e,
                                                        session.getConnectStream().newPromise());
                                              }
                                            }
                                          }
                                          ReferenceCountUtil.release(msg);
                                          return;
                                        }

                                        if (quic != null) {
                                          quic.attr(
                                                  WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI)
                                              .set(settings.get(0x2b64L));
                                          quic.attr(
                                                  WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI)
                                              .set(settings.get(0x2b65L));
                                          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)
                                              .set(settings.get(0x2b61L));
                                        }
                                      }
                                    }
                                    ReferenceCountUtil.release(msg);
                                  }
                                },
                                (streamType) -> {
                                  if (streamType == 0x54) {
                                    return new ChannelInitializer<QuicStreamChannel>() {
                                      @Override
                                      protected void initChannel(QuicStreamChannel ch) {
                                        addTrafficShapers(ch);
                                        ch.pipeline()
                                            .addLast(
                                                new ByteToMessageDecoder() {
                                                  private boolean sessionHeaderRead = false;

                                                  @Override
                                                  protected void decode(
                                                      ChannelHandlerContext ctx,
                                                      ByteBuf in,
                                                      List<Object> out) {
                                                    if (!sessionHeaderRead) {
                                                      in.markReaderIndex();
                                                      long sessionId = readVariableLengthInt(in);
                                                      if (sessionId == -1) {
                                                        in.resetReaderIndex();
                                                        return;
                                                      }
                                                      ctx.channel()
                                                          .attr(WebTransportAttributeKeys.SESSION_ID_KEY)
                                                          .set(sessionId);
                                                      sessionHeaderRead = true;
                                                    }

                                                    if (!in.isReadable()) {
                                                      return;
                                                    }

                                                    String savedPath =
                                                        ctx.channel()
                                                            .parent()
                                                            .attr(WebTransportAttributeKeys.SESSION_PATH_KEY)
                                                            .get();
                                                    ctx.channel()
                                                        .attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)
                                                        .set(streamType);
                                                    ctx.channel()
                                                        .attr(WebTransportAttributeKeys.SESSION_PATH_KEY)
                                                        .set(savedPath);
                                                    out.add(in.readRetainedSlice(in.readableBytes()));
                                                  }
                                                });
                                        ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                        ch.pipeline().addLast(new WebTransportCapsuleHandler());
                                        ch.pipeline().addLast(new MessageDispatcher());
                                      }
                                    };
                                  }
                                  return null;
                                },
                                new DefaultHttp3SettingsFrame(settings),
                                true));
                  }
                })
            .build();
    this.channel =
        new Bootstrap()
            .group(group)
            .channel(NioDatagramChannel.class)
            .handler(serverCodec)
            .bind(new InetSocketAddress(port))
            .sync()
            .channel();
    logger.debug("✅ WebTransport server listening on " + port);
    this.channel.closeFuture().sync();
  }

  public void stop() {
    logger.info("Stopping WebTransport server...");
    if (channel != null) {
      try {
        channel.close().sync();
      } catch (Exception e) {
        logger.error("Error closing server channel", e);
      } finally {
        channel = null;
      }
    }
    if (group != null) {
      try {
        group.shutdownGracefully().sync();
      } catch (Exception e) {
        logger.error("Error shutting down event loop group", e);
      } finally {
        group = null;
      }
    }
    if (globalTrafficShaper != null) {
      globalTrafficShaper.release();
      globalTrafficShaper = null;
    }
    if (businessExecutor != null) {
      businessExecutor.shutdown();
      try {
        if (!businessExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          businessExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        businessExecutor.shutdownNow();
      }
    }
    logger.info("WebTransport server stopped successfully.");
  }

  public static QuicTokenHandler getTokenHandler() {
    String tokenHandlerType = WebTransportConfig.get("webtransport4j.quic.token.handler", "hmac");
    if ("insecure".equalsIgnoreCase(tokenHandlerType)) {
      logger.info("🔑 QUIC Token Handler configured: INSECURE (InsecureQuicTokenHandler)");
      return InsecureQuicTokenHandler.INSTANCE;
    } else if ("hmac".equalsIgnoreCase(tokenHandlerType)) {
      long expirationMs = WebTransportConfig.getLong("webtransport4j.quic.token.handler.hmac.expiration.ms", 60000L);
      String keyHex = WebTransportConfig.get("webtransport4j.quic.token.handler.hmac.key", null);
      if (keyHex != null && !keyHex.trim().isEmpty()) {
        byte[] key = parseHex(keyHex);
        if (key != null && key.length >= 16) {
          logger.info("🔑 QUIC Token Handler configured: HMAC (HmacQuicTokenHandler) with custom configured key, expiration: " + expirationMs + "ms");
          return new HmacQuicTokenHandler(key, expirationMs);
        } else {
          logger.warn("⚠️ Configured HMAC key is too short (must be at least 16 bytes / 32 hex characters). Falling back to random key.");
        }
      }
      logger.info("🔑 QUIC Token Handler configured: HMAC (HmacQuicTokenHandler) with randomly generated key, expiration: " + expirationMs + "ms");
      return new HmacQuicTokenHandler(expirationMs);
    } else {
      try {
        logger.info("🔑 QUIC Token Handler configured: Custom Class (" + tokenHandlerType + ")");
        return (QuicTokenHandler) Class.forName(tokenHandlerType)
            .getDeclaredConstructor()
            .newInstance();
      } catch (Exception e) {
        logger.error("❌ Failed to load custom QuicTokenHandler: " + tokenHandlerType + ". Falling back to HmacQuicTokenHandler.", e);
        return new HmacQuicTokenHandler();
      }
    }
  }

  private static byte[] parseHex(String hex) {
    if (hex == null || hex.trim().isEmpty()) {
      return null;
    }
    String normalized = hex.trim();
    if (normalized.length() % 2 != 0) {
      logger.warn("⚠️ HMAC key hex string length is not even: " + normalized + ". Falling back to plain string bytes.");
      return normalized.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    try {
      byte[] data = new byte[normalized.length() / 2];
      for (int i = 0; i < normalized.length(); i += 2) {
        int high = Character.digit(normalized.charAt(i), 16);
        int low = Character.digit(normalized.charAt(i + 1), 16);
        if (high == -1 || low == -1) {
          throw new IllegalArgumentException("Non-hex character found");
        }
        data[i / 2] = (byte) ((high << 4) + low);
      }
      return data;
    } catch (Exception e) {
      logger.warn("⚠️ Failed to parse HMAC key as hex, falling back to plain string bytes: " + e.getMessage());
      return normalized.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
  }

  public static void validateConfig(
      long quicMaxBidi,
      long wtMaxBidi,
      long quicMaxUni,
      long wtMaxUni,
      long quicMaxData,
      long wtMaxData) {
    if (quicMaxBidi < wtMaxBidi) {
      throw new IllegalArgumentException(
          "Configuration Mismatch: quic.max.streams.bidi ("
              + quicMaxBidi
              + ") must be greater than or equal to webtransport.initial.max.streams.bidi ("
              + wtMaxBidi
              + ")");
    }
    if (quicMaxUni < wtMaxUni) {
      throw new IllegalArgumentException(
          "Configuration Mismatch: quic.max.streams.uni ("
              + quicMaxUni
              + ") must be greater than or equal to webtransport.initial.max.streams.uni ("
              + wtMaxUni
              + ")");
    }
    if (quicMaxData < wtMaxData) {
      throw new IllegalArgumentException(
          "Configuration Mismatch: quic.initial.max.data ("
              + quicMaxData
              + ") must be greater than or equal to webtransport.initial.max.data ("
              + wtMaxData
              + ")");
    }
  }

  public static void addTrafficShapers(QuicStreamChannel stream) {
    QuicChannel quic = stream.parent();
    if (globalTrafficShaper != null) {
      stream.pipeline().addFirst("global-traffic-shaper", globalTrafficShaper);
      logger.debug("🔧 Added global traffic shaper to stream " + stream.streamId());
    }
    GlobalTrafficShapingHandler connShaper = quic.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER).get();
    if (connShaper != null) {
      stream.pipeline().addFirst("conn-traffic-shaper", connShaper);
      logger.debug("🔧 Added connection traffic shaper to stream " + stream.streamId());
    }
    long streamWriteLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.stream.write.limit", 0L);
    long streamReadLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.stream.read.limit", 0L);
    if (streamWriteLimit > 0 || streamReadLimit > 0) {
      ChannelTrafficShapingHandler streamShaper =
          new ChannelTrafficShapingHandler(streamWriteLimit, streamReadLimit);
      stream.pipeline().addFirst("stream-traffic-shaper", streamShaper);
      logger.debug("🔧 Added stream traffic shaper to stream " + stream.streamId());
    }
  }
}
