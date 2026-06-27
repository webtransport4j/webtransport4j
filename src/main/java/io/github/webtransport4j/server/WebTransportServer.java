package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;


import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.SslSessionTicketKey;
import io.netty.handler.codec.quic.QuicTokenHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebTransportServer {
  private static final Logger logger = LoggerFactory.getLogger(WebTransportServer.class);
  private int port;
  
  private final Map<String, WebTransportHandler> handlers = new ConcurrentHashMap<>();
  private final WebTransportHandler defaultHandler;

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
    if (!normalized.startsWith("/")) {
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

    if (logger.isDebugEnabled()) {
        logger.debug("🚀 STARTING DEBUG SERVER...");
    }
    this.group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    long globalWriteLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.global.write.limit", 0L);
    long globalReadLimit =
        WebTransportConfig.getLong("webtransport4j.server.traffic.global.read.limit", 0L);
    if (globalWriteLimit > 0 || globalReadLimit > 0) {
      globalTrafficShaper =
          new GlobalTrafficShapingHandler(group, globalWriteLimit, globalReadLimit);
      channel.attr(WebTransportAttributeKeys.GLOBAL_TRAFFIC_SHAPER).set(globalTrafficShaper);
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
        if (sslContext.sessionContext() != null) {
          sslContext.sessionContext().setTicketKeys(ticketKeys);
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
    logger.info("Server side settings : {}", settings);
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
            .handler(new QuicChannelInitializer(this, settings, businessExecutor, allowedOrigins))
            .build();
    this.channel =
        new Bootstrap()
            .group(group)
            .channel(NioDatagramChannel.class)
            .handler(serverCodec)
            .bind(new InetSocketAddress(port))
            .sync()
            .channel();
    if (logger.isDebugEnabled()) {
        logger.debug("✅ WebTransport server listening on {}", port);
    }
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
          logger.info("🔑 QUIC Token Handler configured: HMAC (HmacQuicTokenHandler) with custom configured key, expiration: {}ms", expirationMs);
          return new HmacQuicTokenHandler(key, expirationMs);
        } else {
          logger.warn("⚠️ Configured HMAC key is too short (must be at least 16 bytes / 32 hex characters). Falling back to random key.");
        }
      }
        logger.info("🔑 QUIC Token Handler configured: HMAC (HmacQuicTokenHandler) with randomly generated key, expiration: {}ms", expirationMs);
      return new HmacQuicTokenHandler(expirationMs);
    } else {
      try {
        logger.info("🔑 QUIC Token Handler configured: Custom Class ({})", tokenHandlerType);
        return (QuicTokenHandler) Class.forName(tokenHandlerType)
            .getDeclaredConstructor()
            .newInstance();
      } catch (Exception e) {
        logger.error("❌ Failed to load custom QuicTokenHandler: {}. Falling back to HmacQuicTokenHandler.", tokenHandlerType, e);
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
      logger.warn("⚠️ HMAC key hex string length is not even: {}. Falling back to plain string bytes.", normalized);
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
      logger.warn("⚠️ Failed to parse HMAC key as hex, falling back to plain string bytes: {}", e.getMessage());
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


}
