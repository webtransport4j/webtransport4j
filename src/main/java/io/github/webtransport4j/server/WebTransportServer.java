package io.github.webtransport4j.server;

import io.github.webtransport4j.api.NoOpWebTransportMetricsListener;
import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.ReactiveWebTransportHandlerAdapter;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.EpollQuicUtils;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicCongestionControlAlgorithm;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicTokenHandler;
import io.netty.handler.codec.quic.SslSessionTicketKey;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main WebTransport server managing QUIC connections. */
public class WebTransportServer {

  static {
    // Disable PooledByteBufAllocator cache for non-FastThreadLocal threads (like
    // Virtual Threads)
    // to prevent severe direct memory leaks/exhaustion when executing async task
    // queues.
    System.setProperty("io.netty.allocator.useCacheForAllThreads", "false");
  }

  private static final Logger logger = LoggerFactory.getLogger(WebTransportServer.class);

  private Integer configuredPort;
  private String sslKeyPath;
  private String sslCertPath;
  private QuicSslContext sslContext;
  private List<String> allowedOrigins;
  private QuicTokenHandler quicTokenHandler;
  private String transportType;
  private Long idleTimeoutSeconds;
  private Long initialMaxStreamsBidi;
  private Long initialMaxStreamsUni;
  private Long initialMaxData;

  private final Map<String, WebTransportHandler> handlers = new ConcurrentHashMap<>();
  private WebTransportHandler defaultHandler;

  private final AtomicInteger globalActiveSessions = new AtomicInteger(0);

  /**
   * The observability metrics listener. Defaults to a no-op implementation.
   */
  private volatile WebTransportMetricsListener metricsListener = NoOpWebTransportMetricsListener.INSTANCE;

  private Supplier<MessageDispatcher> messageDispatcherSupplier = () -> DefaultMessageDispatcher.INSTANCE;
  private final ExecutorService businessExecutor;

  public static GlobalTrafficShapingHandler globalTrafficShaper;

  /** Lifecycle states of the WebTransport server. */
  public enum ServerState {
    STOPPED,
    STARTING,
    STARTED,
    STOPPING
  }

  private final AtomicReference<ServerState> state = new AtomicReference<>(ServerState.STOPPED);

  private EventLoopGroup group;
  private Channel channel;
  private Thread shutdownHook;
  private volatile QuicSslContext activeSslContext;
  private TlsCertificateWatcher tlsWatcher;

  public @Nullable QuicSslContext getActiveSslContext() {
    return activeSslContext;
  }

  public boolean checkAndReloadTlsCertificates() {
    if (tlsWatcher != null) {
      return tlsWatcher.checkAndReload();
    }
    return false;
  }

  public static @NonNull WebTransportServerBuilder builder() {
    return new WebTransportServerBuilder();
  }

  /** Web Transport Server. */
  public WebTransportServer(WebTransportHandler defaultHandler) {
    if (defaultHandler == null) {
      throw new IllegalArgumentException("defaultHandler cannot be null");
    }
    this.defaultHandler = defaultHandler;
    handlers.put("/", defaultHandler);
    this.businessExecutor = BusinessExecutorFactory.create();
  }

  public WebTransportServer() {
    this.defaultHandler = new WebTransportHandler() {
    };
    handlers.put("/", defaultHandler);
    this.businessExecutor = BusinessExecutorFactory.create();
  }

  /** Web Transport Server with custom business executor. */
  public WebTransportServer(WebTransportHandler defaultHandler, ExecutorService businessExecutor) {
    if (defaultHandler == null) {
      throw new IllegalArgumentException("defaultHandler cannot be null");
    }
    this.defaultHandler = defaultHandler;
    handlers.put("/", defaultHandler);
    this.businessExecutor = businessExecutor != null ? businessExecutor : BusinessExecutorFactory.create();
  }

  /**
   * Constructs a WebTransportServer using a {@link WebTransportServerBuilder}.
   */
  public WebTransportServer(@NonNull WebTransportServerBuilder builder) {
    this.configuredPort = builder.getPort();
    this.sslKeyPath = builder.getSslKeyPath();
    this.sslCertPath = builder.getSslCertPath();
    this.sslContext = builder.getSslContext();
    this.allowedOrigins = builder.getAllowedOrigins();
    this.quicTokenHandler = builder.getQuicTokenHandler();
    this.transportType = builder.getTransportType();
    this.idleTimeoutSeconds = builder.getIdleTimeoutSeconds();
    this.initialMaxStreamsBidi = builder.getInitialMaxStreamsBidi();
    this.initialMaxStreamsUni = builder.getInitialMaxStreamsUni();
    this.initialMaxData = builder.getInitialMaxData();

    if (builder.getMetricsListener() != null) {
      this.metricsListener = builder.getMetricsListener();
    }
    if (builder.getMessageDispatcherSupplier() != null) {
      this.messageDispatcherSupplier = builder.getMessageDispatcherSupplier();
    }
    this.businessExecutor = builder.getBusinessExecutor() != null
        ? builder.getBusinessExecutor()
        : BusinessExecutorFactory.create();

    this.defaultHandler = builder.getDefaultHandler() != null
        ? builder.getDefaultHandler()
        : new WebTransportHandler() {
        };

    this.handlers.put("/", this.defaultHandler);
    this.handlers.putAll(builder.getHandlers());
  }

  public Map<String, WebTransportHandler> getHandlers() {
    return handlers;
  }

  private static @Nullable String normalizePath(@Nullable String path) {
    if (path == null) {
      return null;
    }
    String trimmed = path.trim();
    if (trimmed.length() > 1 && trimmed.endsWith("/")) {
      return trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  /** Register Handler. */
  public void registerHandler(@NonNull String path, @Nullable WebTransportHandler handler) {
    String normalized = normalizePath(path);
    if (normalized == null || !normalized.startsWith("/")) {
      throw new IllegalArgumentException("path must not be null and empty and must start with '/'");
    }
    if (handler == null) {
      handlers.remove(normalized);
    } else {
      handlers.put(normalized, handler);
    }
  }

  /** Registers a reactive handler for a path. */
  public void registerReactiveHandler(@NonNull String path, @Nullable ReactiveWebTransportHandler reactiveHandler) {
    if (reactiveHandler == null) {
      registerHandler(path, (WebTransportHandler) null);
    } else {
      registerHandler(path, new ReactiveWebTransportHandlerAdapter(reactiveHandler));
    }
  }

  /** Sets a custom metrics listener for observability export. */
  public void setMetricsListener(@NonNull WebTransportMetricsListener listener) {
    this.metricsListener = listener;
  }

  public @NonNull WebTransportMetricsListener getMetricsListener() {
    return metricsListener;
  }

  public void setMessageDispatcher(@NonNull MessageDispatcher dispatcher) {
    this.messageDispatcherSupplier = () -> dispatcher;
  }

  public void setMessageDispatcherSupplier(@NonNull Supplier<MessageDispatcher> supplier) {
    this.messageDispatcherSupplier = supplier;
  }

  public @NonNull Supplier<MessageDispatcher> getMessageDispatcherSupplier() {
    return messageDispatcherSupplier;
  }

  /** Returns the handler for a path. */
  public @NonNull WebTransportHandler getHandler(@NonNull String path) {
    String normalized = normalizePath(path);
    WebTransportHandler handler = handlers.get(normalized);
    return (handler != null) ? handler : this.defaultHandler;
  }

  /** Returns the actual bound server port, or configured port if not started. */
  public int getPort() {
    if (channel != null && channel.isActive() && channel.localAddress() instanceof InetSocketAddress) {
      return ((InetSocketAddress) channel.localAddress()).getPort();
    }
    if (configuredPort != null) {
      return configuredPort;
    }
    return WebTransportConfig.getInt("webtransport4j.server.port", 4433);
  }

  public ExecutorService getBusinessExecutor() {
    return businessExecutor;
  }

  /** Returns the number of active WebTransport sessions across all QUIC connections. */
  public int getActiveSessionCount() {
    return globalActiveSessions.get();
  }

  /** Returns the current lifecycle state of the server. */
  public ServerState getState() {
    return state.get();
  }

  /** Returns true if the server is active and listening. */
  public boolean isStarted() {
    return state.get() == ServerState.STARTED && channel != null && channel.isActive();
  }

  /** Returns true if the server is active and listening. */
  public boolean isRunning() {
    return isStarted();
  }

  /**
   * Starts the WebTransport server non-blockingly. Returns immediately once the
   * server channel is bound.
   */
  public void start() throws Exception {
    if (!state.compareAndSet(ServerState.STOPPED, ServerState.STARTING)) {
      ServerState current = state.get();
      if (current == ServerState.STARTED || current == ServerState.STARTING) {
        logger.warn("⚠️ Server is already {} on port {}", current.name().toLowerCase(), getPort());
        return;
      }
      throw new IllegalStateException("Cannot start WebTransportServer while in state: " + current);
    }

    try {
      if (defaultHandler == null) {
        throw new IllegalStateException(
            "Server cannot start without a registered default path handler.");
      }
      int targetPort = configuredPort != null ? configuredPort
          : WebTransportConfig.getInt("webtransport4j.server.port", 4433);

      List<String> resolvedOrigins = this.allowedOrigins;
      if (resolvedOrigins == null) {
        String originsProp = WebTransportConfig.getNonNull("webtransport4j.allowed.origins", "*");
        resolvedOrigins = Arrays.asList(originsProp.split(","));
      }

      shutdownHook = new Thread(
          () -> {
            logger.info("Shutdown hook triggered. Stopping server...");
            stop();
          });
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      if (logger.isDebugEnabled()) {
        logger.debug("🚀 STARTING WEBTRANSPORT SERVER...");
      }

      Bootstrap bootstrap = new Bootstrap();
      String resolvedTransport = this.transportType != null ? this.transportType
          : WebTransportConfig.get("webtransport4j.server.transport", "auto");
      TransportConfig transportConfig = resolveTransport(resolvedTransport, bootstrap);

      this.group = new MultiThreadIoEventLoopGroup(
          Runtime.getRuntime().availableProcessors(), transportConfig.ioHandlerFactory);

      setupTrafficShaping();

      QuicSslContext sslCtx = buildSslContext();
      this.activeSslContext = sslCtx;

      String resolvedKeyPath = this.sslKeyPath != null ? this.sslKeyPath
          : WebTransportConfig.get("webtransport4j.ssl.key.path", null);
      String resolvedCertPath = this.sslCertPath != null ? this.sslCertPath
          : WebTransportConfig.get("webtransport4j.ssl.cert.path", null);
      boolean hotReloadEnabled = WebTransportConfig.getBoolean("webtransport4j.ssl.hot_reload.enabled", true);

      if (hotReloadEnabled && resolvedKeyPath != null && resolvedCertPath != null) {
        this.tlsWatcher = new TlsCertificateWatcher(resolvedKeyPath, resolvedCertPath, newCtx -> {
          this.activeSslContext = newCtx;
        });
        this.tlsWatcher.start();
      }

      Http3Settings settings = buildHttp3Settings();

      long idleTimeout = this.idleTimeoutSeconds != null ? this.idleTimeoutSeconds
          : (long) WebTransportConfig.getInt("webtransport4j.quic.idle.timeout.seconds", 60);

      QuicServerCodecBuilder builder = Http3.newQuicServerCodecBuilder()
          .sslContext(sslCtx)
          .maxIdleTimeout(idleTimeout, TimeUnit.SECONDS)
          .initialMaxData(this.initialMaxData != null ? this.initialMaxData
              : WebTransportConfig.getLong("webtransport4j.quic.initial.max.data", 0L))
          .initialMaxStreamDataBidirectionalLocal(
              WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.local", 0L))
          .initialMaxStreamDataBidirectionalRemote(
              WebTransportConfig.getLong("webtransport4j.quic.stream.data.bidi.remote", 0L))
          .initialMaxStreamsBidirectional(
              this.initialMaxStreamsBidi != null ? this.initialMaxStreamsBidi
                  : WebTransportConfig.getLong("webtransport4j.quic.max.streams.bidi", 0L))
          .datagram(
              WebTransportConfig.getInt("webtransport4j.quic.datagram.recv.queue.len", 0),
              WebTransportConfig.getInt("webtransport4j.quic.datagram.send.queue.len", 0))
          .initialMaxStreamsUnidirectional(
              this.initialMaxStreamsUni != null ? this.initialMaxStreamsUni
                  : WebTransportConfig.getLong("webtransport4j.quic.max.streams.uni", 0L))
          .initialMaxStreamDataUnidirectional(
              WebTransportConfig.getLong("webtransport4j.quic.stream.data.uni", 0L))
          .tokenHandler(resolveTokenHandler())
          .handler(
              new QuicChannelInitializer(
                  this, settings, businessExecutor, resolvedOrigins, globalActiveSessions));

      configureOptionalQuicParams(builder);

      ChannelHandler serverCodec = builder.build();
      bindServer(bootstrap, transportConfig, serverCodec, targetPort);
      state.set(ServerState.STARTED);
    } catch (Exception e) {
      state.set(ServerState.STOPPED);
      throw e;
    }
  }

  /**
   * Starts the server non-blockingly and then blocks until server shutdown.
   */
  public void startAndAwait() throws Exception {
    start();
    awaitShutdown();
  }

  /**
   * Blocks the current thread until the server channel is closed.
   */
  public void awaitShutdown() throws InterruptedException {
    if (channel != null) {
      channel.closeFuture().sync();
    }
  }

  private static class TransportConfig {
    final IoHandlerFactory ioHandlerFactory;
    final Class<? extends Channel> channelClass;
    final boolean epollGroEnabled;

    TransportConfig(
        IoHandlerFactory ioHandlerFactory,
        Class<? extends Channel> channelClass,
        boolean epollGroEnabled) {
      this.ioHandlerFactory = ioHandlerFactory;
      this.channelClass = channelClass;
      this.epollGroEnabled = epollGroEnabled;
    }
  }

  private @NonNull TransportConfig resolveTransport(String transportType, Bootstrap bootstrap) {
    IoHandlerFactory ioHandlerFactory = null;
    Class<? extends Channel> channelClass = null;
    boolean epollGroEnabled = false;

    if ("auto".equalsIgnoreCase(transportType) || "iouring".equalsIgnoreCase(transportType)) {
      try {
        Class<?> ioUringClass = Class.forName("io.netty.channel.uring.IOUring");
        Method isAvailableMethod = ioUringClass.getMethod("isAvailable");
        boolean isAvailable = (boolean) isAvailableMethod.invoke(null);
        if (isAvailable) {
          Class<?> ioHandlerClass = Class.forName("io.netty.channel.uring.IOUringIoHandler");
          Method newFactoryMethod = ioHandlerClass.getMethod("newFactory");
          ioHandlerFactory = (IoHandlerFactory) newFactoryMethod.invoke(null);

          @SuppressWarnings("unchecked")
          Class<? extends Channel> clazz = (Class<? extends Channel>) Class
              .forName("io.netty.channel.uring.IOUringDatagramChannel");
          channelClass = clazz;

          logger.info("Using IOUring native transport");
        }
      } catch (Throwable t) {
        if ("iouring".equalsIgnoreCase(transportType)) {
          logger.warn(
              "IOUring transport was requested but is not available on the classpath or OS.", t);
        } else if (logger.isDebugEnabled()) {
          logger.debug("IOUring is not available (not on classpath or not supported by OS).");
        }
      }
    }

    if (ioHandlerFactory == null
        && ("auto".equalsIgnoreCase(transportType) || "epoll".equalsIgnoreCase(transportType))) {
      try {
        Class<?> epollClass = Class.forName("io.netty.channel.epoll.Epoll");
        Class<?> epollOptionClass = Class.forName("io.netty.channel.epoll.EpollChannelOption");
        Method isAvailableMethod = epollClass.getMethod("isAvailable");
        boolean isAvailable = (boolean) isAvailableMethod.invoke(null);
        if (isAvailable) {
          Class<?> ioHandlerClass = Class.forName("io.netty.channel.epoll.EpollIoHandler");
          Method newFactoryMethod = ioHandlerClass.getMethod("newFactory");
          ioHandlerFactory = (IoHandlerFactory) newFactoryMethod.invoke(null);
          boolean udpGro = WebTransportConfig.getBoolean("webtransport4j.epoll.udpgro", true);
          epollGroEnabled = udpGro;
          @SuppressWarnings("unchecked")
          ChannelOption<Boolean> udpGroOption = (ChannelOption<Boolean>) epollOptionClass.getField("UDP_GRO").get(null);
          bootstrap.option(udpGroOption, udpGro);

          boolean udpGso = WebTransportConfig.getBoolean("webtransport4j.epoll.udpgso", true);
          if (udpGso) {
            int gsoSize = WebTransportConfig.getInt("webtransport4j.epoll.gso.size", 64);
            if (gsoSize < 1 || gsoSize > 64) {
              throw new IllegalArgumentException(
                  "webtransport4j.epoll.gso.size must be in range 1 - 64");
            }
            bootstrap.option(
                QuicChannelOption.SEGMENTED_DATAGRAM_PACKET_ALLOCATOR,
                EpollQuicUtils.newSegmentedAllocator(gsoSize));
          }
          @SuppressWarnings("unchecked")
          Class<? extends Channel> clazz = (Class<? extends Channel>) Class
              .forName("io.netty.channel.epoll.EpollDatagramChannel");
          channelClass = clazz;

          logger.info("Using Epoll native transport");
        }
      } catch (Throwable t) {
        if ("epoll".equalsIgnoreCase(transportType)) {
          logger.warn("Epoll transport was requested but is not available.", t);
        } else if (logger.isDebugEnabled()) {
          logger.debug("Epoll is not available.");
        }
      }
    }

    if (ioHandlerFactory == null
        && ("auto".equalsIgnoreCase(transportType) || "kqueue".equalsIgnoreCase(transportType))) {
      try {
        Class<?> kqueueClass = Class.forName("io.netty.channel.kqueue.KQueue");
        Method isAvailableMethod = kqueueClass.getMethod("isAvailable");
        boolean isAvailable = (boolean) isAvailableMethod.invoke(null);
        if (isAvailable) {
          Class<?> ioHandlerClass = Class.forName("io.netty.channel.kqueue.KQueueIoHandler");
          Method newFactoryMethod = ioHandlerClass.getMethod("newFactory");
          ioHandlerFactory = (IoHandlerFactory) newFactoryMethod.invoke(null);

          @SuppressWarnings("unchecked")
          Class<? extends Channel> clazz = (Class<? extends Channel>) Class
              .forName("io.netty.channel.kqueue.KQueueDatagramChannel");
          channelClass = clazz;

          logger.info("Using KQueue native transport");
        }
      } catch (Throwable t) {
        if ("kqueue".equalsIgnoreCase(transportType)) {
          logger.warn("KQueue transport was requested but is not available.", t);
        } else if (logger.isDebugEnabled()) {
          logger.debug("KQueue is not available.");
        }
      }
    }

    if (ioHandlerFactory == null) {
      logger.info("Using NIO transport");
      ioHandlerFactory = NioIoHandler.newFactory();
      channelClass = NioDatagramChannel.class;
    }

    return new TransportConfig(ioHandlerFactory, channelClass, epollGroEnabled);
  }

  private void setupTrafficShaping() {
    long globalWriteLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.global.write.limit", 0L);
    long globalReadLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.global.read.limit", 0L);
    if (globalWriteLimit > 0 || globalReadLimit > 0) {
      globalTrafficShaper = new GlobalTrafficShapingHandler(group, globalWriteLimit, globalReadLimit);
    }
  }

  private @NonNull QuicSslContext buildSslContext() throws Exception {
    if (this.sslContext != null) {
      return this.sslContext;
    }
    String keyPath = this.sslKeyPath != null ? this.sslKeyPath
        : WebTransportConfig.get("webtransport4j.ssl.key.path", null);
    String certPath = this.sslCertPath != null ? this.sslCertPath
        : WebTransportConfig.get("webtransport4j.ssl.cert.path", null);
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
    QuicSslContext resolvedSslCtx;
    if (keyPath != null && certPath != null) {
      QuicSslContextBuilder builder = QuicSslContextBuilder.forServer(new File(keyPath), null, new File(certPath))
          .applicationProtocols(Http3.supportedApplicationProtocols());
      if (sessionTimeout > 0) {
        builder.sessionTimeout(sessionTimeout);
      }
      if (sessionCacheSize > 0) {
        builder.sessionCacheSize(sessionCacheSize);
      }
      resolvedSslCtx = builder.build();
    } else {
      throw new IllegalStateException(
          "SSL key path and certificate path must be configured. Set webtransport4j.ssl.key.path"
              + " and webtransport4j.ssl.cert.path in configuration or builder.");
    }
    String ticketKeysStr = WebTransportConfig.get("webtransport4j.ssl.session.ticket.keys", null);
    if (ticketKeysStr != null && !ticketKeysStr.trim().isEmpty()) {
      try {
        String[] keysList = ticketKeysStr.split(",");
        SslSessionTicketKey[] ticketKeys = new SslSessionTicketKey[keysList.length];
        for (int i = 0; i < keysList.length; i++) {
          String hex = keysList[i].trim();
          if (hex.length() != 96) {
            throw new IllegalArgumentException(
                "Session ticket key must be exactly 96 hex characters (16 byte name + 16 byte HMAC"
                    + " + 16 byte AES)");
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
        if (resolvedSslCtx.sessionContext() != null) {
          resolvedSslCtx.sessionContext().setTicketKeys(ticketKeys);
          logger.info(
              "🔑 Explicit TLS Session Ticket Keys loaded. 1-RTT Session Resumption across servers"
                  + " is fully supported.");
        }
      } catch (Exception e) {
        logger.error("❌ Failed to parse webtransport4j.ssl.session.ticket.keys", e);
      }
    }
    return resolvedSslCtx;
  }

  private @NonNull Http3Settings buildHttp3Settings() {
    String allowedProp = WebTransportConfig.getNonNull(
        "webtransport4j.webtransport.settings.nonstandardallowed",
        "0x2c7cf000,0x2b64,0x2b65,0x2b61");

    LongSet allowed = new LongOpenHashSet();
    for (String val : allowedProp.split(",")) {
      allowed.add(Long.decode(val.trim()).longValue());
    }
    Http3Settings settings = new Http3Settings((id, value) -> allowed.contains(id));
    long wtMaxStreamsUni = this.initialMaxStreamsUni != null ? this.initialMaxStreamsUni
        : WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.streams.uni", 0L);
    long wtMaxStreamsBidi = this.initialMaxStreamsBidi != null ? this.initialMaxStreamsBidi
        : WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.streams.bidi", 0L);
    long wtInitialMaxData = this.initialMaxData != null ? this.initialMaxData
        : WebTransportConfig.getLong("webtransport4j.webtransport.initial.max.data", 0L);
    long quicMaxStreamsUni = this.initialMaxStreamsUni != null ? this.initialMaxStreamsUni
        : WebTransportConfig.getLong("webtransport4j.quic.max.streams.uni", 0L);
    long quicMaxStreamsBidi = this.initialMaxStreamsBidi != null ? this.initialMaxStreamsBidi
        : WebTransportConfig.getLong("webtransport4j.quic.max.streams.bidi", 0L);
    long quicInitialMaxData = this.initialMaxData != null ? this.initialMaxData
        : WebTransportConfig.getLong("webtransport4j.quic.initial.max.data", 0L);
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
    settings.put(
        0x2c7cf000L,
        WebTransportConfig.getLong("webtransport4j.webtransport.settings.wt_enabled.value", 0L));
    settings.put(0x2b64L, wtMaxStreamsUni);
    settings.put(0x2b65L, wtMaxStreamsBidi);
    settings.put(0x2b61L, wtInitialMaxData);
    settings.put(0x2b603742L, 1L);
    if (logger.isDebugEnabled()) {
      logger.debug("Server side settings : {}", settings);
    }
    return settings;
  }

  private void configureOptionalQuicParams(QuicServerCodecBuilder builder) {
    String greaseVal = WebTransportConfig.get("webtransport4j.quic.grease.enabled", null);
    if (greaseVal != null) {
      builder.grease(Boolean.parseBoolean(greaseVal));
    }

    String maxSendUdpVal = WebTransportConfig.get("webtransport4j.quic.payload.size.send.max", null);
    if (maxSendUdpVal != null) {
      builder.maxSendUdpPayloadSize(Long.parseLong(maxSendUdpVal));
    }

    String maxRecvUdpVal = WebTransportConfig.get("webtransport4j.quic.payload.size.recv.max", null);
    if (maxRecvUdpVal != null) {
      builder.maxRecvUdpPayloadSize(Long.parseLong(maxRecvUdpVal));
    }

    String ackExponentVal = WebTransportConfig.get("webtransport4j.quic.ack.delay.exponent", null);
    if (ackExponentVal != null) {
      builder.ackDelayExponent(Long.parseLong(ackExponentVal));
    }

    String maxAckDelayVal = WebTransportConfig.get("webtransport4j.quic.ack.delay.max.ms", null);
    if (maxAckDelayVal != null) {
      builder.maxAckDelay(Long.parseLong(maxAckDelayVal), TimeUnit.MILLISECONDS);
    }

    String migrationVal = WebTransportConfig.get("webtransport4j.quic.active.migration.enabled", null);
    if (migrationVal != null) {
      builder.activeMigration(Boolean.parseBoolean(migrationVal));
    }

    String hystartVal = WebTransportConfig.get("webtransport4j.quic.hystart.enabled", null);
    if (hystartVal != null) {
      builder.hystart(Boolean.parseBoolean(hystartVal));
    }

    String discoverPmtuVal = WebTransportConfig.get("webtransport4j.quic.discover.pmtu.enabled", null);
    if (discoverPmtuVal != null) {
      builder.discoverPmtu(Boolean.parseBoolean(discoverPmtuVal));
    }

    String ccAlgoVal = WebTransportConfig.get("webtransport4j.quic.congestion.control.algorithm", null);
    if (ccAlgoVal != null) {
      try {
        builder.congestionControlAlgorithm(
            QuicCongestionControlAlgorithm.valueOf(ccAlgoVal.toUpperCase()));
      } catch (IllegalArgumentException e) {
        logger.warn("⚠️ Invalid congestion control algorithm '{}'. Using default.", ccAlgoVal);
      }
    }

    String initialCwndVal = WebTransportConfig.get("webtransport4j.quic.initial.congestion.window.packets", null);
    if (initialCwndVal != null) {
      builder.initialCongestionWindowPackets(Integer.parseInt(initialCwndVal));
    }

    String localConnIdLenVal = WebTransportConfig.get("webtransport4j.quic.connection.id.length.local", null);
    if (localConnIdLenVal != null) {
      builder.localConnectionIdLength(Integer.parseInt(localConnIdLenVal));
    }

    String activeConnIdLimitVal = WebTransportConfig.get("webtransport4j.quic.connection.id.limit.active", null);
    if (activeConnIdLimitVal != null) {
      builder.activeConnectionIdLimit(Long.parseLong(activeConnIdLimitVal));
    }
  }

  private void bindServer(
      Bootstrap bootstrap, @NonNull TransportConfig transportConfig, ChannelHandler serverCodec, int targetPort)
      throws Exception {
    int defaultRecvBufSize = transportConfig.epollGroEnabled ? 65536 : 2048;
    int recvBufSize = WebTransportConfig.getInt("webtransport4j.server.recv.buffer.size", defaultRecvBufSize);
    FixedRecvByteBufAllocator recvByteBufAllocator = new FixedRecvByteBufAllocator(recvBufSize);
    recvByteBufAllocator.maxMessagesPerRead(Integer.MAX_VALUE);
    int sndBuf = WebTransportConfig.getInt("webtransport4j.server.socket.sndbuf", 0);
    if (sndBuf > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, sndBuf);
    }
    int rcvBuf = WebTransportConfig.getInt("webtransport4j.server.socket.rcvbuf", 0);
    if (rcvBuf > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, rcvBuf);
    }

    ChannelFuture bindFuture = bootstrap
        .group(group)
        .channel(transportConfig.channelClass)
        .handler(serverCodec)
        .option(ChannelOption.RECVBUF_ALLOCATOR, recvByteBufAllocator)
        .bind(new InetSocketAddress(targetPort));

    bindFuture.sync();
    this.channel = bindFuture.channel();
    this.channel.attr(WebTransportAttributeKeys.METRICS_LISTENER).set(metricsListener);

    int boundPort = getPort();
    logger.info("✅ WebTransport server started on port {}", boundPort);
  }

  /** Stops the server gracefully. */
  public void stop() {
    stop(5, TimeUnit.SECONDS);
  }

  /** Stops the server with a specified timeout. */
  public void stop(long timeout, @NonNull TimeUnit unit) {
    ServerState previous = state.getAndSet(ServerState.STOPPING);
    if (previous == ServerState.STOPPED || previous == ServerState.STOPPING) {
      logger.debug("Server is already stopped or stopping.");
      return;
    }
    try {
      if (tlsWatcher != null) {
        tlsWatcher.stop();
        tlsWatcher = null;
      }
      if (shutdownHook != null) {
        try {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (Exception ignored) {
        }
        shutdownHook = null;
      }
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
          group.shutdownGracefully(1, timeout, unit).sync();
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
      IpRateLimitingHandler.stopReloader();
      if (businessExecutor != null && !businessExecutor.isShutdown()) {
        businessExecutor.shutdown();
        try {
          if (!businessExecutor.awaitTermination(timeout, unit)) {
            businessExecutor.shutdownNow();
          }
        } catch (InterruptedException e) {
          businessExecutor.shutdownNow();
        }
      }
      logger.info("WebTransport server stopped successfully.");
    } finally {
      state.set(ServerState.STOPPED);
    }
  }

  private QuicTokenHandler resolveTokenHandler() {
    if (this.quicTokenHandler != null) {
      return this.quicTokenHandler;
    }
    return getTokenHandler();
  }

  /** Returns the token handler. */
  public static @NonNull QuicTokenHandler getTokenHandler() {
    String tokenHandlerType = WebTransportConfig.get("webtransport4j.quic.token.handler", "hmac");
    if ("insecure".equalsIgnoreCase(tokenHandlerType)) {
      logger.info("🔑 QUIC Token Handler configured: INSECURE (InsecureQuicTokenHandler)");
      return InsecureQuicTokenHandler.INSTANCE;
    } else if ("hmac".equalsIgnoreCase(tokenHandlerType)) {
      long expirationMs = WebTransportConfig.getLong(
          "webtransport4j.quic.token.handler.hmac.expiration.ms", 60000L);
      String keyHex = WebTransportConfig.get("webtransport4j.quic.token.handler.hmac.key", null);
      if (keyHex != null && !keyHex.trim().isEmpty()) {
        byte[] key = parseHex(keyHex);
        if (key != null && key.length >= 16) {
          logger.info(
              "🔑 QUIC Token Handler configured: HMAC (HmacQuicTokenHandler) with custom configured"
                  + " key, expiration: {}ms",
              expirationMs);
          return new HmacQuicTokenHandler(key, expirationMs);
        } else {
          logger.warn(
              "⚠️ Configured HMAC key is too short (must be at least 16 bytes / 32 hex characters)."
                  + " Falling back to random key.");
        }
      }
      logger.info(
          "🔑 QUIC Token Handler configured: HMAC (HmacQuicTokenHandler) with randomly generated"
              + " key, expiration: {}ms",
          expirationMs);
      return new HmacQuicTokenHandler(expirationMs);
    } else {
      try {
        logger.info("🔑 QUIC Token Handler configured: Custom Class ({})", tokenHandlerType);
        return (QuicTokenHandler) Class.forName(tokenHandlerType).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        logger.error(
            "❌ Failed to load custom QuicTokenHandler: {}. Falling back to HmacQuicTokenHandler.",
            tokenHandlerType,
            e);
        return new HmacQuicTokenHandler();
      }
    }
  }

  private static byte @Nullable [] parseHex(@Nullable String hex) {
    if (hex == null || hex.trim().isEmpty()) {
      return null;
    }
    String normalized = hex.trim();
    if (normalized.length() % 2 != 0) {
      logger.warn(
          "⚠️ HMAC key hex string length is not even: {}. Falling back to plain string bytes.",
          normalized);
      return normalized.getBytes(StandardCharsets.UTF_8);
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
      logger.warn(
          "⚠️ Failed to parse HMAC key as hex, falling back to plain string bytes: {}",
          e.getMessage());
      return normalized.getBytes(StandardCharsets.UTF_8);
    }
  }

  /** Validate Config. */
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
