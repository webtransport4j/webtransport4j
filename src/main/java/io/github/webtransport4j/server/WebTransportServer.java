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
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main WebTransport server managing QUIC connections.
 *
 * <p>Lifecycle is serialized on {@code lifecycleLock}. An in-flight {@link #start()} that loses a
 * race with {@link #stop()} discards the channel it just bound instead of leaking it. The server is
 * {@link AutoCloseable} so try-with-resources and framework destroy callbacks both shut it down.
 */
public class WebTransportServer implements AutoCloseable {

  static {
    // Must be set before Netty's PooledByteBufAllocator is initialized. If the property is already
    // present we never override operator intent. Prefer -D on the JVM command line in production.
    if (System.getProperty("io.netty.allocator.useCacheForAllThreads") == null) {
      System.setProperty("io.netty.allocator.useCacheForAllThreads", "false");
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(WebTransportServer.class);

  private static final WebTransportHandler NO_OP_HANDLER = new WebTransportHandler() {};

  private static final int DEFAULT_PORT = 4433;
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final int MIN_PORT = 0;
  private static final int MAX_PORT = 65535;
  private static final int DEFAULT_IDLE_TIMEOUT_SECONDS = 60;
  private static final long DEFAULT_INITIAL_MAX_DATA = 16L * 1024 * 1024;
  private static final long DEFAULT_STREAM_DATA = 1024L * 1024;
  private static final long DEFAULT_MAX_STREAMS_BIDI = 128L;
  private static final long DEFAULT_MAX_STREAMS_UNI = 128L;
  private static final int DEFAULT_DATAGRAM_QUEUE_LEN = 32;
  private static final int DEFAULT_RECV_BUFFER_SIZE = 65536;
  private static final int DEFAULT_SOCKET_BUFFER_SIZE = 16 * 1024 * 1024;
  private static final int DEFAULT_BIND_TIMEOUT_SECONDS = 10;
  private static final int DEFAULT_GSO_SIZE = 64;
  private static final long DEFAULT_HMAC_EXPIRATION_MS = 60_000L;
  private static final int HMAC_KEY_MIN_BYTES = 16;
  private static final int SESSION_TICKET_KEY_HEX_LEN = 96;
  private static final long SETTING_WT_ENABLED = 0x2c7cf000L;
  private static final long SETTING_WT_MAX_STREAMS_UNI = 0x2b64L;
  private static final long SETTING_WT_MAX_STREAMS_BIDI = 0x2b65L;
  private static final long SETTING_WT_INITIAL_MAX_DATA = 0x2b61L;
  private static final long SETTING_WEBTRANSPORT_MAX_SESSIONS = 0x2b603742L;
  private static final String DEFAULT_ALLOWED_SETTINGS =
      "0x2c7cf000,0x2b64,0x2b65,0x2b61,0x2b603742";

  private Integer configuredPort;
  private String configuredHost;
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
  private volatile WebTransportHandler defaultHandler;

  private final AtomicInteger globalActiveSessions = new AtomicInteger(0);
  private final AtomicInteger globalSessionSlots = new AtomicInteger(0);

  private volatile WebTransportMetricsListener metricsListener =
      NoOpWebTransportMetricsListener.INSTANCE;

  private volatile Supplier<MessageDispatcher> messageDispatcherSupplier =
      () -> DefaultMessageDispatcher.INSTANCE;
  private final ExecutorService businessExecutor;
  private final boolean ownsBusinessExecutor;

  private volatile GlobalTrafficShapingHandler trafficShaper;
  private Long configuredGlobalWriteLimit;
  private Long configuredGlobalReadLimit;

  // Weak keys remember transferred handlers without keeping released handlers alive.
  private static final Map<GlobalTrafficShapingHandler, Boolean> OWNED_TRAFFIC_SHAPERS =
      new WeakHashMap<>();
  private static final Object SERVER_INSTANCES_LOCK = new Object();
  private static final AtomicInteger ACTIVE_SERVER_INSTANCES = new AtomicInteger(0);

  /** Lifecycle states of the WebTransport server. */
  public enum ServerState {
    STOPPED,
    STARTING,
    STARTED,
    STOPPING
  }

  private final Object lifecycleLock = new Object();
  private final AtomicReference<ServerState> state = new AtomicReference<>(ServerState.STOPPED);
  /** Incremented by {@link #stop} to invalidate an in-flight {@link #start}. */
  private final AtomicLong startEpoch = new AtomicLong();

  private EventLoopGroup group;
  private Channel channel;
  private Thread shutdownHook;
  private volatile QuicSslContext activeSslContext;
  private volatile TlsCertificateWatcher tlsWatcher;
  private SelfSignedCertificate generatedCertificate;

  public @Nullable QuicSslContext getActiveSslContext() {
    return activeSslContext;
  }

  /**
   * Checks for certificate modifications and hot-reloads if changes are detected.
   *
   * @return true if certificates were reloaded, false otherwise
   */
  public boolean checkAndReloadTlsCertificates() {
    TlsCertificateWatcher watcher = tlsWatcher;
    return watcher != null && watcher.checkAndReload();
  }

  public static @NonNull WebTransportServerBuilder builder() {
    return new WebTransportServerBuilder();
  }

  /** Web Transport Server. */
  public WebTransportServer(WebTransportHandler defaultHandler) {
    this.defaultHandler = requireHandler(defaultHandler);
    handlers.put("/", this.defaultHandler);
    this.businessExecutor = BusinessExecutorFactory.create();
    this.ownsBusinessExecutor = true;
  }

  /** Constructs a WebTransportServer with a default no-op handler. */
  public WebTransportServer() {
    this.defaultHandler = NO_OP_HANDLER;
    handlers.put("/", this.defaultHandler);
    this.businessExecutor = BusinessExecutorFactory.create();
    this.ownsBusinessExecutor = true;
  }

  /** Web Transport Server with custom business executor. */
  public WebTransportServer(WebTransportHandler defaultHandler, ExecutorService businessExecutor) {
    this.defaultHandler = requireHandler(defaultHandler);
    handlers.put("/", this.defaultHandler);
    this.ownsBusinessExecutor = businessExecutor == null;
    this.businessExecutor =
        businessExecutor != null ? businessExecutor : BusinessExecutorFactory.create();
  }

  /** Constructs a WebTransportServer using a {@link WebTransportServerBuilder}. */
  public WebTransportServer(@NonNull WebTransportServerBuilder builder) {
    Objects.requireNonNull(builder, "builder");
    this.configuredPort = builder.getPort();
    this.configuredHost = builder.getHost();
    this.sslKeyPath = builder.getSslKeyPath();
    this.sslCertPath = builder.getSslCertPath();
    this.sslContext = builder.getSslContext();
    this.allowedOrigins = copyOrigins(builder.getAllowedOrigins());
    this.quicTokenHandler = builder.getQuicTokenHandler();
    this.transportType = builder.getTransportType();
    this.idleTimeoutSeconds = builder.getIdleTimeoutSeconds();
    this.initialMaxStreamsBidi = builder.getInitialMaxStreamsBidi();
    this.initialMaxStreamsUni = builder.getInitialMaxStreamsUni();
    this.initialMaxData = builder.getInitialMaxData();
    this.trafficShaper = claimTrafficShaper(builder.getTrafficShaper());
    this.configuredGlobalWriteLimit = builder.getGlobalTrafficWriteLimit();
    this.configuredGlobalReadLimit = builder.getGlobalTrafficReadLimit();

    if (builder.getMetricsListener() != null) {
      this.metricsListener = builder.getMetricsListener();
    }
    if (builder.getMessageDispatcherSupplier() != null) {
      this.messageDispatcherSupplier = builder.getMessageDispatcherSupplier();
    }
    this.ownsBusinessExecutor = builder.getBusinessExecutor() == null;
    this.businessExecutor =
        builder.getBusinessExecutor() != null
            ? builder.getBusinessExecutor()
            : BusinessExecutorFactory.create();

    this.handlers.putAll(builder.getHandlers());
    WebTransportHandler configuredDefault = builder.getDefaultHandler();
    if (configuredDefault != null) {
      this.defaultHandler = configuredDefault;
      this.handlers.put("/", configuredDefault);
    } else {
      this.defaultHandler = this.handlers.getOrDefault("/", NO_OP_HANDLER);
      this.handlers.putIfAbsent("/", this.defaultHandler);
    }
  }

  /**
   * Returns an unmodifiable live view of registered path handlers. Mutate the routing table through
   * {@link #registerHandler} / {@link #registerReactiveHandler}.
   */
  public Map<String, WebTransportHandler> getHandlers() {
    return Collections.unmodifiableMap(handlers);
  }

  private static @NonNull WebTransportHandler requireHandler(@Nullable WebTransportHandler handler) {
    if (handler == null) {
      throw new IllegalArgumentException("defaultHandler cannot be null");
    }
    return handler;
  }

  private static @Nullable List<String> copyOrigins(@Nullable List<String> origins) {
    if (origins == null) {
      return null;
    }
    List<String> copy = new ArrayList<>(origins.size());
    for (String origin : origins) {
      if (origin == null) {
        continue;
      }
      String trimmed = origin.trim();
      if (!trimmed.isEmpty()) {
        copy.add(trimmed);
      }
    }
    return Collections.unmodifiableList(copy);
  }

  private static @Nullable String normalizePath(@Nullable String path) {
    if (path == null) {
      return null;
    }
    String trimmed = path.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (trimmed.length() > 1 && trimmed.endsWith("/")) {
      return trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  /** Register Handler. Passing {@code null} removes the path. */
  public void registerHandler(@NonNull String path, @Nullable WebTransportHandler handler) {
    String normalized = normalizePath(path);
    if (normalized == null || !normalized.startsWith("/")) {
      throw new IllegalArgumentException(
          "path must not be null or empty and must start with '/'");
    }
    if (handler == null) {
      handlers.remove(normalized);
      if ("/".equals(normalized)) {
        this.defaultHandler = NO_OP_HANDLER;
      }
    } else {
      handlers.put(normalized, handler);
      if ("/".equals(normalized)) {
        this.defaultHandler = handler;
      }
    }
  }

  /** Registers a reactive handler for a path. */
  public void registerReactiveHandler(
      @NonNull String path, @Nullable ReactiveWebTransportHandler reactiveHandler) {
    if (reactiveHandler == null) {
      registerHandler(path, null);
    } else {
      registerHandler(path, new ReactiveWebTransportHandlerAdapter(reactiveHandler));
    }
  }

  /** Sets a custom metrics listener for observability export. */
  public void setMetricsListener(@NonNull WebTransportMetricsListener listener) {
    this.metricsListener = Objects.requireNonNull(listener, "listener");
  }

  public @NonNull WebTransportMetricsListener getMetricsListener() {
    return metricsListener;
  }

  public void setMessageDispatcher(@NonNull MessageDispatcher dispatcher) {
    MessageDispatcher actual = Objects.requireNonNull(dispatcher, "dispatcher");
    this.messageDispatcherSupplier = () -> actual;
  }

  public void setMessageDispatcherSupplier(@NonNull Supplier<MessageDispatcher> supplier) {
    this.messageDispatcherSupplier = Objects.requireNonNull(supplier, "supplier");
  }

  public @NonNull Supplier<MessageDispatcher> getMessageDispatcherSupplier() {
    return messageDispatcherSupplier;
  }

  /**
   * Returns the traffic shaping handler for this server instance, or null if traffic shaping is not
   * enabled.
   */
  public @Nullable GlobalTrafficShapingHandler getTrafficShaper() {
    return trafficShaper;
  }

  /**
   * Sets the traffic shaping handler while this server is stopped, transferring exclusive ownership.
   * A handler previously transferred to a server cannot be reused. The caller remains responsible
   * for releasing a handler replaced before startup.
   *
   * @throws IllegalStateException if the server is not stopped or the handler was already transferred
   */
  public void setTrafficShaper(@Nullable GlobalTrafficShapingHandler trafficShaper) {
    synchronized (lifecycleLock) {
      if (state.get() != ServerState.STOPPED) {
        throw new IllegalStateException(
            "Traffic shaper can only be replaced while the server is stopped");
      }
      if (this.trafficShaper != trafficShaper) {
        this.trafficShaper = claimTrafficShaper(trafficShaper);
      }
    }
  }

  private static GlobalTrafficShapingHandler claimTrafficShaper(
      GlobalTrafficShapingHandler handler) {
    if (handler != null) {
      synchronized (OWNED_TRAFFIC_SHAPERS) {
        if (OWNED_TRAFFIC_SHAPERS.putIfAbsent(handler, Boolean.TRUE) != null) {
          throw new IllegalStateException(
              "Traffic shaper ownership has already been transferred to a server");
        }
      }
    }
    return handler;
  }

  private void releaseTrafficShaper() {
    GlobalTrafficShapingHandler handler = this.trafficShaper;
    this.trafficShaper = null;
    if (handler != null) {
      handler.release();
    }
  }

  private static void registerServerInstance() {
    synchronized (SERVER_INSTANCES_LOCK) {
      ACTIVE_SERVER_INSTANCES.incrementAndGet();
    }
  }

  private static void unregisterServerInstance() {
    synchronized (SERVER_INSTANCES_LOCK) {
      if (ACTIVE_SERVER_INSTANCES.decrementAndGet() == 0) {
        IpRateLimitingHandler.stopReloader();
      }
    }
  }

  /** Returns the handler for a path, or the default handler if none is registered. */
  public @NonNull WebTransportHandler getHandler(@NonNull String path) {
    String normalized = normalizePath(path);
    WebTransportHandler handler = normalized == null ? null : handlers.get(normalized);
    WebTransportHandler fallback = this.defaultHandler;
    return handler != null ? handler : (fallback != null ? fallback : NO_OP_HANDLER);
  }

  /** Returns the actual bound server port, or configured port if not started. */
  public int getPort() {
    Channel ch = this.channel;
    if (ch != null && ch.isActive() && ch.localAddress() instanceof InetSocketAddress) {
      return ((InetSocketAddress) ch.localAddress()).getPort();
    }
    if (configuredPort != null) {
      return configuredPort;
    }
    return WebTransportConfig.getInt("webtransport4j.server.port", DEFAULT_PORT);
  }

  /** Returns the bound server host address, or configured host if not started. */
  public String getHost() {
    Channel ch = this.channel;
    if (ch != null && ch.isActive() && ch.localAddress() instanceof InetSocketAddress) {
      return ((InetSocketAddress) ch.localAddress()).getHostString();
    }
    if (configuredHost != null) {
      return configuredHost;
    }
    return WebTransportConfig.get("webtransport4j.server.host", DEFAULT_HOST);
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
    Channel ch = this.channel;
    return state.get() == ServerState.STARTED && ch != null && ch.isActive();
  }

  /** Returns true if the server is active and listening. */
  public boolean isRunning() {
    return isStarted();
  }

  /**
   * Starts the WebTransport server non-blockingly. Returns immediately once the server channel is
   * bound. Concurrent calls while already starting or started are ignored. A concurrent {@link
   * #stop()} wins: the bound channel is closed and the server stays stopped.
   */
  public void start() throws Exception {
    if (!state.compareAndSet(ServerState.STOPPED, ServerState.STARTING)) {
      ServerState current = state.get();
      if (current == ServerState.STARTED || current == ServerState.STARTING) {
        logger.warn("⚠️ Server is already {} on port {}", current.name().toLowerCase(Locale.ROOT), getPort());
        return;
      }
      throw new IllegalStateException("Cannot start WebTransportServer while in state: " + current);
    }

    final long epoch = startEpoch.incrementAndGet();
    registerServerInstance();
    try {
      doStart(epoch);
    } catch (Exception e) {
      try {
        stop(5, TimeUnit.SECONDS, false);
      } catch (RuntimeException cleanupFailure) {
        e.addSuppressed(cleanupFailure);
      }
      throw e;
    }
  }

  private void doStart(long epoch) throws Exception {
    if (defaultHandler == null) {
      throw new IllegalStateException("Server cannot start without a registered default path handler.");
    }

    final int targetPort = resolvePort();
    final String targetHost = resolveHost();
    List<String> resolvedOrigins = resolveAllowedOrigins();

    installShutdownHook();

    if (logger.isDebugEnabled()) {
      logger.debug("Starting WebTransport server on {}:{}", targetHost, targetPort);
    }

    Bootstrap bootstrap = new Bootstrap();
    String resolvedTransport =
        this.transportType != null
            ? this.transportType
            : WebTransportConfig.get("webtransport4j.server.transport", "auto");
    TransportConfig transportConfig = resolveTransport(resolvedTransport, bootstrap);

    EventLoopGroup newGroup =
        new MultiThreadIoEventLoopGroup(
            Math.max(1, Runtime.getRuntime().availableProcessors()),
            transportConfig.ioHandlerFactory);

    Channel newChannel = null;
    TlsCertificateWatcher newWatcher = null;
    boolean published = false;
    try {
      this.group = newGroup;
      setupTrafficShaping();

      QuicSslContext sslCtx = buildSslContext();
      this.activeSslContext = sslCtx;

      newWatcher = maybeStartTlsWatcher();
      this.tlsWatcher = newWatcher;

      Http3Settings settings = buildHttp3Settings();
      ResolvedLimits limits = resolveLimits();

      QuicServerCodecBuilder codecBuilder =
          Http3.newQuicServerCodecBuilder()
              .sslContext(sslCtx)
              .maxIdleTimeout(limits.idleTimeoutSeconds, TimeUnit.SECONDS)
              .initialMaxData(limits.quicMaxData)
              .initialMaxStreamDataBidirectionalLocal(limits.streamDataBidiLocal)
              .initialMaxStreamDataBidirectionalRemote(limits.streamDataBidiRemote)
              .initialMaxStreamsBidirectional(limits.quicMaxBidi)
              .datagram(limits.datagramRecvQueue, limits.datagramSendQueue)
              .initialMaxStreamsUnidirectional(limits.quicMaxUni)
              .initialMaxStreamDataUnidirectional(limits.streamDataUni)
              .tokenHandler(resolveTokenHandler())
              .handler(
                  new QuicChannelInitializer(
                      this,
                      settings,
                      businessExecutor,
                      resolvedOrigins,
                      globalActiveSessions,
                      globalSessionSlots));

      configureOptionalQuicParams(codecBuilder);

      ChannelHandler serverCodec = codecBuilder.build();
      newChannel = bindServer(bootstrap, transportConfig, serverCodec, targetHost, targetPort);
      published = publishIfStillStarting(epoch, newGroup, newChannel, newWatcher);
      if (!published) {
        closeQuietly(newChannel);
        throw new IllegalStateException("Server start aborted because shutdown was requested");
      }
    } catch (Exception e) {
      if (!published && newChannel != null && newChannel != this.channel) {
        closeQuietly(newChannel);
      }
      throw e;
    }
  }

  private boolean publishIfStillStarting(
      long epoch, EventLoopGroup newGroup, Channel newChannel, TlsCertificateWatcher newWatcher) {
    synchronized (lifecycleLock) {
      if (epoch != startEpoch.get() || state.get() != ServerState.STARTING) {
        return false;
      }
      this.group = newGroup;
      this.channel = newChannel;
      this.tlsWatcher = newWatcher;
      state.set(ServerState.STARTED);
      return true;
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
    Channel ch = this.channel;
    if (ch != null) {
      ch.closeFuture().sync();
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

  private static final class ResolvedLimits {
    final long idleTimeoutSeconds;
    final long quicMaxData;
    final long wtMaxData;
    final long quicMaxBidi;
    final long wtMaxBidi;
    final long quicMaxUni;
    final long wtMaxUni;
    final long streamDataBidiLocal;
    final long streamDataBidiRemote;
    final long streamDataUni;
    final int datagramRecvQueue;
    final int datagramSendQueue;

    ResolvedLimits(
        long idleTimeoutSeconds,
        long quicMaxData,
        long wtMaxData,
        long quicMaxBidi,
        long wtMaxBidi,
        long quicMaxUni,
        long wtMaxUni,
        long streamDataBidiLocal,
        long streamDataBidiRemote,
        long streamDataUni,
        int datagramRecvQueue,
        int datagramSendQueue) {
      this.idleTimeoutSeconds = idleTimeoutSeconds;
      this.quicMaxData = quicMaxData;
      this.wtMaxData = wtMaxData;
      this.quicMaxBidi = quicMaxBidi;
      this.wtMaxBidi = wtMaxBidi;
      this.quicMaxUni = quicMaxUni;
      this.wtMaxUni = wtMaxUni;
      this.streamDataBidiLocal = streamDataBidiLocal;
      this.streamDataBidiRemote = streamDataBidiRemote;
      this.streamDataUni = streamDataUni;
      this.datagramRecvQueue = datagramRecvQueue;
      this.datagramSendQueue = datagramSendQueue;
    }
  }

  private int resolvePort() {
    int port =
        configuredPort != null
            ? configuredPort
            : WebTransportConfig.getInt("webtransport4j.server.port", DEFAULT_PORT);
    if (port < MIN_PORT || port > MAX_PORT) {
      throw new IllegalArgumentException(
          "webtransport4j.server.port must be between " + MIN_PORT + " and " + MAX_PORT + ": " + port);
    }
    return port;
  }

  private String resolveHost() {
    String host =
        configuredHost != null
            ? configuredHost
            : WebTransportConfig.get("webtransport4j.server.host", DEFAULT_HOST);
    if (host == null || host.trim().isEmpty()) {
      return DEFAULT_HOST;
    }
    return host.trim();
  }

  private @NonNull List<String> resolveAllowedOrigins() {
    List<String> resolvedOrigins = this.allowedOrigins;
    if (resolvedOrigins == null) {
      String originsProp = WebTransportConfig.get("webtransport4j.allowed.origins", null);
      if (originsProp == null || originsProp.trim().isEmpty()) {
        originsProp = WebTransportConfig.get("webtransport4j.server.allowed_origins", "*");
      }
      resolvedOrigins = copyOrigins(Arrays.asList(originsProp.split(",")));
      if (resolvedOrigins == null || resolvedOrigins.isEmpty()) {
        resolvedOrigins = Collections.singletonList("*");
      }
    }
    boolean devMode = WebTransportConfig.getBoolean("webtransport4j.dev_mode", false);
    if (!devMode && resolvedOrigins.size() == 1 && "*".equals(resolvedOrigins.get(0))) {
      logger.warn(
          "⚠️ Allowed origins is '*'. This accepts any browser origin. Set "
              + "webtransport4j.allowed.origins to an explicit allow-list in production.");
    }
    return resolvedOrigins;
  }

  private void installShutdownHook() {
    Thread hook =
        new Thread(
            () -> {
              logger.info("Shutdown hook triggered. Stopping server...");
              stop();
            },
            "webtransport-server-shutdown-hook");
    hook.setDaemon(false);
    this.shutdownHook = hook;
    Runtime.getRuntime().addShutdownHook(hook);
  }

  private @Nullable TlsCertificateWatcher maybeStartTlsWatcher() {
    String resolvedKeyPath =
        this.sslKeyPath != null
            ? this.sslKeyPath
            : WebTransportConfig.get("webtransport4j.ssl.key.path", null);
    String resolvedCertPath =
        this.sslCertPath != null
            ? this.sslCertPath
            : WebTransportConfig.get("webtransport4j.ssl.cert.path", null);
    boolean hotReloadEnabled =
        WebTransportConfig.getBoolean("webtransport4j.ssl.hot_reload.enabled", true);

    if (hotReloadEnabled && resolvedKeyPath != null && resolvedCertPath != null) {
      TlsCertificateWatcher watcher =
          new TlsCertificateWatcher(
              resolvedKeyPath,
              resolvedCertPath,
              newCtx -> {
                this.activeSslContext = newCtx;
              });
      watcher.start();
      return watcher;
    }
    return null;
  }

  private @NonNull ResolvedLimits resolveLimits() {
    long idleTimeout =
        this.idleTimeoutSeconds != null
            ? this.idleTimeoutSeconds
            : WebTransportConfig.getInt(
                "webtransport4j.quic.idle.timeout.seconds", DEFAULT_IDLE_TIMEOUT_SECONDS);
    if (idleTimeout <= 0) {
      throw new IllegalArgumentException("idle timeout must be > 0 seconds: " + idleTimeout);
    }

    long quicMaxData =
        firstPositive(
            this.initialMaxData,
            WebTransportConfig.getLong(
                "webtransport4j.quic.initial.max.data", DEFAULT_INITIAL_MAX_DATA),
            DEFAULT_INITIAL_MAX_DATA);
    long wtMaxData =
        firstPositive(
            this.initialMaxData,
            WebTransportConfig.getLong(
                "webtransport4j.webtransport.initial.max.data", DEFAULT_INITIAL_MAX_DATA),
            DEFAULT_INITIAL_MAX_DATA);

    long quicMaxBidi =
        firstPositive(
            this.initialMaxStreamsBidi,
            WebTransportConfig.getLong(
                "webtransport4j.quic.max.streams.bidi", DEFAULT_MAX_STREAMS_BIDI),
            DEFAULT_MAX_STREAMS_BIDI);
    long wtMaxBidi =
        firstPositive(
            this.initialMaxStreamsBidi,
            WebTransportConfig.getLong(
                "webtransport4j.webtransport.initial.max.streams.bidi", DEFAULT_MAX_STREAMS_BIDI),
            DEFAULT_MAX_STREAMS_BIDI);

    long quicMaxUni =
        firstPositive(
            this.initialMaxStreamsUni,
            WebTransportConfig.getLong(
                "webtransport4j.quic.max.streams.uni", DEFAULT_MAX_STREAMS_UNI),
            DEFAULT_MAX_STREAMS_UNI);
    long wtMaxUni =
        firstPositive(
            this.initialMaxStreamsUni,
            WebTransportConfig.getLong(
                "webtransport4j.webtransport.initial.max.streams.uni", DEFAULT_MAX_STREAMS_UNI),
            DEFAULT_MAX_STREAMS_UNI);

    long streamDataBidiLocal =
        positiveOrDefault(
            WebTransportConfig.getLong(
                "webtransport4j.quic.stream.data.bidi.local", DEFAULT_STREAM_DATA),
            DEFAULT_STREAM_DATA);
    long streamDataBidiRemote =
        positiveOrDefault(
            WebTransportConfig.getLong(
                "webtransport4j.quic.stream.data.bidi.remote", DEFAULT_STREAM_DATA),
            DEFAULT_STREAM_DATA);
    long streamDataUni =
        positiveOrDefault(
            WebTransportConfig.getLong("webtransport4j.quic.stream.data.uni", DEFAULT_STREAM_DATA),
            DEFAULT_STREAM_DATA);

    int datagramRecvQueue =
        positiveOrDefault(
            WebTransportConfig.getInt(
                "webtransport4j.quic.datagram.recv.queue.len", DEFAULT_DATAGRAM_QUEUE_LEN),
            DEFAULT_DATAGRAM_QUEUE_LEN);
    int datagramSendQueue =
        positiveOrDefault(
            WebTransportConfig.getInt(
                "webtransport4j.quic.datagram.send.queue.len", DEFAULT_DATAGRAM_QUEUE_LEN),
            DEFAULT_DATAGRAM_QUEUE_LEN);

    validateConfig(quicMaxBidi, wtMaxBidi, quicMaxUni, wtMaxUni, quicMaxData, wtMaxData);
    return new ResolvedLimits(
        idleTimeout,
        quicMaxData,
        wtMaxData,
        quicMaxBidi,
        wtMaxBidi,
        quicMaxUni,
        wtMaxUni,
        streamDataBidiLocal,
        streamDataBidiRemote,
        streamDataUni,
        datagramRecvQueue,
        datagramSendQueue);
  }

  private static long firstPositive(@Nullable Long override, long configured, long fallback) {
    if (override != null) {
      return override;
    }
    return configured > 0 ? configured : fallback;
  }

  private static long positiveOrDefault(long configured, long fallback) {
    return configured > 0 ? configured : fallback;
  }

  private static int positiveOrDefault(int configured, int fallback) {
    return configured > 0 ? configured : fallback;
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
          Class<? extends Channel> clazz =
              (Class<? extends Channel>)
                  Class.forName("io.netty.channel.uring.IOUringDatagramChannel");
          channelClass = clazz;

          logger.info("Using IOUring native transport");
        }
      } catch (Throwable t) {
        if ("iouring".equalsIgnoreCase(transportType)) {
          logger.warn(
              "⚠️ IOUring transport was requested but is not available on the classpath or OS.", t);
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
          if (udpGro && !WebTransportUtils.isLinuxUdpGroSupported()) {
            udpGro = false;
          }
          epollGroEnabled = udpGro;
          if (udpGro) {
            @SuppressWarnings("unchecked")
            ChannelOption<Boolean> udpGroOption =
                (ChannelOption<Boolean>) epollOptionClass.getField("UDP_GRO").get(null);
            bootstrap.option(udpGroOption, udpGro);
          }

          boolean udpGso = WebTransportConfig.getBoolean("webtransport4j.epoll.udpgso", true);
          if (udpGso && !WebTransportUtils.isLinuxUdpGsoSupported()) {
            udpGso = false;
          }

          if (udpGso) {
            int gsoSize = WebTransportConfig.getInt("webtransport4j.epoll.gso.size", DEFAULT_GSO_SIZE);
            validateGsoSize(gsoSize);
            bootstrap.option(
                QuicChannelOption.SEGMENTED_DATAGRAM_PACKET_ALLOCATOR,
                EpollQuicUtils.newSegmentedAllocator(gsoSize));
          }
          @SuppressWarnings("unchecked")
          Class<? extends Channel> clazz =
              (Class<? extends Channel>)
                  Class.forName("io.netty.channel.epoll.EpollDatagramChannel");
          channelClass = clazz;

          logger.info("Using Epoll native transport (GRO: {}, GSO: {})", udpGro, udpGso);
        }
      } catch (Throwable t) {
        if ("epoll".equalsIgnoreCase(transportType)) {
          logger.warn("⚠️ Epoll transport was requested but is not available.", t);
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
          Class<? extends Channel> clazz =
              (Class<? extends Channel>)
                  Class.forName("io.netty.channel.kqueue.KQueueDatagramChannel");
          channelClass = clazz;

          logger.info("Using KQueue native transport");
        }
      } catch (Throwable t) {
        if ("kqueue".equalsIgnoreCase(transportType)) {
          logger.warn("⚠️ KQueue transport was requested but is not available.", t);
        } else if (logger.isDebugEnabled()) {
          logger.debug("KQueue is not available.");
        }
      }
    }

    if (ioHandlerFactory == null) {
      if (transportType != null
          && !"auto".equalsIgnoreCase(transportType)
          && !"nio".equalsIgnoreCase(transportType)
          && !"iouring".equalsIgnoreCase(transportType)
          && !"epoll".equalsIgnoreCase(transportType)
          && !"kqueue".equalsIgnoreCase(transportType)) {
        throw new IllegalArgumentException("Unknown transport type: " + transportType);
      }
      logger.info("Using NIO transport");
      ioHandlerFactory = NioIoHandler.newFactory();
      channelClass = NioDatagramChannel.class;
    }

    return new TransportConfig(ioHandlerFactory, channelClass, epollGroEnabled);
  }

  private void setupTrafficShaping() {
    long globalWriteLimit =
        configuredGlobalWriteLimit != null
            ? configuredGlobalWriteLimit
            : WebTransportConfig.getLong("webtransport4j.server.traffic.global.write.limit", 0L);
    long globalReadLimit =
        configuredGlobalReadLimit != null
            ? configuredGlobalReadLimit
            : WebTransportConfig.getLong("webtransport4j.server.traffic.global.read.limit", 0L);
    if (this.trafficShaper == null && (globalWriteLimit > 0 || globalReadLimit > 0)) {
      this.trafficShaper =
          claimTrafficShaper(new GlobalTrafficShapingHandler(group, globalWriteLimit, globalReadLimit));
    }
  }

  private @NonNull QuicSslContext buildSslContext() throws Exception {
    if (this.sslContext != null) {
      return this.sslContext;
    }
    String keyPath =
        this.sslKeyPath != null
            ? this.sslKeyPath
            : WebTransportConfig.get("webtransport4j.ssl.key.path", null);
    String certPath =
        this.sslCertPath != null
            ? this.sslCertPath
            : WebTransportConfig.get("webtransport4j.ssl.cert.path", null);

    boolean devMode = WebTransportConfig.getBoolean("webtransport4j.dev_mode", false);

    File keyFile;
    File certFile;

    if ((keyPath == null) != (certPath == null)) {
      throw new IllegalStateException(
          "Both SSL key path and certificate path must be configured together.");
    }

    if (keyPath != null) {
      keyFile = new File(keyPath);
      certFile = new File(certPath);

      if (!keyFile.isFile() || !keyFile.canRead()) {
        throw new IllegalStateException(
            "SSL private key does not exist, is not a regular file, or is unreadable: "
                + keyFile.getAbsolutePath());
      }

      if (!certFile.isFile() || !certFile.canRead()) {
        throw new IllegalStateException(
            "SSL certificate does not exist, is not a regular file, or is unreadable: "
                + certFile.getAbsolutePath());
      }
    } else if (devMode) {
      logger.warn(
          "⚠️ WEBTRANSPORT4J DEVELOPMENT MODE IS ENABLED. A self-signed TLS certificate will be "
              + "generated. Do not use this configuration in production. Configure "
              + "webtransport4j.ssl.key.path and webtransport4j.ssl.cert.path.");

      SelfSignedCertificate ssc = new SelfSignedCertificate("localhost");
      this.generatedCertificate = ssc;
      keyFile = ssc.privateKey();
      certFile = ssc.certificate();
    } else {
      throw new IllegalStateException(
          "SSL key path and certificate path must be configured for production. "
              + "Set webtransport4j.ssl.key.path and webtransport4j.ssl.cert.path, "
              + "or enable webtransport4j.dev_mode=true for local development.");
    }

    long sessionTimeout =
        WebTransportConfig.getLong("webtransport4j.ssl.session.timeout.seconds", -1L);
    long sessionCacheSize =
        WebTransportConfig.getLong("webtransport4j.ssl.session.cache.size", -1L);
    QuicSslContextBuilder sslBuilder =
        QuicSslContextBuilder.forServer(keyFile, null, certFile)
            .applicationProtocols(Http3.supportedApplicationProtocols());
    if (sessionTimeout > 0) {
      sslBuilder.sessionTimeout(sessionTimeout);
    }
    if (sessionCacheSize > 0) {
      sslBuilder.sessionCacheSize(sessionCacheSize);
    }
    QuicSslContext resolvedSslCtx = sslBuilder.build();
    applySessionTicketKeys(resolvedSslCtx);
    return resolvedSslCtx;
  }

  private void applySessionTicketKeys(QuicSslContext resolvedSslCtx) {
    String ticketKeysStr = WebTransportConfig.get("webtransport4j.ssl.session.ticket.keys", null);
    if (ticketKeysStr == null || ticketKeysStr.trim().isEmpty()) {
      return;
    }
    String[] keysList = ticketKeysStr.split(",");
    SslSessionTicketKey[] ticketKeys = new SslSessionTicketKey[keysList.length];
    for (int i = 0; i < keysList.length; i++) {
      String hex = keysList[i].trim();
      if (hex.length() != SESSION_TICKET_KEY_HEX_LEN) {
        throw new IllegalArgumentException(
            "Session ticket key must be exactly "
                + SESSION_TICKET_KEY_HEX_LEN
                + " hex characters (16 byte name + 16 byte HMAC + 16 byte AES)");
      }
      byte[] keyBytes;
      try {
        keyBytes = ByteBufUtil.decodeHexDump(hex);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to parse webtransport4j.ssl.session.ticket.keys at index " + i, e);
      }
      byte[] name = new byte[16];
      byte[] hmacKey = new byte[16];
      byte[] aesKey = new byte[16];
      System.arraycopy(keyBytes, 0, name, 0, 16);
      System.arraycopy(keyBytes, 16, hmacKey, 0, 16);
      System.arraycopy(keyBytes, 32, aesKey, 0, 16);
      ticketKeys[i] = new SslSessionTicketKey(name, hmacKey, aesKey);
    }
    if (resolvedSslCtx.sessionContext() == null) {
      throw new IllegalStateException(
          "TLS session context is unavailable; cannot install session ticket keys");
    }
    resolvedSslCtx.sessionContext().setTicketKeys(ticketKeys);
    logger.info(
        "Explicit TLS session ticket keys loaded ({}). 1-RTT session resumption across servers is enabled.",
        ticketKeys.length);
  }

  /** Builds the HTTP/3 settings frame for this WebTransport server instance. */
  public @NonNull Http3Settings buildHttp3Settings() {
    String allowedProp =
        WebTransportConfig.getNonNull(
            "webtransport4j.webtransport.settings.nonstandardallowed", DEFAULT_ALLOWED_SETTINGS);

    LongSet allowed = new LongOpenHashSet();
    for (String val : allowedProp.split(",")) {
      String trimmed = val.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      allowed.add(Long.decode(trimmed));
    }
    Http3Settings settings = new Http3Settings((id, value) -> allowed.contains(id));
    ResolvedLimits limits = resolveLimits();

    // WebTransport over HTTP/3 requires Extended CONNECT and, for datagrams, H3 DATAGRAM.
    settings.enableH3Datagram(
        WebTransportConfig.getBoolean(
            "webtransport4j.webtransport.settings.enable_h3_datagram", true));
    settings.enableConnectProtocol(
        WebTransportConfig.getBoolean(
            "webtransport4j.webtransport.settings.enable_connect_protocol", true));
    settings.put(
        SETTING_WT_ENABLED,
        WebTransportConfig.getLong("webtransport4j.webtransport.settings.wt_enabled.value", 1L));
    settings.put(SETTING_WT_MAX_STREAMS_UNI, limits.wtMaxUni);
    settings.put(SETTING_WT_MAX_STREAMS_BIDI, limits.wtMaxBidi);
    settings.put(SETTING_WT_INITIAL_MAX_DATA, limits.wtMaxData);
    settings.put(SETTING_WEBTRANSPORT_MAX_SESSIONS, 1L);
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

    applyLongIfPresent(
        "webtransport4j.quic.payload.size.send.max", builder::maxSendUdpPayloadSize);
    applyLongIfPresent(
        "webtransport4j.quic.payload.size.recv.max", builder::maxRecvUdpPayloadSize);
    applyLongIfPresent("webtransport4j.quic.ack.delay.exponent", builder::ackDelayExponent);

    String maxAckDelayVal = WebTransportConfig.get("webtransport4j.quic.ack.delay.max.ms", null);
    if (maxAckDelayVal != null) {
      builder.maxAckDelay(Long.parseLong(maxAckDelayVal), TimeUnit.MILLISECONDS);
    }

    applyBooleanIfPresent(
        "webtransport4j.quic.active.migration.enabled", builder::activeMigration);
    applyBooleanIfPresent("webtransport4j.quic.hystart.enabled", builder::hystart);
    applyBooleanIfPresent("webtransport4j.quic.discover.pmtu.enabled", builder::discoverPmtu);

    String ccAlgoVal =
        WebTransportConfig.get("webtransport4j.quic.congestion.control.algorithm", null);
    if (ccAlgoVal != null) {
      try {
        builder.congestionControlAlgorithm(
            QuicCongestionControlAlgorithm.valueOf(ccAlgoVal.toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid congestion control algorithm '" + ccAlgoVal + "'", e);
      }
    }

    String initialCwndVal =
        WebTransportConfig.get("webtransport4j.quic.initial.congestion.window.packets", null);
    if (initialCwndVal != null) {
      builder.initialCongestionWindowPackets(Integer.parseInt(initialCwndVal));
    }

    String localConnIdLenVal =
        WebTransportConfig.get("webtransport4j.quic.connection.id.length.local", null);
    if (localConnIdLenVal != null) {
      builder.localConnectionIdLength(Integer.parseInt(localConnIdLenVal));
    }

    applyLongIfPresent(
        "webtransport4j.quic.connection.id.limit.active", builder::activeConnectionIdLimit);
  }

  private interface LongSetter {
    void set(long value);
  }

  private interface BooleanSetter {
    void set(boolean value);
  }

  private static void applyLongIfPresent(String key, LongSetter setter) {
    String value = WebTransportConfig.get(key, null);
    if (value != null) {
      setter.set(Long.parseLong(value));
    }
  }

  private static void applyBooleanIfPresent(String key, BooleanSetter setter) {
    String value = WebTransportConfig.get(key, null);
    if (value != null) {
      setter.set(Boolean.parseBoolean(value));
    }
  }

  private Channel bindServer(
      Bootstrap bootstrap,
      @NonNull TransportConfig transportConfig,
      ChannelHandler serverCodec,
      String targetHost,
      int targetPort)
      throws Exception {
    int recvBufSize =
        WebTransportConfig.getInt(
            "webtransport4j.server.recv.buffer.size", DEFAULT_RECV_BUFFER_SIZE);
    if (recvBufSize <= 0) {
      throw new IllegalArgumentException(
          "webtransport4j.server.recv.buffer.size must be > 0: " + recvBufSize);
    }
    FixedRecvByteBufAllocator recvByteBufAllocator = new FixedRecvByteBufAllocator(recvBufSize);
    recvByteBufAllocator.maxMessagesPerRead(Integer.MAX_VALUE);

    int sndBuf =
        WebTransportConfig.getInt("webtransport4j.server.socket.sndbuf", DEFAULT_SOCKET_BUFFER_SIZE);
    if (sndBuf > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, sndBuf);
    }
    int rcvBuf =
        WebTransportConfig.getInt("webtransport4j.server.socket.rcvbuf", DEFAULT_SOCKET_BUFFER_SIZE);
    if (rcvBuf > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, rcvBuf);
    }
    bootstrap.option(ChannelOption.SO_REUSEADDR, true);

    InetSocketAddress bindAddress;
    if (targetHost == null
        || targetHost.trim().isEmpty()
        || "0.0.0.0".equals(targetHost)
        || "::".equals(targetHost)
        || "*".equals(targetHost)) {
      bindAddress = new InetSocketAddress(targetPort);
    } else {
      bindAddress = new InetSocketAddress(targetHost, targetPort);
    }

    ChannelFuture bindFuture =
        bootstrap
            .group(group)
            .channel(transportConfig.channelClass)
            .handler(serverCodec)
            .option(ChannelOption.RECVBUF_ALLOCATOR, recvByteBufAllocator)
            .bind(bindAddress);

    int bindTimeoutSeconds =
        WebTransportConfig.getInt(
            "webtransport4j.server.bind.timeout.seconds", DEFAULT_BIND_TIMEOUT_SECONDS);
    if (!bindFuture.await(bindTimeoutSeconds, TimeUnit.SECONDS)) {
      bindFuture.cancel(true);
      throw new IllegalStateException(
          "Timed out after " + bindTimeoutSeconds + "s binding to " + bindAddress);
    }
    if (!bindFuture.isSuccess()) {
      throw new IllegalStateException("Failed to bind to " + bindAddress, bindFuture.cause());
    }

    Channel bound = bindFuture.channel();
    bound.attr(WebTransportAttributeKeys.METRICS_LISTENER).set(metricsListener);
    bound.attr(WebTransportAttributeKeys.SERVER_KEY).set(this);
    GlobalTrafficShapingHandler effectiveShaper = getTrafficShaper();
    if (effectiveShaper != null) {
      bound.attr(WebTransportAttributeKeys.GLOBAL_TRAFFIC_SHAPER).set(effectiveShaper);
    }

    logger.info("WebTransport server started on {}:{}", getHostFrom(bound), getPortFrom(bound));
    return bound;
  }

  private static int getPortFrom(Channel ch) {
    if (ch.localAddress() instanceof InetSocketAddress) {
      return ((InetSocketAddress) ch.localAddress()).getPort();
    }
    return -1;
  }

  private static String getHostFrom(Channel ch) {
    if (ch.localAddress() instanceof InetSocketAddress) {
      return ((InetSocketAddress) ch.localAddress()).getHostString();
    }
    return "unknown";
  }

  /** Stops the server gracefully. */
  public void stop() {
    stop(5, TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    stop();
  }

  /** Stops the server with a specified timeout. */
  public void stop(long timeout, @NonNull TimeUnit unit) {
    stop(timeout, Objects.requireNonNull(unit, "unit"), true);
  }

  private void stop(long timeout, TimeUnit unit, boolean shutdownExecutor) {
    synchronized (lifecycleLock) {
      startEpoch.incrementAndGet();
      ServerState previous = state.get();
      if (previous == ServerState.STOPPED) {
        if (shutdownExecutor) {
          shutdownBusinessExecutor(timeout, unit);
        }
        logger.debug("Server is already stopped.");
        return;
      }
      if (previous == ServerState.STOPPING) {
        logger.debug("Server is already stopping.");
        return;
      }
      state.set(ServerState.STOPPING);
    }
    try {
      stopTlsWatcher(tlsWatcher);
      tlsWatcher = null;
      if (shutdownHook != null) {
        try {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException expected) {
          // JVM is already shutting down.
        } catch (Exception unexpected) {
          logger.debug("Could not remove shutdown hook", unexpected);
        }
        shutdownHook = null;
      }
      logger.info("Stopping WebTransport server...");
      Channel ch = this.channel;
      this.channel = null;
      if (ch != null) {
        try {
          if (!ch.close().await(timeout, unit)) {
            logger.warn("⚠️ Timed out closing server channel");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Interrupted while closing server channel", e);
        } catch (Exception e) {
          logger.error("Error closing server channel", e);
        }
      }
      EventLoopGroup g = this.group;
      this.group = null;
      if (g != null) {
        try {
          if (!g.shutdownGracefully(0, timeout, unit).await(timeout, unit)) {
            logger.warn("⚠️ Timed out shutting down event loop group");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Interrupted while shutting down event loop group", e);
        } catch (Exception e) {
          logger.error("Error shutting down event loop group", e);
        }
      }
      releaseTrafficShaper();
      unregisterServerInstance();
      deleteGeneratedCertificate();
      if (shutdownExecutor) {
        shutdownBusinessExecutor(timeout, unit);
      }
      logger.info("WebTransport server stopped successfully.");
    } finally {
      state.set(ServerState.STOPPED);
    }
  }

  private void deleteGeneratedCertificate() {
    SelfSignedCertificate ssc = this.generatedCertificate;
    this.generatedCertificate = null;
    if (ssc != null) {
      try {
        ssc.delete();
      } catch (Exception e) {
        logger.debug("Failed to delete generated self-signed certificate", e);
      }
    }
  }

  private static void stopTlsWatcher(@Nullable TlsCertificateWatcher watcher) {
    if (watcher != null) {
      watcher.stop();
    }
  }

  private static void closeQuietly(@Nullable Channel ch) {
    if (ch != null) {
      try {
        ch.close().await(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception ignored) {
        // best-effort rollback of a start that never published
      }
    }
  }

  private void shutdownBusinessExecutor(long timeout, TimeUnit unit) {
    if (ownsBusinessExecutor && businessExecutor != null && !businessExecutor.isShutdown()) {
      businessExecutor.shutdown();
      try {
        if (!businessExecutor.awaitTermination(timeout, unit)) {
          businessExecutor.shutdownNow();
          if (!businessExecutor.awaitTermination(timeout, unit)) {
            logger.warn("⚠️ Business executor did not terminate");
          }
        }
      } catch (InterruptedException e) {
        businessExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
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
      logger.warn(
          "⚠️ QUIC token handler is INSECURE (InsecureQuicTokenHandler). Address validation tokens "
              + "are not cryptographically bound. Do not use this in production.");
      return InsecureQuicTokenHandler.INSTANCE;
    } else if ("hmac".equalsIgnoreCase(tokenHandlerType)
        || tokenHandlerType == null
        || tokenHandlerType.trim().isEmpty()) {
      long expirationMs =
          WebTransportConfig.getLong(
              "webtransport4j.quic.token.handler.hmac.expiration.ms", DEFAULT_HMAC_EXPIRATION_MS);
      if (expirationMs <= 0) {
        throw new IllegalArgumentException(
            "webtransport4j.quic.token.handler.hmac.expiration.ms must be > 0");
      }
      String keyHex = WebTransportConfig.get("webtransport4j.quic.token.handler.hmac.key", null);
      if (keyHex != null && !keyHex.trim().isEmpty()) {
        byte[] key = parseHex(keyHex);
        if (key.length < HMAC_KEY_MIN_BYTES) {
          throw new IllegalArgumentException(
              "Configured HMAC key is too short (must be at least "
                  + HMAC_KEY_MIN_BYTES
                  + " bytes / "
                  + (HMAC_KEY_MIN_BYTES * 2)
                  + " hex characters).");
        }
        logger.info(
            "QUIC token handler configured: HMAC with custom key, expiration: {}ms", expirationMs);
        return new HmacQuicTokenHandler(key, expirationMs);
      }
      logger.info(
          "QUIC token handler configured: HMAC with randomly generated key, expiration: {}ms. "
              + "Set webtransport4j.quic.token.handler.hmac.key for multi-instance deployments.",
          expirationMs);
      return new HmacQuicTokenHandler(expirationMs);
    } else {
      try {
        Class<?> tokenClass = Class.forName(tokenHandlerType);
        if (!QuicTokenHandler.class.isAssignableFrom(tokenClass)) {
          throw new IllegalArgumentException(
              tokenHandlerType + " does not implement " + QuicTokenHandler.class.getName());
        }
        logger.info("QUIC token handler configured: custom class {}", tokenHandlerType);
        return (QuicTokenHandler) tokenClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to load custom QuicTokenHandler: " + tokenHandlerType, e);
      }
    }
  }

  private static byte[] parseHex(@NonNull String hex) {
    String normalized = hex.trim();
    if (normalized.isEmpty() || (normalized.length() % 2) != 0) {
      throw new IllegalArgumentException(
          "HMAC key must be a non-empty even-length hex string, got length " + normalized.length());
    }
    byte[] data = new byte[normalized.length() / 2];
    for (int i = 0; i < normalized.length(); i += 2) {
      int high = Character.digit(normalized.charAt(i), 16);
      int low = Character.digit(normalized.charAt(i + 1), 16);
      if (high == -1 || low == -1) {
        throw new IllegalArgumentException(
            "HMAC key contains a non-hex character at index " + i);
      }
      data[i / 2] = (byte) ((high << 4) + low);
    }
    return data;
  }

  /** Validate Config. */
  public static void validateConfig(
      long quicMaxBidi,
      long wtMaxBidi,
      long quicMaxUni,
      long wtMaxUni,
      long quicMaxData,
      long wtMaxData) {
    if (quicMaxBidi < 0 || wtMaxBidi < 0 || quicMaxUni < 0 || wtMaxUni < 0
        || quicMaxData < 0 || wtMaxData < 0) {
      throw new IllegalArgumentException("Flow-control limits must be >= 0");
    }
    if (quicMaxBidi < wtMaxBidi) {
      throw new IllegalArgumentException(
          "Configuration mismatch: quic.max.streams.bidi ("
              + quicMaxBidi
              + ") must be greater than or equal to webtransport.initial.max.streams.bidi ("
              + wtMaxBidi
              + ")");
    }
    if (quicMaxUni < wtMaxUni) {
      throw new IllegalArgumentException(
          "Configuration mismatch: quic.max.streams.uni ("
              + quicMaxUni
              + ") must be greater than or equal to webtransport.initial.max.streams.uni ("
              + wtMaxUni
              + ")");
    }
    if (quicMaxData < wtMaxData) {
      throw new IllegalArgumentException(
          "Configuration mismatch: quic.initial.max.data ("
              + quicMaxData
              + ") must be greater than or equal to webtransport.initial.max.data ("
              + wtMaxData
              + ")");
    }
  }

  /** Validate Epoll UDP GSO size. */
  public static void validateGsoSize(int gsoSize) {
    if (gsoSize < 1 || gsoSize > 64) {
      throw new IllegalArgumentException("webtransport4j.epoll.gso.size must be in range 1 - 64");
    }
  }

  @Override
  public String toString() {
    return "WebTransportServer{state="
        + state.get()
        + ", host="
        + getHost()
        + ", port="
        + getPort()
        + ", sessions="
        + getActiveSessionCount()
        + "}";
  }
}