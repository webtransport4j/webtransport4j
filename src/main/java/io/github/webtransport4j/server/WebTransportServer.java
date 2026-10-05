package io.github.webtransport4j.server;

import io.github.webtransport4j.api.AsyncWebTransportMetricsListener;
import io.github.webtransport4j.api.NoOpWebTransportMetricsListener;
import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.ReactiveWebTransportHandlerAdapter;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.internal.EventLoopSafety;
import io.github.webtransport4j.security.ClientAuthMode;
import io.github.webtransport4j.security.OriginValidator;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
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
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.EpollQuicUtils;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicCongestionControlAlgorithm;
import io.netty.handler.codec.quic.QuicConnectionIdGenerator;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicSslEngine;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicTokenHandler;
import io.netty.handler.codec.quic.SslSessionTicketKey;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main WebTransport server managing QUIC connections.
 *
 * <p>{@link #stop()} is restartable. {@link #close()} is terminal ({@link ServerState#CLOSED}).
 * Owned business-executor lifecycle:
 *
 * <pre>
 *   Sequence                    Final state   Owned executor
 *   new → stop()                STOPPED       shut down (recreated on next start)
 *   new → close()               CLOSED        shut down
 *   start → stop                STOPPED       shut down (recreated on next start)
 *   start → stop → start        STARTED       fresh owned executor
 *   start → close               CLOSED        shut down
 *   stop in progress → close    CLOSED        shut down (close waits for in-flight stop)
 *   close → start               exception     remains shut down
 *   close → close               CLOSED        idempotent
 * </pre>
 *
 * <p>Startup resources (event-loop group, bound channel, TLS watcher, shutdown hook, generated
 * certificate, and dynamically created traffic shaper) stay local until {@code
 * publishIfStillStarting}. If {@link #stop()} wins first, it returns {@code STOPPED} before those
 * objects exist; {@code doStart} then discards them itself. The epoch is not enough on its own — it
 * only prevents publishing.
 *
 * <p>The QUIC codec does not snapshot {@link QuicSslContext}. New handshakes call {@link
 * #newQuicSslEngine(QuicChannel)}, which reads the live {@link #activeSslContext} so certificate
 * hot-reload actually affects new connections. Existing QUIC connections keep the engine they were
 * created with.
 */
public class WebTransportServer implements AutoCloseable {

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
  private static final int DEFAULT_MAX_MESSAGES_PER_READ = 64;
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
  private QuicConnectionIdGenerator connectionIdGenerator;
  private String transportType;
  private Long idleTimeoutSeconds;
  private Long initialMaxStreamsBidi;
  private Long initialMaxStreamsUni;
  private Long initialMaxData;
  private ClientAuthMode clientAuthMode;
  private File trustCertFile;
  private X509Certificate[] trustCertificates;
  private TrustManagerFactory trustManagerFactory;
  private TrustManager trustManager;
  private OriginValidator originValidator;
  private Boolean strictOriginValidation;

  private final Map<String, WebTransportHandler> handlers = new ConcurrentHashMap<>();
  private volatile WebTransportHandler defaultHandler;
  private final Set<WebTransportSession> activeSessionsSet = ConcurrentHashMap.newKeySet();
  private final Map<Long, WebTransportSession> activeSessionsById = new ConcurrentHashMap<>();
  private final Map<Long, WebTransportSession> activeSessionsMap = new ConcurrentHashMap<>();

  private final AtomicInteger globalActiveSessions = new AtomicInteger(0);
  private final AtomicInteger globalSessionSlots = new AtomicInteger(0);
  private final AtomicBoolean draining = new AtomicBoolean(false);
  private final Set<QuicChannel> activeQuicChannels = ConcurrentHashMap.newKeySet();

  private volatile WebTransportMetricsListener metricsListener =
      NoOpWebTransportMetricsListener.INSTANCE;

  private volatile Supplier<MessageDispatcher> messageDispatcherSupplier =
      () -> DefaultMessageDispatcher.INSTANCE;
  private volatile ExecutorService businessExecutor;
  private final boolean ownsBusinessExecutor;

  private volatile GlobalTrafficShapingHandler trafficShaper;
  // Caller-supplied shapers survive restartable stop(); dynamically created shapers do not,
  // because they are tied to the event-loop group used for that server run.
  private volatile boolean trafficShaperExternallySupplied;
  private Long configuredGlobalWriteLimit;
  private Long configuredGlobalReadLimit;

  // Weak keys remember transferred handlers without keeping released handlers alive.
  private static final Map<GlobalTrafficShapingHandler, Boolean> OWNED_TRAFFIC_SHAPERS =
      new WeakHashMap<>();
  private static final Object SERVER_INSTANCES_LOCK = new Object();
  private static final AtomicInteger ACTIVE_SERVER_INSTANCES = new AtomicInteger(0);

  /** Guarded by {@link #SERVER_INSTANCES_LOCK}. */
  private boolean instanceCounted;

  /** Lifecycle states of the WebTransport server. */
  public enum ServerState {
    STOPPED,
    STARTING,
    STARTED,
    DRAINING,
    STOPPING,
    /** Terminal after {@link #close()}. {@link #start()} will fail. */
    CLOSED
  }

  private final Object lifecycleLock = new Object();
  private final java.util.concurrent.Semaphore sessionClosures =
      new java.util.concurrent.Semaphore(0);
  private final Set<QuicChannel> connections = ConcurrentHashMap.newKeySet();

  void registerConnection(QuicChannel connection) {
    connections.add(connection);
    connection.closeFuture().addListener(f -> connections.remove(connection));
    if (!isAcceptingSessions()) {
      drainConnection(connection);
    }
  }

  private void drainConnection(QuicChannel connection) {
    if (!connection.isOpen()) {
      return;
    }
    connection.attr(WebTransportAttributeKeys.CONNECTION_DRAINING).set(true);
    try {
      connection
          .eventLoop()
          .execute(
              () -> {
                QuicStreamChannel control = Http3.getLocalControlStream(connection);
                if (control != null && control.isActive()) {
                  control.writeAndFlush(
                      new io.netty.handler.codec.http3.DefaultHttp3GoAwayFrame(
                          0x3ffffffffffffffcL));
                }
              });
    } catch (java.util.concurrent.RejectedExecutionException closedLoop) {
      logger.debug("Connection event loop already stopped during drain", closedLoop);
    }
  }

  private final AtomicReference<ServerState> state = new AtomicReference<>(ServerState.STOPPED);

  /** Incremented by {@link #stop} to invalidate an in-flight {@link #start}. */
  private final AtomicLong startEpoch = new AtomicLong();

  /** Sticky: once {@link #close()} runs, restart is forbidden even if stop is still in flight. */
  private volatile boolean permanentlyClosed;

  private EventLoopGroup group;
  private Channel channel;
  private Thread shutdownHook;
  private volatile QuicSslContext activeSslContext;
  private volatile TlsCertificateWatcher tlsWatcher;
  private SelfSignedCertificate generatedCertificate;

  /**
   * Returns the SSL context used for <em>new</em> QUIC handshakes. Existing connections keep the
   * engine created at accept time. Returns null after shutdown and before a context has been built
   * for the current server run.
   */
  public @Nullable QuicSslContext getActiveSslContext() {
    return activeSslContext;
  }

  /**
   * Checks for certificate modifications and hot-reloads if changes are detected. On success the
   * live handshake context is swapped; in-flight connections are not interrupted.
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
    this.connectionIdGenerator = builder.getConnectionIdGenerator();
    this.transportType = builder.getTransportType();
    this.idleTimeoutSeconds = builder.getIdleTimeoutSeconds();
    this.initialMaxStreamsBidi = builder.getInitialMaxStreamsBidi();
    this.initialMaxStreamsUni = builder.getInitialMaxStreamsUni();
    this.initialMaxData = builder.getInitialMaxData();
    GlobalTrafficShapingHandler builderTrafficShaper = builder.getTrafficShaper();
    this.trafficShaper = claimTrafficShaper(builderTrafficShaper);
    this.trafficShaperExternallySupplied = builderTrafficShaper != null;
    this.configuredGlobalWriteLimit = builder.getGlobalTrafficWriteLimit();
    this.configuredGlobalReadLimit = builder.getGlobalTrafficReadLimit();
    this.clientAuthMode = builder.getClientAuthMode();
    this.trustCertFile = builder.getTrustCertFile();
    this.trustCertificates = builder.getTrustCertificates();
    this.trustManagerFactory = builder.getTrustManagerFactory();
    this.trustManager = builder.getTrustManager();
    this.originValidator = builder.getOriginValidator();
    this.strictOriginValidation = builder.getStrictOriginValidation();

    if (builder.getMetricsListener() != null) {
      this.metricsListener = isolateMetricsListener(builder.getMetricsListener());
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

  private static @NonNull WebTransportHandler requireHandler(
      @Nullable WebTransportHandler handler) {
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
    EventLoopSafety.requireBlockingAllowed();
    synchronized (this) {
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
    EventLoopSafety.requireBlockingAllowed();
    WebTransportMetricsListener previous = this.metricsListener;
    this.metricsListener = isolateMetricsListener(Objects.requireNonNull(listener, "listener"));
    closeMetricsListener(previous);
  }

  public @NonNull WebTransportMetricsListener getMetricsListener() {
    return metricsListener;
  }

  private static @NonNull WebTransportMetricsListener isolateMetricsListener(
      @NonNull WebTransportMetricsListener listener) {
    if (listener == NoOpWebTransportMetricsListener.INSTANCE
        || listener instanceof AsyncWebTransportMetricsListener) {
      return listener;
    }
    return new AsyncWebTransportMetricsListener(listener);
  }

  private static void closeMetricsListener(@Nullable WebTransportMetricsListener listener) {
    if (listener instanceof AsyncWebTransportMetricsListener) {
      ((AsyncWebTransportMetricsListener) listener).close();
    }
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
   * Sets the traffic shaping handler while this server is stopped, transferring exclusive
   * ownership. A handler previously transferred to a server cannot be reused. The caller remains
   * responsible for releasing a handler replaced before startup.
   *
   * @throws IllegalStateException if the server is not stopped or the handler was already
   *     transferred
   */
  public void setTrafficShaper(@Nullable GlobalTrafficShapingHandler trafficShaper) {
    EventLoopSafety.requireBlockingAllowed();
    synchronized (lifecycleLock) {
      if (state.get() != ServerState.STOPPED) {
        throw new IllegalStateException(
            "Traffic shaper can only be replaced while the server is stopped");
      }
      if (this.trafficShaper != trafficShaper) {
        this.trafficShaper = claimTrafficShaper(trafficShaper);
        this.trafficShaperExternallySupplied = trafficShaper != null;
      }
    }
  }

  private static GlobalTrafficShapingHandler claimTrafficShaper(
      GlobalTrafficShapingHandler handler) {
    if (handler != null) {
      EventLoopSafety.requireBlockingAllowed();
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
    this.trafficShaperExternallySupplied = false;
    if (handler != null) {
      handler.release();
    }
  }

  private void releaseTrafficShaperSafely() {
    try {
      releaseTrafficShaper();
    } catch (RuntimeException e) {
      logger.error("Error releasing global traffic shaper", e);
    }
  }

  private void registerServerInstance() {
    EventLoopSafety.requireBlockingAllowed();
    synchronized (SERVER_INSTANCES_LOCK) {
      if (!instanceCounted) {
        ACTIVE_SERVER_INSTANCES.incrementAndGet();
        instanceCounted = true;
      }
    }
  }

  private void unregisterServerInstance() {
    EventLoopSafety.requireBlockingAllowed();
    synchronized (SERVER_INSTANCES_LOCK) {
      if (!instanceCounted) {
        return;
      }
      instanceCounted = false;
      if (ACTIVE_SERVER_INSTANCES.decrementAndGet() == 0) {
        IpRateLimitingHandler.stopReloader();
      }
    }
  }

  private void unregisterServerInstanceSafely() {
    try {
      unregisterServerInstance();
    } catch (RuntimeException e) {
      logger.error("Error unregistering server instance", e);
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

  /**
   * Returns the executor used for application callbacks. After {@link #stop()} of an owned executor
   * this may be a terminated pool; {@link #start()} replaces it before accepting connections again.
   */
  public ExecutorService getBusinessExecutor() {
    return businessExecutor;
  }

  /** Returns the number of active WebTransport sessions across all QUIC connections. */
  public int getActiveSessionCount() {
    return activeSessionsSet.size();
  }

  /**
   * Registers an active WebTransport session.
   *
   * @param session the session to register
   */
  public void registerSession(@NonNull WebTransportSession session) {
    Objects.requireNonNull(session, "session cannot be null");
    activeSessionsSet.add(session);
    activeSessionsById.put(session.getUniqueSessionId(), session);
    activeSessionsMap.put(session.getSessionStreamId(), session);
    if (state.get() == ServerState.DRAINING || state.get() == ServerState.STOPPING) {
      session.drain();
    }
  }

  /**
   * Unregisters an active WebTransport session.
   *
   * @param session the session to unregister
   */
  public void unregisterSession(@NonNull WebTransportSession session) {
    Objects.requireNonNull(session, "session cannot be null");
    activeSessionsSet.remove(session);
    activeSessionsById.remove(session.getUniqueSessionId());
    activeSessionsMap.remove(session.getSessionStreamId(), session);
    sessionClosures.release();
  }

  /**
   * Unregisters an active WebTransport session by its session stream ID or unique session ID.
   *
   * @param sessionId the stream ID or unique ID of the session
   */
  public void unregisterSession(long sessionId) {
    WebTransportSession removed = activeSessionsById.remove(sessionId);
    if (removed != null) {
      activeSessionsSet.remove(removed);
      activeSessionsMap.remove(removed.getSessionStreamId(), removed);
      sessionClosures.release();
      return;
    }
    removed = activeSessionsMap.remove(sessionId);
    if (removed != null) {
      activeSessionsSet.remove(removed);
      activeSessionsById.remove(removed.getUniqueSessionId());
      sessionClosures.release();
      return;
    }
    for (WebTransportSession s : activeSessionsSet) {
      if (s.getUniqueSessionId() == sessionId
          || s.getSessionStreamId() == sessionId
          || Math.abs((long) System.identityHashCode(s)) == sessionId) {
        activeSessionsSet.remove(s);
        activeSessionsById.remove(s.getUniqueSessionId());
        activeSessionsMap.remove(s.getSessionStreamId(), s);
        sessionClosures.release();
        break;
      }
    }
  }

  /**
   * Returns an unmodifiable collection of all currently active WebTransport sessions.
   *
   * @return collection of active sessions
   */
  public @NonNull Collection<WebTransportSession> getActiveSessions() {
    return Collections.unmodifiableCollection(activeSessionsSet);
  }

  /**
   * Retrieves an active WebTransport session by its unique session ID or session stream ID.
   *
   * @param sessionId the stream ID or unique session ID of the session
   * @return the session, or null if not found
   */
  public @Nullable WebTransportSession getSession(long sessionId) {
    WebTransportSession session = activeSessionsById.get(sessionId);
    if (session != null) {
      return session;
    }
    session = activeSessionsMap.get(sessionId);
    if (session != null) {
      return session;
    }
    for (WebTransportSession s : activeSessionsSet) {
      if (s.getUniqueSessionId() == sessionId
          || s.getSessionStreamId() == sessionId
          || Math.abs((long) System.identityHashCode(s)) == sessionId) {
        return s;
      }
    }
    return null;
  }

  /** Returns the current lifecycle state of the server. */
  public ServerState getState() {
    return state.get();
  }

  /** Returns true if the server is active and listening. */
  public boolean isStarted() {
    Channel ch = this.channel;
    return (state.get() == ServerState.STARTED || state.get() == ServerState.DRAINING)
        && ch != null
        && ch.isActive();
  }

  /** Returns whether the server admits new WebTransport sessions. */
  public boolean isAcceptingSessions() {
    return state.get() == ServerState.STARTED;
  }

  /** Stops admitting sessions and notifies existing sessions without closing their streams. */
  public void drain() {
    if (!state.compareAndSet(ServerState.STARTED, ServerState.DRAINING)) {
      return;
    }
    for (QuicChannel connection : connections) {
      drainConnection(connection);
    }
    drainActiveSessions();
  }

  private void drainActiveSessions() {
    for (WebTransportSession session : activeSessionsSet) {
      try {
        if (session.isOpen()) {
          session.drain();
        }
      } catch (RuntimeException failure) {
        logger.warn("Could not notify session of shutdown", failure);
      }
    }
  }

  /** Returns true if the server is active and listening. */
  public boolean isRunning() {
    return isStarted();
  }

  /** Returns true if the server is in a coordinated draining phase. */
  public boolean isDraining() {
    return draining.get();
  }

  /** Returns the number of active WebTransport sessions across all QUIC connections. */
  public int getActiveSessionsCount() {
    return getActiveSessionCount();
  }

  void registerQuicChannel(@Nullable QuicChannel ch) {
    if (ch != null) {
      activeQuicChannels.add(ch);
    }
  }

  void unregisterQuicChannel(@Nullable QuicChannel ch) {
    if (ch != null) {
      activeQuicChannels.remove(ch);
    }
  }

  /**
   * Enters the coordinated draining phase per WebTransport Draft-16.
   *
   * <p>Broadcasts {@code WT_DRAIN_SESSION} capsules to all connected clients and marks sessions
   * as draining. Waits up to the specified timeout for sessions to close cleanly before stopping.
   *
   * @param timeout maximum time to wait for sessions to drain
   * @param unit the time unit of the timeout argument
   */
  public void drain(long timeout, @NonNull TimeUnit unit) {
    Objects.requireNonNull(unit, "unit");
    if (!draining.compareAndSet(false, true)) {
      return;
    }
    logger.info("Initiating graceful WebTransport server drain (timeout: {} {})", timeout, unit);

    for (QuicChannel qch : activeQuicChannels) {
      if (qch.isActive()) {
        WebTransportSessionManager mgr = qch.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
        if (mgr != null) {
          for (WebTransportSession s : mgr.getSessions()) {
            s.markDraining();
            if (s instanceof DefaultWebTransportSession) {
              QuicStreamChannel connectStream = ((DefaultWebTransportSession) s).getConnectStream();
              if (connectStream != null && connectStream.isActive()) {
                ByteBuf buf = qch.alloc().buffer(8);
                WebTransportUtils.writeVarInt(buf, 0x78aeL);
                WebTransportUtils.writeVarInt(buf, 0L);
                connectStream.writeAndFlush(new DefaultHttp3DataFrame(buf));
              }
            }
          }
        }
      }
    }

    long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
    while (globalActiveSessions.get() > 0 && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    stop(5, TimeUnit.SECONDS);
  }

  /**
   * Programmatically reloads the TLS certificate context using updated PEM files.
   *
   * @param certFile the new certificate chain file
   * @param keyFile the new private key file
   * @throws Exception if reading the files or building the QuicSslContext fails
   */
  public void reloadTlsCertificate(@NonNull File certFile, @NonNull File keyFile) throws Exception {
    Objects.requireNonNull(certFile, "certFile");
    Objects.requireNonNull(keyFile, "keyFile");
    QuicSslContext newContext =
        QuicSslContextBuilder.forServer(keyFile, null, certFile)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .earlyData(true)
            .build();
    installReloadedSslContext(newContext);
  }

  /**
   * Programmatically installs an already configured {@link QuicSslContext}.
   *
   * @param newContext the new QUIC SSL context
   */
  public void reloadTlsCertificate(@NonNull QuicSslContext newContext) {
    installReloadedSslContext(newContext);
  }

  /**
   * Returns the configured mTLS client authentication mode.
   *
   * @return client authentication mode, or {@code null} if default
   */
  public @Nullable ClientAuthMode getClientAuthMode() {
    return clientAuthMode;
  }

  /**
   * Returns the custom origin validator, if configured.
   *
   * @return origin validator, or {@code null} if none
   */
  public @Nullable OriginValidator getOriginValidator() {
    return originValidator;
  }

  /**
   * Returns {@code true} if strict origin validation is enforced.
   *
   * @return true if strict origin validation is active
   */
  public boolean isStrictOriginValidation() {
    return strictOriginValidation != null
        ? strictOriginValidation
        : WebTransportConfig.getBoolean("webtransport4j.security.strict_origin", false);
  }

  /**
   * Starts the WebTransport server non-blockingly. Returns immediately once the server channel is
   * bound. Concurrent calls while already starting or started are ignored. A concurrent {@link
   * #stop()} wins: unpublished startup resources are discarded locally even if {@code stop()}
   * already returned. {@link #close()} is terminal and a later {@code start()} throws.
   */
  public void start() throws Exception {
    EventLoopSafety.requireBlockingAllowed();
    IpRateLimitingHandler.ensureReloaderStarted();
    if (permanentlyClosed || state.get() == ServerState.CLOSED) {
      throw new IllegalStateException("WebTransportServer has been closed and cannot be restarted");
    }
    if (!state.compareAndSet(ServerState.STOPPED, ServerState.STARTING)) {
      ServerState current = state.get();
      if (current == ServerState.STARTED || current == ServerState.STARTING) {
        logger.warn(
            "!!! WARNING !!! Server is already {} on port {} !!! WARNING !!!",
            current.name().toLowerCase(Locale.ROOT),
            getPort());
        return;
      }
      throw new IllegalStateException("Cannot start WebTransportServer while in state: " + current);
    }
    if (permanentlyClosed) {
      state.compareAndSet(ServerState.STARTING, ServerState.STOPPED);
      throw new IllegalStateException("WebTransportServer has been closed and cannot be restarted");
    }

    final long epoch = startEpoch.incrementAndGet();
    registerServerInstance();
    try {
      doStart(epoch);
    } catch (Exception e) {
      cleanupFailedStart(e);
      throw e;
    } catch (Error e) {
      cleanupFailedStart(e);
      throw e;
    }
  }

  private void cleanupFailedStart(Throwable failure) {
    try {
      stop(5, TimeUnit.SECONDS, true, false);
    } catch (RuntimeException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
  }

  private static final class SslContextBuildResult {
    final QuicSslContext context;
    final @Nullable SelfSignedCertificate generatedCertificate;

    SslContextBuildResult(
        QuicSslContext context, @Nullable SelfSignedCertificate generatedCertificate) {
      this.context = context;
      this.generatedCertificate = generatedCertificate;
    }
  }

  private void doStart(long epoch) throws Exception {
    if (defaultHandler == null) {
      throw new IllegalStateException(
          "Server cannot start without a registered default path handler.");
    }
    abortIfStartInvalidated(epoch);

    final int targetPort = resolvePort();
    final String targetHost = resolveHost();
    List<String> resolvedOrigins = resolveAllowedOrigins();

    if (logger.isDebugEnabled()) {
      logger.debug("Starting WebTransport server on {}:{}", targetHost, targetPort);
    }

    Bootstrap bootstrap = new Bootstrap();
    String resolvedTransport =
        this.transportType != null
            ? this.transportType
            : WebTransportConfig.get("webtransport4j.server.transport", "auto");
    TransportConfig transportConfig = resolveTransport(resolvedTransport, bootstrap);

    abortIfStartInvalidated(epoch);

    EventLoopGroup newGroup = null;
    Channel newChannel = null;
    TlsCertificateWatcher newWatcher = null;
    GlobalTrafficShapingHandler createdShaper = null;
    SelfSignedCertificate generatedCert = null;
    ExecutorService createdExecutor = null;
    QuicSslContext previousActiveSslContext = this.activeSslContext;
    boolean startupSslContextInstalled = false;
    boolean published = false;
    try {
      newGroup =
          new MultiThreadIoEventLoopGroup(
              Math.max(1, Runtime.getRuntime().availableProcessors()),
              transportConfig.ioHandlerFactory);

      abortIfStartInvalidated(epoch);

      ExecutorService previousExecutor = this.businessExecutor;
      ExecutorService executor = ensureBusinessExecutor();
      if (ownsBusinessExecutor && executor != previousExecutor) {
        createdExecutor = executor;
      }

      GlobalTrafficShapingHandler preexistingShaper = this.trafficShaper;
      GlobalTrafficShapingHandler shaper = resolveTrafficShaper(newGroup);
      if (preexistingShaper == null && shaper != null) {
        createdShaper = shaper;
      }

      SslContextBuildResult sslResult = buildSslContext();
      QuicSslContext sslCtx = sslResult.context;
      generatedCert = sslResult.generatedCertificate;
      this.activeSslContext = sslCtx;
      startupSslContextInstalled = true;

      abortIfStartInvalidated(epoch);

      newWatcher = maybeStartTlsWatcher();

      Http3Settings settings = buildHttp3Settings();
      ResolvedLimits limits = resolveLimits();

      // sslEngineProvider reads activeSslContext on every new handshake. Do not call
      // sslContext(sslCtx): that captures the instance and makes hot-reload a no-op.
      QuicServerCodecBuilder codecBuilder =
          Http3.newQuicServerCodecBuilder()
              .sslEngineProvider(this::newQuicSslEngine)
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
                      executor,
                      resolvedOrigins,
                      globalActiveSessions,
                      globalSessionSlots));

      QuicConnectionIdGenerator cidGen = resolveConnectionIdGenerator();
      if (cidGen != null) {
        codecBuilder.connectionIdAddressGenerator(cidGen);
      }

      configureOptionalQuicParams(codecBuilder);

      abortIfStartInvalidated(epoch);

      ChannelHandler serverCodec = codecBuilder.build();
      newChannel =
          bindServer(
              bootstrap, transportConfig, serverCodec, targetHost, targetPort, newGroup, shaper);

      published =
          publishIfStillStarting(epoch, newGroup, newChannel, newWatcher, shaper, generatedCert);
      if (!published) {
        throw new IllegalStateException("Server start aborted because shutdown was requested");
      }
      logger.info(
          "WebTransport server started on {}:{}", getHostFrom(newChannel), getPortFrom(newChannel));
    } finally {
      if (!published) {
        discardUnpublishedStart(
            newChannel,
            newGroup,
            newWatcher,
            createdShaper,
            generatedCert,
            createdExecutor,
            previousActiveSslContext,
            startupSslContextInstalled);
      }
    }
  }

  private void abortIfStartInvalidated(long epoch) {
    if (permanentlyClosed || epoch != startEpoch.get() || state.get() != ServerState.STARTING) {
      throw new IllegalStateException("Server start aborted because shutdown was requested");
    }
  }

  /**
   * Publishes startup resources only if this start attempt still owns the lifecycle. The shutdown
   * hook is registered here so a concurrent {@link #stop()} that already finished cannot leak a
   * hook {@code doStart} created after it returned.
   */
  private boolean publishIfStillStarting(
      long epoch,
      EventLoopGroup newGroup,
      Channel newChannel,
      TlsCertificateWatcher newWatcher,
      GlobalTrafficShapingHandler shaper,
      SelfSignedCertificate generatedCert) {
    Thread hook =
        new Thread(
            () -> {
              logger.info("Shutdown hook triggered. Closing server...");
              close();
            },
            "webtransport-server-shutdown-hook");
    hook.setDaemon(false);

    EventLoopSafety.requireBlockingAllowed();

    synchronized (lifecycleLock) {
      if (permanentlyClosed || epoch != startEpoch.get() || state.get() != ServerState.STARTING) {
        return false;
      }
      try {
        Runtime.getRuntime().addShutdownHook(hook);
      } catch (IllegalStateException e) {
        return false;
      }
      this.shutdownHook = hook;
      this.group = newGroup;
      this.channel = newChannel;
      this.tlsWatcher = newWatcher;
      this.trafficShaper = shaper;
      if (generatedCert != null) {
        this.generatedCertificate = generatedCert;
      }
      state.set(ServerState.STARTED);
      return true;
    }
  }

  /**
   * Always runs for an unpublished start. {@link #stop()} may already have returned {@code STOPPED}
   * before these locals existed, so cleanup cannot be delegated to {@code stop()}.
   */
  private void discardUnpublishedStart(
      @Nullable Channel newChannel,
      @Nullable EventLoopGroup newGroup,
      @Nullable TlsCertificateWatcher newWatcher,
      @Nullable GlobalTrafficShapingHandler createdShaper,
      @Nullable SelfSignedCertificate generatedCert,
      @Nullable ExecutorService createdExecutor,
      @Nullable QuicSslContext previousActiveSslContext,
      boolean startupSslContextInstalled) {
    closeQuietly(newChannel);
    shutdownGroupQuietly(newGroup);
    stopTlsWatcherSafely(newWatcher);
    if (createdShaper != null) {
      try {
        createdShaper.release();
      } catch (Exception e) {
        logger.debug("Failed to release unpublished traffic shaper", e);
      }
      if (this.trafficShaper == createdShaper) {
        this.trafficShaper = null;
      }
    }
    if (generatedCert != null) {
      try {
        generatedCert.delete();
      } catch (Exception e) {
        logger.debug("Failed to delete unpublished generated certificate", e);
      }
      if (this.generatedCertificate == generatedCert) {
        this.generatedCertificate = null;
      }
    }
    if (createdExecutor != null) {
      try {
        createdExecutor.shutdownNow();
      } catch (RuntimeException e) {
        logger.error("Error shutting down unpublished business executor", e);
      }
    }
    if (startupSslContextInstalled) {
      this.activeSslContext = previousActiveSslContext;
    }
    if (this.group == newGroup) {
      this.group = null;
    }
    if (this.tlsWatcher == newWatcher) {
      this.tlsWatcher = null;
    }
    if (this.channel == newChannel) {
      this.channel = null;
    }
  }

  private static void shutdownGroupQuietly(@Nullable EventLoopGroup g) {
    if (g != null && !g.isShuttingDown() && !g.isShutdown()) {
      try {
        g.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception ignored) {
        // best-effort rollback of a start that never published
      }
    }
  }

  /** Starts the server non-blockingly and then blocks until server shutdown. */
  public void startAndAwait() throws Exception {
    start();
    awaitShutdown();
  }

  /** Blocks the current thread until the server channel is closed. */
  public void awaitShutdown() throws InterruptedException {
    EventLoopSafety.requireBlockingAllowed();
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
          "webtransport4j.server.port must be between "
              + MIN_PORT
              + " and "
              + MAX_PORT
              + ": "
              + port);
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
    if (!devMode && resolvedOrigins.contains("*")) {
      logger.warn(
          "!!! WARNING !!! Allowed origins is '*'. This accepts any browser origin. Set"
              + " webtransport4j.allowed.origins to an explicit allow-list in production. !!!"
              + " WARNING !!!");
    }
    return resolvedOrigins;
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

    if (this.sslContext != null) {
      // Caller injected a QuicSslContext and owns rotation. Watching files would fight that.
      return null;
    }

    if (hotReloadEnabled && resolvedKeyPath != null && resolvedCertPath != null) {
      long epoch = startEpoch.get();
      TlsCertificateWatcher watcher =
          new TlsCertificateWatcher(
              resolvedKeyPath,
              resolvedCertPath,
              context -> {
                EventLoopSafety.requireBlockingAllowed();
                synchronized (lifecycleLock) {
                  if (startEpoch.get() == epoch) {
                    installReloadedSslContext(context);
                  }
                }
              },
              this::applyClientAuthAndTrust,
              WebTransportConfig.getInt("webtransport4j.ssl.hot_reload.interval_secs", 5));
      watcher.start();
      return watcher;
    }
    return null;
  }

  /**
   * Creates a {@link QuicSslEngine} for a new QUIC connection from the live {@link
   * #activeSslContext}. Invoked by the codec on every handshake, so a hot-reloaded context is
   * picked up without rebuilding the server codec.
   */
  private QuicSslEngine newQuicSslEngine(QuicChannel channel) {
    QuicSslContext ctx = this.activeSslContext;
    if (ctx == null) {
      throw new IllegalStateException("No active QUIC SSL context");
    }
    return ctx.newEngine(channel.alloc());
  }

  /**
   * Installs a context built by {@link TlsCertificateWatcher}. Session ticket keys are re-applied
   * so 1-RTT resumption keeps working after rotation. Existing connections retain their original
   * engine.
   */
  private void installReloadedSslContext(QuicSslContext newCtx) {
    Objects.requireNonNull(newCtx, "newCtx");
    EventLoopSafety.requireBlockingAllowed();
    synchronized (lifecycleLock) {
      if (permanentlyClosed || state.get() == ServerState.STOPPING) {
        return;
      }
      applySessionTicketKeys(newCtx);
      this.activeSslContext = newCtx;
    }
    logger.info(
        "TLS certificate context reloaded; new handshakes will use the updated certificate");
  }

  /**
   * Returns a live business executor, allocating a new owned pool if {@link #stop()} terminated the
   * previous one. Caller-supplied executors are never replaced: a shutdown pool fails start-up.
   */
  private ExecutorService ensureBusinessExecutor() {
    ExecutorService current = this.businessExecutor;
    if (isLive(current)) {
      return current;
    }
    if (!ownsBusinessExecutor) {
      throw new IllegalStateException(
          "Caller-supplied business executor is shutdown; supply a live executor or omit it so the"
              + " server can manage one");
    }
    EventLoopSafety.requireBlockingAllowed();
    synchronized (lifecycleLock) {
      current = this.businessExecutor;
      if (isLive(current)) {
        return current;
      }
      current = BusinessExecutorFactory.create();
      this.businessExecutor = current;
      logger.info("Allocated a new business executor for server restart");
      return current;
    }
  }

  private static boolean isLive(@Nullable ExecutorService executor) {
    return executor != null && !executor.isShutdown() && !executor.isTerminated();
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
    String requested = transportType == null ? "auto" : transportType.trim();
    if (requested.isEmpty()) {
      throw new IllegalArgumentException("Transport type must not be empty");
    }

    boolean auto = "auto".equalsIgnoreCase(requested);
    boolean requestIoUring = "iouring".equalsIgnoreCase(requested);
    boolean requestEpoll = "epoll".equalsIgnoreCase(requested);
    boolean requestKqueue = "kqueue".equalsIgnoreCase(requested);
    boolean requestNio = "nio".equalsIgnoreCase(requested);
    if (!auto && !requestIoUring && !requestEpoll && !requestKqueue && !requestNio) {
      throw new IllegalArgumentException("Unknown transport type: " + requested);
    }

    IoHandlerFactory ioHandlerFactory = null;
    Class<? extends Channel> channelClass = null;
    boolean epollGroEnabled = false;

    if (auto || requestIoUring) {
      try {
        Class<?> ioUringClass = Class.forName("io.netty.channel.uring.IOUring");
        Method isAvailableMethod = ioUringClass.getMethod("isAvailable");
        boolean isAvailable = (boolean) isAvailableMethod.invoke(null);
        if (!isAvailable) {
          if (requestIoUring) {
            throw new IllegalStateException(
                "IOUring transport was explicitly requested but is not available");
          }
        } else {
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
      } catch (ReflectiveOperationException e) {
        rethrowFatalInvocationTarget(e);
        if (requestIoUring) {
          throw unavailableTransport("IOUring", e);
        }
        logger.debug("IOUring is not available (not on classpath or not supported by OS).", e);
      } catch (LinkageError | SecurityException e) {
        if (requestIoUring) {
          throw unavailableTransport("IOUring", e);
        }
        logger.debug("IOUring is not available (not on classpath or not supported by OS).", e);
      }
    }

    if (ioHandlerFactory == null && (auto || requestEpoll)) {
      try {
        Class<?> epollClass = Class.forName("io.netty.channel.epoll.Epoll");
        Class<?> epollOptionClass = Class.forName("io.netty.channel.epoll.EpollChannelOption");
        Method isAvailableMethod = epollClass.getMethod("isAvailable");
        boolean isAvailable = (boolean) isAvailableMethod.invoke(null);
        if (!isAvailable) {
          if (requestEpoll) {
            throw new IllegalStateException(
                "Epoll transport was explicitly requested but is not available");
          }
        } else {
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
            bootstrap.option(udpGroOption, true);
          }

          boolean udpGso = WebTransportConfig.getBoolean("webtransport4j.epoll.udpgso", true);
          if (udpGso && !WebTransportUtils.isLinuxUdpGsoSupported()) {
            udpGso = false;
          }
          if (udpGso) {
            int gsoSize =
                WebTransportConfig.getInt("webtransport4j.epoll.gso.size", DEFAULT_GSO_SIZE);
            // Configuration errors are intentionally outside any broad runtime catch and fail
            // startup.
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
      } catch (ReflectiveOperationException e) {
        rethrowFatalInvocationTarget(e);
        if (requestEpoll) {
          throw unavailableTransport("Epoll", e);
        }
        logger.debug("Epoll is not available.", e);
      } catch (LinkageError | SecurityException e) {
        if (requestEpoll) {
          throw unavailableTransport("Epoll", e);
        }
        logger.debug("Epoll is not available.", e);
      }
    }

    if (ioHandlerFactory == null && (auto || requestKqueue)) {
      try {
        Class<?> kqueueClass = Class.forName("io.netty.channel.kqueue.KQueue");
        Method isAvailableMethod = kqueueClass.getMethod("isAvailable");
        boolean isAvailable = (boolean) isAvailableMethod.invoke(null);
        if (!isAvailable) {
          if (requestKqueue) {
            throw new IllegalStateException(
                "KQueue transport was explicitly requested but is not available");
          }
        } else {
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
      } catch (ReflectiveOperationException e) {
        rethrowFatalInvocationTarget(e);
        if (requestKqueue) {
          throw unavailableTransport("KQueue", e);
        }
        logger.debug("KQueue is not available.", e);
      } catch (LinkageError | SecurityException e) {
        if (requestKqueue) {
          throw unavailableTransport("KQueue", e);
        }
        logger.debug("KQueue is not available.", e);
      }
    }

    if (ioHandlerFactory == null) {
      if (!auto && !requestNio) {
        // Explicit native selections never silently degrade to NIO.
        throw new IllegalStateException(
            "Requested transport '" + requested + "' could not be initialized");
      }
      logger.info("Using NIO transport");
      ioHandlerFactory = NioIoHandler.newFactory();
      channelClass = NioDatagramChannel.class;
    }

    return new TransportConfig(ioHandlerFactory, channelClass, epollGroEnabled);
  }

  private static IllegalStateException unavailableTransport(String transport, Throwable cause) {
    return new IllegalStateException(
        transport + " transport was explicitly requested but could not be initialized", cause);
  }

  private static void rethrowFatalInvocationTarget(ReflectiveOperationException error) {
    if (!(error instanceof java.lang.reflect.InvocationTargetException)) {
      return;
    }
    Throwable cause = ((java.lang.reflect.InvocationTargetException) error).getCause();
    if (cause instanceof Error && !(cause instanceof LinkageError)) {
      throw (Error) cause;
    }
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
  }

  private @Nullable GlobalTrafficShapingHandler resolveTrafficShaper(
      EventLoopGroup eventLoopGroup) {
    if (this.trafficShaper != null) {
      return this.trafficShaper;
    }
    long globalWriteLimit =
        configuredGlobalWriteLimit != null
            ? configuredGlobalWriteLimit
            : WebTransportConfig.getLong("webtransport4j.server.traffic.global.write.limit", 0L);
    long globalReadLimit =
        configuredGlobalReadLimit != null
            ? configuredGlobalReadLimit
            : WebTransportConfig.getLong("webtransport4j.server.traffic.global.read.limit", 0L);
    if (globalWriteLimit > 0 || globalReadLimit > 0) {
      return claimTrafficShaper(
          new GlobalTrafficShapingHandler(eventLoopGroup, globalWriteLimit, globalReadLimit));
    }
    return null;
  }

  private @NonNull SslContextBuildResult buildSslContext() throws Exception {
    if (this.sslContext != null) {
      if (clientAuthMode != null
          || resolveClientAuthMode() != ClientAuthMode.NONE
          || trustCertFile != null
          || trustCertificates != null
          || trustManagerFactory != null
          || trustManager != null
          || WebTransportConfig.get("webtransport4j.ssl.trust_cert.path", null) != null) {
        throw new IllegalArgumentException(
            "Configure client authentication and trust on the supplied sslContext; "
                + "server clientAuth/trustManager settings cannot modify it");
      }
      return new SslContextBuildResult(this.sslContext, null);
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
    SelfSignedCertificate generated = null;

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
          "!!! WARNING !!! WEBTRANSPORT4J DEVELOPMENT MODE IS ENABLED. A self-signed TLS"
              + " certificate will be generated. Do not use this configuration in production."
              + " Configure webtransport4j.ssl.key.path and webtransport4j.ssl.cert.path. !!!"
              + " WARNING !!!");

      generated = new SelfSignedCertificate("localhost");
      keyFile = generated.privateKey();
      certFile = generated.certificate();
    } else {
      throw new IllegalStateException(
          "SSL key path and certificate path must be configured for production. "
              + "Set webtransport4j.ssl.key.path and webtransport4j.ssl.cert.path, "
              + "or enable webtransport4j.dev_mode=true for local development.");
    }

    try {
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
      applyClientAuthAndTrust(sslBuilder);
      QuicSslContext resolvedSslCtx = sslBuilder.build();
      applySessionTicketKeys(resolvedSslCtx);
      return new SslContextBuildResult(resolvedSslCtx, generated);
    } catch (Exception | Error e) {
      deleteGeneratedCertificateQuietly(generated, "failed TLS startup");
      throw e;
    }
  }

  private ClientAuthMode resolveClientAuthMode() {
    if (clientAuthMode != null) {
      return clientAuthMode;
    }
    String configured = WebTransportConfig.get("webtransport4j.ssl.client_auth", "NONE");
    try {
      return ClientAuthMode.valueOf(configured.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException(
          "webtransport4j.ssl.client_auth must be NONE, OPTIONAL, or REQUIRE", invalid);
    }
  }

  void applyClientAuthAndTrust(QuicSslContextBuilder sslBuilder) {
    ClientAuthMode mode = resolveClientAuthMode();

    if (mode == ClientAuthMode.REQUIRE) {
      sslBuilder.clientAuth(io.netty.handler.ssl.ClientAuth.REQUIRE);
    } else if (mode == ClientAuthMode.OPTIONAL) {
      sslBuilder.clientAuth(io.netty.handler.ssl.ClientAuth.OPTIONAL);
    } else {
      sslBuilder.clientAuth(io.netty.handler.ssl.ClientAuth.NONE);
    }

    if (this.trustCertFile != null) {
      requireReadableTrustFile(this.trustCertFile);
      sslBuilder.trustManager(this.trustCertFile);
    } else if (this.trustCertificates != null) {
      sslBuilder.trustManager(this.trustCertificates);
    } else if (this.trustManagerFactory != null) {
      sslBuilder.trustManager(this.trustManagerFactory);
    } else if (this.trustManager != null) {
      sslBuilder.trustManager(this.trustManager);
    } else {
      String trustPath = WebTransportConfig.get("webtransport4j.ssl.trust_cert.path", null);
      if (trustPath != null) {
        File trustFile = new File(trustPath.trim());
        requireReadableTrustFile(trustFile);
        sslBuilder.trustManager(trustFile);
      }
    }
  }

  private static void requireReadableTrustFile(File file) {
    if (!file.isFile() || !file.canRead()) {
      throw new IllegalArgumentException(
          "TLS trust certificate file must be a readable file: " + file);
    }
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
        "Explicit TLS session ticket keys loaded ({}). 1-RTT session resumption across servers is"
            + " enabled.",
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

    applyLongIfPresent("webtransport4j.quic.payload.size.send.max", builder::maxSendUdpPayloadSize);
    applyLongIfPresent("webtransport4j.quic.payload.size.recv.max", builder::maxRecvUdpPayloadSize);
    applyLongIfPresent("webtransport4j.quic.ack.delay.exponent", builder::ackDelayExponent);

    String maxAckDelayVal = WebTransportConfig.get("webtransport4j.quic.ack.delay.max.ms", null);
    if (maxAckDelayVal != null) {
      builder.maxAckDelay(Long.parseLong(maxAckDelayVal), TimeUnit.MILLISECONDS);
    }

    applyBooleanIfPresent("webtransport4j.quic.active.migration.enabled", builder::activeMigration);
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
      int targetPort,
      EventLoopGroup eventLoopGroup,
      @Nullable GlobalTrafficShapingHandler shaper)
      throws Exception {
    int recvBufSize =
        WebTransportConfig.getInt(
            "webtransport4j.server.recv.buffer.size", DEFAULT_RECV_BUFFER_SIZE);
    if (recvBufSize <= 0) {
      throw new IllegalArgumentException(
          "webtransport4j.server.recv.buffer.size must be > 0: " + recvBufSize);
    }
    int maxMessagesPerRead =
        WebTransportConfig.getInt(
            "webtransport4j.server.recv.max_messages_per_read", DEFAULT_MAX_MESSAGES_PER_READ);
    if (maxMessagesPerRead <= 0) {
      throw new IllegalArgumentException(
          "webtransport4j.server.recv.max_messages_per_read must be > 0: " + maxMessagesPerRead);
    }
    FixedRecvByteBufAllocator recvByteBufAllocator = new FixedRecvByteBufAllocator(recvBufSize);
    recvByteBufAllocator.maxMessagesPerRead(maxMessagesPerRead);

    int sndBuf =
        WebTransportConfig.getInt(
            "webtransport4j.server.socket.sndbuf", DEFAULT_SOCKET_BUFFER_SIZE);
    if (sndBuf > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, sndBuf);
    }
    int rcvBuf =
        WebTransportConfig.getInt(
            "webtransport4j.server.socket.rcvbuf", DEFAULT_SOCKET_BUFFER_SIZE);
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

    int bindTimeoutSeconds =
        WebTransportConfig.getInt(
            "webtransport4j.server.bind.timeout.seconds", DEFAULT_BIND_TIMEOUT_SECONDS);
    if (bindTimeoutSeconds <= 0) {
      throw new IllegalArgumentException(
          "webtransport4j.server.bind.timeout.seconds must be > 0: " + bindTimeoutSeconds);
    }

    ChannelFuture bindFuture =
        bootstrap
            .group(eventLoopGroup)
            .channel(transportConfig.channelClass)
            .handler(serverCodec)
            .option(ChannelOption.RECVBUF_ALLOCATOR, recvByteBufAllocator)
            .bind(bindAddress);

    if (!bindFuture.await(bindTimeoutSeconds, TimeUnit.SECONDS)) {
      bindFuture.cancel(true);
      closeQuietly(bindFuture.channel());
      throw new IllegalStateException(
          "Timed out after " + bindTimeoutSeconds + "s binding to " + bindAddress);
    }
    if (!bindFuture.isSuccess()) {
      closeQuietly(bindFuture.channel());
      throw new IllegalStateException("Failed to bind to " + bindAddress, bindFuture.cause());
    }

    Channel bound = bindFuture.channel();
    bound.attr(WebTransportAttributeKeys.METRICS_LISTENER).set(metricsListener);
    bound.attr(WebTransportAttributeKeys.SERVER_KEY).set(this);
    if (shaper != null) {
      bound.attr(WebTransportAttributeKeys.GLOBAL_TRAFFIC_SHAPER).set(shaper);
    }
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

  /** Stops the server gracefully. The instance remains restartable via {@link #start()}. */
  public void stop() {
    stop(5, TimeUnit.SECONDS);
  }

  /** Stops the server with a specified timeout. The instance remains restartable. */
  public void stop(long timeout, @NonNull TimeUnit unit) {
    stop(timeout, Objects.requireNonNull(unit, "unit"), true, false);
  }

  private void stop(long timeout, TimeUnit unit, boolean shutdownExecutor, boolean terminal) {
    EventLoopSafety.requireBlockingAllowed();
    if (timeout < 0L) {
      throw new IllegalArgumentException("timeout must be >= 0: " + timeout);
    }
    final long deadline = System.nanoTime() + unit.toNanos(timeout);
    for (QuicChannel connection : connections) {
      if (connection.eventLoop().inEventLoop()) {
        throw new IllegalStateException("Blocking stop must not run on a QUIC event loop");
      }
    }
    if (terminal) {
      permanentlyClosed = true;
    }
    EventLoopSafety.requireBlockingAllowed();
    synchronized (lifecycleLock) {
      startEpoch.incrementAndGet();
      ServerState previous = state.get();
      if (previous == ServerState.CLOSED) {
        logger.debug("Server is already closed.");
        return;
      }
      if (previous == ServerState.STOPPED) {
        if (shutdownExecutor || permanentlyClosed) {
          shutdownBusinessExecutorSafely(timeout, unit);
        }
        if (permanentlyClosed) {
          // close() owns and must release a caller-supplied shaper even if start() never ran.
          releaseTrafficShaperSafely();
          this.activeSslContext = null;
          deleteGeneratedCertificate();
          state.set(ServerState.CLOSED);
        }
        lifecycleLock.notifyAll();
        logger.debug("Server is already stopped.");
        return;
      }
      if (previous == ServerState.STOPPING) {
        if (terminal) {
          awaitNotStopping(timeout, unit);
          if (state.get() == ServerState.CLOSED) {
            return;
          }
          shutdownBusinessExecutorSafely(timeout, unit);
          releaseTrafficShaperSafely();
          this.activeSslContext = null;
          deleteGeneratedCertificate();
          state.set(ServerState.CLOSED);
          lifecycleLock.notifyAll();
        } else {
          logger.debug("Server is already stopping.");
        }
        return;
      }
      state.set(ServerState.STOPPING);
    }
    try {
      for (QuicChannel connection : connections) {
        drainConnection(connection);
      }
      drainActiveSessions();
      sessionClosures.drainPermits();
      while (!activeSessionsSet.isEmpty()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          break;
        }
        try {
          if (!sessionClosures.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
            break;
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      for (WebTransportSession session : activeSessionsSet) {
        try {
          session.close();
        } catch (RuntimeException failure) {
          logger.warn("Could not close session at shutdown deadline", failure);
        } finally {
          unregisterSession(session);
        }
      }
      timeout = Math.max(0, deadline - System.nanoTime());
      unit = TimeUnit.NANOSECONDS;
      TlsCertificateWatcher watcher = tlsWatcher;
      tlsWatcher = null;
      stopTlsWatcherSafely(watcher);
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
            logger.warn("!!! WARNING !!! Timed out closing server channel. !!! WARNING !!!");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Interrupted while closing server channel.", e);
        } catch (Exception e) {
          logger.error("Error closing server channel", e);
        }
      }
      EventLoopGroup g = this.group;
      this.group = null;
      timeout = Math.max(0, deadline - System.nanoTime());
      if (g != null) {
        try {
          if (!g.shutdownGracefully(0, timeout, unit).await(timeout, unit)) {
            logger.warn("!!! WARNING !!! Timed out shutting down event loop group !!! WARNING !!!");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Interrupted while shutting down event loop group", e);
        } catch (Exception e) {
          logger.error("Error shutting down event loop group", e);
        }
      }
      if (permanentlyClosed || !trafficShaperExternallySupplied) {
        releaseTrafficShaperSafely();
      }
      this.activeSslContext = null;
      unregisterServerInstanceSafely();
      deleteGeneratedCertificate();
      if (shutdownExecutor || permanentlyClosed) {
        timeout = Math.max(0, deadline - System.nanoTime());
        shutdownBusinessExecutorSafely(timeout, unit);
      }
      logger.info("WebTransport server stopped successfully.");
    } finally {
      EventLoopSafety.requireBlockingAllowed();
      synchronized (lifecycleLock) {
        if (permanentlyClosed) {
          state.set(ServerState.CLOSED);
        } else {
          state.set(ServerState.STOPPED);
        }
        lifecycleLock.notifyAll();
      }
    }
  }

  /**
   * Waits until an in-flight {@link #stop} leaves {@link ServerState#STOPPING}. Caller must hold
   * {@link #lifecycleLock}.
   */
  private void awaitNotStopping(long timeout, TimeUnit unit) {
    long deadline = System.nanoTime() + unit.toNanos(Math.max(0L, timeout));
    while (state.get() == ServerState.STOPPING) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0L) {
        logger.warn(
            "!!! WARNING !!! Timed out waiting for in-flight stop to finish !!! WARNING !!!");
        return;
      }
      try {
        TimeUnit.NANOSECONDS.timedWait(lifecycleLock, remaining);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void deleteGeneratedCertificate() {
    SelfSignedCertificate ssc = this.generatedCertificate;
    this.generatedCertificate = null;
    deleteGeneratedCertificateQuietly(ssc, "server shutdown");
  }

  private static void deleteGeneratedCertificateQuietly(
      @Nullable SelfSignedCertificate certificate, String reason) {
    if (certificate != null) {
      try {
        certificate.delete();
      } catch (Exception e) {
        logger.debug("Failed to delete generated self-signed certificate during {}", reason, e);
      }
    }
  }

  private static void stopTlsWatcher(@Nullable TlsCertificateWatcher watcher) {
    if (watcher != null) {
      watcher.stop();
    }
  }

  private static void stopTlsWatcherSafely(@Nullable TlsCertificateWatcher watcher) {
    try {
      stopTlsWatcher(watcher);
    } catch (RuntimeException e) {
      logger.error("Error stopping TLS certificate watcher", e);
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
    ExecutorService executor = this.businessExecutor;
    if (ownsBusinessExecutor && executor != null && !executor.isShutdown()) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(timeout, unit)) {
          executor.shutdownNow();
          if (!executor.awaitTermination(timeout, unit)) {
            logger.warn("!!! WARNING !!! Business executor did not terminate !!! WARNING !!!");
          }
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private void shutdownBusinessExecutorSafely(long timeout, TimeUnit unit) {
    try {
      shutdownBusinessExecutor(timeout, unit);
    } catch (RuntimeException e) {
      logger.error("Error shutting down business executor", e);
    }
  }

  private QuicTokenHandler resolveTokenHandler() {
    if (this.quicTokenHandler != null) {
      return this.quicTokenHandler;
    }
    return getTokenHandler();
  }

  private QuicConnectionIdGenerator resolveConnectionIdGenerator() {
    if (this.connectionIdGenerator != null) {
      return this.connectionIdGenerator;
    }
    String serverIdVal = WebTransportConfig.get("webtransport4j.quic.server.id", null);
    if (serverIdVal == null) {
      serverIdVal = System.getenv("SERVER_ID");
    }
    if (serverIdVal != null && !serverIdVal.trim().isEmpty()) {
      int serverId = Integer.parseInt(serverIdVal.trim());
      logger.info(
          "QUIC-LB Connection ID routing configured: ServerIdConnectionIdGenerator (Server ID: {})",
          serverId);
      return new ServerIdConnectionIdGenerator(serverId);
    }
    String generatorType =
        WebTransportConfig.get("webtransport4j.quic.connection.id.generator", null);
    if (generatorType != null && !generatorType.trim().isEmpty()) {
      if ("random".equalsIgnoreCase(generatorType)) {
        return QuicConnectionIdGenerator.randomGenerator();
      } else if ("sign".equalsIgnoreCase(generatorType) || "hmac".equalsIgnoreCase(generatorType)) {
        return QuicConnectionIdGenerator.signGenerator();
      } else {
        try {
          Class<?> genClass = Class.forName(generatorType);
          if (!QuicConnectionIdGenerator.class.isAssignableFrom(genClass)) {
            throw new IllegalArgumentException(
                generatorType + " does not implement " + QuicConnectionIdGenerator.class.getName());
          }
          return (QuicConnectionIdGenerator) genClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          throw new IllegalStateException(
              "Failed to load custom QuicConnectionIdGenerator: " + generatorType, e);
        }
      }
    }
    return null;
  }

  /** Returns the token handler. */
  public static @NonNull QuicTokenHandler getTokenHandler() {
    String tokenHandlerType = WebTransportConfig.get("webtransport4j.quic.token.handler", "hmac");
    if ("insecure".equalsIgnoreCase(tokenHandlerType)) {
      logger.warn(
          "!!! WARNING !!! QUIC token handler is INSECURE (InsecureQuicTokenHandler). Address"
              + " validation tokens are not cryptographically bound. Do not use this in production."
              + " !!! WARNING !!!");
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
        throw new IllegalArgumentException("HMAC key contains a non-hex character at index " + i);
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
    if (quicMaxBidi < 0
        || wtMaxBidi < 0
        || quicMaxUni < 0
        || wtMaxUni < 0
        || quicMaxData < 0
        || wtMaxData < 0) {
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

  /**
   * Terminal shutdown. Releases the owned business executor and moves to {@link
   * ServerState#CLOSED}. A subsequent {@link #start()} throws.
   */
  @Override
  public void close() {
    stop(5, TimeUnit.SECONDS, true, true);
    closeMetricsListener(metricsListener);
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
