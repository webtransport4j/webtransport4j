package io.github.webtransport4j.server;

import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.ReactiveWebTransportHandlerAdapter;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.resilience.OverloadProtectionPolicy;
import io.github.webtransport4j.security.ClientAuthMode;
import io.github.webtransport4j.security.OriginValidator;
import io.netty.handler.codec.quic.QuicConnectionIdGenerator;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicTokenHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.File;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Fluent builder for creating and configuring {@link WebTransportServer} instances. */
public class WebTransportServerBuilder {

  private Integer port;
  private String host;
  private String sslKeyPath;
  private String sslCertPath;
  private QuicSslContext sslContext;
  private List<String> allowedOrigins;
  private ExecutorService businessExecutor;
  private WebTransportMetricsListener metricsListener;
  private QuicTokenHandler quicTokenHandler;
  private QuicConnectionIdGenerator connectionIdGenerator;
  private String transportType;
  private Long idleTimeoutSeconds;
  private Long initialMaxStreamsBidi;
  private Long initialMaxStreamsUni;
  private Long initialMaxData;
  private WebTransportHandler defaultHandler;
  private final Map<String, WebTransportHandler> handlers = new Object2ObjectOpenHashMap<>();
  private Supplier<MessageDispatcher> messageDispatcherSupplier;
  private GlobalTrafficShapingHandler trafficShaper;
  private Long globalTrafficWriteLimit;
  private Long globalTrafficReadLimit;
  private OverloadProtectionPolicy overloadProtectionPolicy;
  private Boolean autoTuneUdpSocket;
  private ClientAuthMode clientAuthMode;
  private File trustCertFile;
  private X509Certificate[] trustCertificates;
  private TrustManagerFactory trustManagerFactory;
  private TrustManager trustManager;
  private OriginValidator originValidator;
  private Boolean strictOriginValidation;

  public WebTransportServerBuilder() {}

  /** Sets the server listening port. */
  public @NonNull WebTransportServerBuilder port(int port) {
    this.port = port;
    return this;
  }

  /** Sets the server bind host IP or address (e.g., "0.0.0.0" or "127.0.0.1"). */
  public @NonNull WebTransportServerBuilder host(@Nullable String host) {
    this.host = host;
    return this;
  }

  /** Sets the path to the SSL private key file (PEM format). */
  public @NonNull WebTransportServerBuilder sslKeyPath(@Nullable String sslKeyPath) {
    this.sslKeyPath = sslKeyPath;
    return this;
  }

  /** Sets the path to the SSL certificate file (PEM format). */
  public @NonNull WebTransportServerBuilder sslCertPath(@Nullable String sslCertPath) {
    this.sslCertPath = sslCertPath;
    return this;
  }

  /** Configures SSL key and certificate paths. */
  public @NonNull WebTransportServerBuilder ssl(
      @Nullable String keyPath, @Nullable String certPath) {
    this.sslKeyPath = keyPath;
    this.sslCertPath = certPath;
    return this;
  }

  /**
   * Sets a pre-built {@link QuicSslContext}. Configure client authentication and trust on that
   * context; combining it with server clientAuth/trustManager options fails at startup.
   */
  public @NonNull WebTransportServerBuilder sslContext(@Nullable QuicSslContext sslContext) {
    this.sslContext = sslContext;
    return this;
  }

  /** Sets the allowed CORS/WebTransport origins. */
  public @NonNull WebTransportServerBuilder allowedOrigins(@NonNull List<String> allowedOrigins) {
    this.allowedOrigins = new ObjectArrayList<>(allowedOrigins);
    return this;
  }

  /** Sets the allowed CORS/WebTransport origins. */
  public @NonNull WebTransportServerBuilder allowedOrigins(@NonNull String... origins) {
    this.allowedOrigins = Arrays.asList(origins);
    return this;
  }

  /**
   * Configures the TLS client authentication (mTLS) mode.
   *
   * @param clientAuthMode the client authentication mode
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder clientAuth(@Nullable ClientAuthMode clientAuthMode) {
    this.clientAuthMode = clientAuthMode;
    return this;
  }

  /**
   * Configures the TLS client authentication mode using Netty's ClientAuth enum.
   *
   * @param clientAuth Netty ClientAuth mode
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder clientAuth(@Nullable ClientAuth clientAuth) {
    if (clientAuth == null) {
      this.clientAuthMode = ClientAuthMode.NONE;
    } else {
      switch (clientAuth) {
        case REQUIRE:
          this.clientAuthMode = ClientAuthMode.REQUIRE;
          break;
        case OPTIONAL:
          this.clientAuthMode = ClientAuthMode.OPTIONAL;
          break;
        case NONE:
        default:
          this.clientAuthMode = ClientAuthMode.NONE;
          break;
      }
    }
    return this;
  }

  /**
   * Sets the trusted CA certificate chain file for verifying mTLS client certificates. Replaces any
   * previously configured trust source.
   *
   * @param trustCertFile CA certificate chain file
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder trustManager(@Nullable File trustCertFile) {
    clearTrustSources();
    this.trustCertFile = trustCertFile;
    return this;
  }

  /**
   * Sets the trusted certificates for verifying mTLS client certificates. Replaces any previously
   * configured trust source and copies the certificate array.
   *
   * @param certificates trusted X.509 certificates
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder trustManager(
      X509Certificate @Nullable ... certificates) {
    clearTrustSources();
    this.trustCertificates = certificates == null ? null : certificates.clone();
    return this;
  }

  /**
   * Sets the TrustManagerFactory for verifying mTLS client certificates. Replaces any previously
   * configured trust source.
   *
   * @param trustManagerFactory trust manager factory
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder trustManager(
      @Nullable TrustManagerFactory trustManagerFactory) {
    clearTrustSources();
    this.trustManagerFactory = trustManagerFactory;
    return this;
  }

  /**
   * Sets the TrustManager for verifying mTLS client certificates. Replaces any previously
   * configured trust source.
   *
   * @param trustManager trust manager
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder trustManager(@Nullable TrustManager trustManager) {
    clearTrustSources();
    this.trustManager = trustManager;
    return this;
  }

  private void clearTrustSources() {
    trustCertFile = null;
    trustCertificates = null;
    trustManagerFactory = null;
    trustManager = null;
  }

  /**
   * Configures a custom origin and authority validator.
   *
   * @param originValidator the origin validator
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder originValidator(
      @Nullable OriginValidator originValidator) {
    this.originValidator = originValidator;
    return this;
  }

  /**
   * Enables or disables strict origin validation (rejecting requests with missing Origin header).
   *
   * @param strictOriginValidation whether strict origin validation is enforced
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder strictOriginValidation(boolean strictOriginValidation) {
    this.strictOriginValidation = strictOriginValidation;
    return this;
  }

  /** Sets the business executor for offloading handler callbacks. */
  public @NonNull WebTransportServerBuilder businessExecutor(
      @Nullable ExecutorService businessExecutor) {
    this.businessExecutor = businessExecutor;
    return this;
  }

  /** Sets the observability metrics listener. */
  public @NonNull WebTransportServerBuilder metricsListener(
      @Nullable WebTransportMetricsListener metricsListener) {
    this.metricsListener = metricsListener;
    return this;
  }

  /** Sets the custom QUIC token handler. */
  public @NonNull WebTransportServerBuilder quicTokenHandler(
      @Nullable QuicTokenHandler quicTokenHandler) {
    this.quicTokenHandler = quicTokenHandler;
    return this;
  }

  /**
   * Sets a custom {@link QuicConnectionIdGenerator} for generating server Destination Connection
   * IDs (DCIDs).
   *
   * @param connectionIdGenerator custom connection ID generator
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder connectionIdGenerator(
      @Nullable QuicConnectionIdGenerator connectionIdGenerator) {
    this.connectionIdGenerator = connectionIdGenerator;
    return this;
  }

  /**
   * Configures QUIC-LB Server ID routing (draft-ietf-quic-load-balancers) with a single-byte server
   * ID (0 to 255). Incoming packets can be routed by L4 balancers using the Connection ID prefix.
   *
   * @param serverId unique server ID (0 to 255)
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder serverId(int serverId) {
    this.connectionIdGenerator = new ServerIdConnectionIdGenerator(serverId);
    return this;
  }

  /** Sets the transport type ("auto", "epoll", "kqueue", "iouring", "nio"). */
  public @NonNull WebTransportServerBuilder transportType(@Nullable String transportType) {
    this.transportType = transportType;
    return this;
  }

  /** Sets the QUIC connection idle timeout. */
  public @NonNull WebTransportServerBuilder idleTimeout(long timeout, @NonNull TimeUnit unit) {
    this.idleTimeoutSeconds = unit.toSeconds(timeout);
    return this;
  }

  /** Sets the max bidirectional and unidirectional streams per connection. */
  public @NonNull WebTransportServerBuilder maxStreams(long maxBidi, long maxUni) {
    this.initialMaxStreamsBidi = maxBidi;
    this.initialMaxStreamsUni = maxUni;
    return this;
  }

  /** Sets the initial connection max data payload limit. */
  public @NonNull WebTransportServerBuilder maxData(long maxData) {
    this.initialMaxData = maxData;
    return this;
  }

  /**
   * Sets the adaptive overload protection policy for protecting server resources.
   *
   * @param overloadProtectionPolicy the overload protection policy
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder overloadProtectionPolicy(
      @Nullable OverloadProtectionPolicy overloadProtectionPolicy) {
    this.overloadProtectionPolicy = overloadProtectionPolicy;
    return this;
  }

  /**
   * Enables or disables automatic OS UDP socket buffer auto-tuning.
   *
   * @param autoTuneUdpSocket whether to auto-tune SO_RCVBUF and SO_SNDBUF
   * @return this builder
   */
  public @NonNull WebTransportServerBuilder autoTuneUdpSocket(boolean autoTuneUdpSocket) {
    this.autoTuneUdpSocket = autoTuneUdpSocket;
    return this;
  }

  /** Sets the default handler for unregistered routes. */
  public @NonNull WebTransportServerBuilder defaultHandler(
      @NonNull WebTransportHandler defaultHandler) {
    this.defaultHandler = defaultHandler;
    return this;
  }

  /** Sets the default reactive handler for unregistered routes. */
  public @NonNull WebTransportServerBuilder defaultReactiveHandler(
      @NonNull ReactiveWebTransportHandler defaultReactiveHandler) {
    this.defaultHandler = new ReactiveWebTransportHandlerAdapter(defaultReactiveHandler);
    return this;
  }

  /** Registers a WebTransport handler for a specific URI path. */
  public @NonNull WebTransportServerBuilder handler(
      @NonNull String path, @NonNull WebTransportHandler handler) {
    this.handlers.put(path, handler);
    return this;
  }

  /** Registers a reactive WebTransport handler for a specific URI path. */
  public @NonNull WebTransportServerBuilder reactiveHandler(
      @NonNull String path, @NonNull ReactiveWebTransportHandler reactiveHandler) {
    this.handlers.put(path, new ReactiveWebTransportHandlerAdapter(reactiveHandler));
    return this;
  }

  /** Sets a custom {@link MessageDispatcher} instance. */
  public @NonNull WebTransportServerBuilder messageDispatcher(
      @NonNull MessageDispatcher dispatcher) {
    this.messageDispatcherSupplier = () -> dispatcher;
    return this;
  }

  /** Sets a custom {@link MessageDispatcher} supplier. */
  public @NonNull WebTransportServerBuilder messageDispatcherSupplier(
      @NonNull Supplier<MessageDispatcher> supplier) {
    this.messageDispatcherSupplier = supplier;
    return this;
  }

  /**
   * Sets a pre-configured handler whose exclusive ownership transfers to the server on build.
   * Building another server with the same handler (including through another builder) is rejected.
   * Use {@link #globalTrafficLimits(long, long)} to create a separate handler for every server.
   */
  public @NonNull WebTransportServerBuilder trafficShaper(
      @Nullable GlobalTrafficShapingHandler trafficShaper) {
    this.trafficShaper = trafficShaper;
    return this;
  }

  /** Sets global bandwidth rate limits in bytes per second for this server instance. */
  public @NonNull WebTransportServerBuilder globalTrafficLimits(long writeLimit, long readLimit) {
    this.globalTrafficWriteLimit = writeLimit;
    this.globalTrafficReadLimit = readLimit;
    return this;
  }

  // Getters for WebTransportServer initialization
  Integer getPort() {
    return port;
  }

  String getHost() {
    return host;
  }

  String getSslKeyPath() {
    return sslKeyPath;
  }

  String getSslCertPath() {
    return sslCertPath;
  }

  QuicSslContext getSslContext() {
    return sslContext;
  }

  List<String> getAllowedOrigins() {
    return allowedOrigins;
  }

  ExecutorService getBusinessExecutor() {
    return businessExecutor;
  }

  WebTransportMetricsListener getMetricsListener() {
    return metricsListener;
  }

  QuicTokenHandler getQuicTokenHandler() {
    return quicTokenHandler;
  }

  QuicConnectionIdGenerator getConnectionIdGenerator() {
    return connectionIdGenerator;
  }

  String getTransportType() {
    return transportType;
  }

  Long getIdleTimeoutSeconds() {
    return idleTimeoutSeconds;
  }

  Long getInitialMaxStreamsBidi() {
    return initialMaxStreamsBidi;
  }

  Long getInitialMaxStreamsUni() {
    return initialMaxStreamsUni;
  }

  Long getInitialMaxData() {
    return initialMaxData;
  }

  WebTransportHandler getDefaultHandler() {
    return defaultHandler;
  }

  Map<String, WebTransportHandler> getHandlers() {
    return handlers;
  }

  Supplier<MessageDispatcher> getMessageDispatcherSupplier() {
    return messageDispatcherSupplier;
  }

  GlobalTrafficShapingHandler getTrafficShaper() {
    return trafficShaper;
  }

  Long getGlobalTrafficWriteLimit() {
    return globalTrafficWriteLimit;
  }

  Long getGlobalTrafficReadLimit() {
    return globalTrafficReadLimit;
  }

  OverloadProtectionPolicy getOverloadProtectionPolicy() {
    return overloadProtectionPolicy;
  }

  Boolean getAutoTuneUdpSocket() {
    return autoTuneUdpSocket;
  }

  ClientAuthMode getClientAuthMode() {
    return clientAuthMode;
  }

  File getTrustCertFile() {
    return trustCertFile;
  }

  X509Certificate[] getTrustCertificates() {
    return trustCertificates;
  }

  TrustManagerFactory getTrustManagerFactory() {
    return trustManagerFactory;
  }

  TrustManager getTrustManager() {
    return trustManager;
  }

  OriginValidator getOriginValidator() {
    return originValidator;
  }

  Boolean getStrictOriginValidation() {
    return strictOriginValidation;
  }

  /** Constructs and returns a configured {@link WebTransportServer} instance. */
  public @NonNull WebTransportServer build() {
    return new WebTransportServer(this);
  }
}
