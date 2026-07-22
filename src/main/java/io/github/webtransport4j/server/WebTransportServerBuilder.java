package io.github.webtransport4j.server;

import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.ReactiveWebTransportHandlerAdapter;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicTokenHandler;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Fluent builder for creating and configuring {@link WebTransportServer} instances.
 */
public class WebTransportServerBuilder {

  private Integer port;
  private String sslKeyPath;
  private String sslCertPath;
  private QuicSslContext sslContext;
  private List<String> allowedOrigins;
  private ExecutorService businessExecutor;
  private WebTransportMetricsListener metricsListener;
  private QuicTokenHandler quicTokenHandler;
  private String transportType;
  private Long idleTimeoutSeconds;
  private Long initialMaxStreamsBidi;
  private Long initialMaxStreamsUni;
  private Long initialMaxData;
  private WebTransportHandler defaultHandler;
  private final Map<String, WebTransportHandler> handlers = new Object2ObjectOpenHashMap<>();
  private Supplier<MessageDispatcher> messageDispatcherSupplier;

  public WebTransportServerBuilder() {}

  /** Sets the server listening port. */
  public @NonNull WebTransportServerBuilder port(int port) {
    this.port = port;
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
  public @NonNull WebTransportServerBuilder ssl(@Nullable String keyPath, @Nullable String certPath) {
    this.sslKeyPath = keyPath;
    this.sslCertPath = certPath;
    return this;
  }

  /** Sets a pre-built {@link QuicSslContext}. */
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

  /** Sets the business executor for offloading handler callbacks. */
  public @NonNull WebTransportServerBuilder businessExecutor(@Nullable ExecutorService businessExecutor) {
    this.businessExecutor = businessExecutor;
    return this;
  }

  /** Sets the observability metrics listener. */
  public @NonNull WebTransportServerBuilder metricsListener(@Nullable WebTransportMetricsListener metricsListener) {
    this.metricsListener = metricsListener;
    return this;
  }

  /** Sets the custom QUIC token handler. */
  public @NonNull WebTransportServerBuilder quicTokenHandler(@Nullable QuicTokenHandler quicTokenHandler) {
    this.quicTokenHandler = quicTokenHandler;
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

  /** Sets the default handler for unregistered routes. */
  public @NonNull WebTransportServerBuilder defaultHandler(@NonNull WebTransportHandler defaultHandler) {
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

  /** Sets a custom {@link MessageDispatcher} supplier. */
  public @NonNull WebTransportServerBuilder messageDispatcherSupplier(
      @NonNull Supplier<MessageDispatcher> supplier) {
    this.messageDispatcherSupplier = supplier;
    return this;
  }

  // Getters for WebTransportServer initialization
  public Integer getPort() { return port; }
  public String getSslKeyPath() { return sslKeyPath; }
  public String getSslCertPath() { return sslCertPath; }
  public QuicSslContext getSslContext() { return sslContext; }
  public List<String> getAllowedOrigins() { return allowedOrigins; }
  public ExecutorService getBusinessExecutor() { return businessExecutor; }
  public WebTransportMetricsListener getMetricsListener() { return metricsListener; }
  public QuicTokenHandler getQuicTokenHandler() { return quicTokenHandler; }
  public String getTransportType() { return transportType; }
  public Long getIdleTimeoutSeconds() { return idleTimeoutSeconds; }
  public Long getInitialMaxStreamsBidi() { return initialMaxStreamsBidi; }
  public Long getInitialMaxStreamsUni() { return initialMaxStreamsUni; }
  public Long getInitialMaxData() { return initialMaxData; }
  public WebTransportHandler getDefaultHandler() { return defaultHandler; }
  public Map<String, WebTransportHandler> getHandlers() { return handlers; }
  public Supplier<MessageDispatcher> getMessageDispatcherSupplier() { return messageDispatcherSupplier; }

  /** Constructs and returns a configured {@link WebTransportServer} instance. */
  public @NonNull WebTransportServer build() {
    return new WebTransportServer(this);
  }
}
