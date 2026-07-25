package io.github.webtransport4j.server;

import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background watcher that monitors SSL key and certificate files for changes on disk and hot-reloads the active {@link QuicSslContext}.
 */
public class TlsCertificateWatcher {
  private static final Logger logger = LoggerFactory.getLogger(TlsCertificateWatcher.class);

  private final String keyPath;
  private final String certPath;
  private final Consumer<QuicSslContext> sslContextConsumer;
  private final int pollIntervalSeconds;

  private ScheduledExecutorService executor;
  private volatile long lastKeyModified = -1L;
  private volatile long lastCertModified = -1L;

  public TlsCertificateWatcher(
      @NonNull String keyPath,
      @NonNull String certPath,
      @NonNull Consumer<QuicSslContext> sslContextConsumer) {
    this(keyPath, certPath, sslContextConsumer, WebTransportConfig.getInt("webtransport4j.ssl.hot_reload.interval_secs", 5));
  }

  public TlsCertificateWatcher(
      @NonNull String keyPath,
      @NonNull String certPath,
      @NonNull Consumer<QuicSslContext> sslContextConsumer,
      int pollIntervalSeconds) {
    this.keyPath = keyPath;
    this.certPath = certPath;
    this.sslContextConsumer = sslContextConsumer;
    this.pollIntervalSeconds = pollIntervalSeconds;
  }

  /** Starts the TLS certificate file watcher. */
  public synchronized void start() {
    if (executor != null && !executor.isShutdown()) {
      return;
    }

    File keyFile = new File(keyPath);
    File certFile = new File(certPath);

    if (keyFile.exists()) {
      lastKeyModified = keyFile.lastModified();
    }
    if (certFile.exists()) {
      lastCertModified = certFile.lastModified();
    }

    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "wt-tls-cert-watcher");
      t.setDaemon(true);
      return t;
    });

    executor.scheduleAtFixedRate(this::checkAndReload, pollIntervalSeconds, pollIntervalSeconds, TimeUnit.SECONDS);
    logger.info("🔑 Started TLS Certificate Hot-Reload Watcher for key: '{}', cert: '{}' (interval: {}s)", keyPath, certPath, pollIntervalSeconds);
  }

  /** Stops the TLS certificate file watcher. */
  public synchronized void stop() {
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
      logger.info("👋 Stopped TLS Certificate Hot-Reload Watcher.");
    }
  }

  /** Check for file updates and reload if modified. */
  public boolean checkAndReload() {
    try {
      File keyFile = new File(keyPath);
      File certFile = new File(certPath);

      if (!keyFile.exists() || !certFile.exists()) {
        return false;
      }

      long currentKeyMod = keyFile.lastModified();
      long currentCertMod = certFile.lastModified();

      if (currentKeyMod > lastKeyModified || currentCertMod > lastCertModified) {
        logger.info("🔄 Modification detected on TLS certificate files. Attempting hot-reload...");

        QuicSslContext newSslCtx = QuicSslContextBuilder.forServer(keyFile, null, certFile)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

        lastKeyModified = currentKeyMod;
        lastCertModified = currentCertMod;

        sslContextConsumer.accept(newSslCtx);
        logger.info("✅ TLS Certificate hot-reloaded successfully. Newly negotiated QUIC connections will use updated certificates.");
        return true;
      }
    } catch (Exception e) {
      logger.error("❌ Failed to hot-reload TLS certificate files: {}", e.getMessage(), e);
    }
    return false;
  }
}
