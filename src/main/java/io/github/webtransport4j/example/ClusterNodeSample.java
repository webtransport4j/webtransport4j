package io.github.webtransport4j.example;

import com.sun.net.httpserver.HttpServer;
import io.github.webtransport4j.observability.otlp.WebTransportOtlpConfig;
import io.github.webtransport4j.observability.otlp.WebTransportOtlpMetricsListener;
import io.github.webtransport4j.server.HmacQuicTokenHandler;
import io.github.webtransport4j.server.WebTransportConfig;
import io.github.webtransport4j.server.WebTransportServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production-ready containerized cluster node entrypoint for WebTransport4J deployments in Kubernetes.
 *
 * <p>Supports shared QUIC token keys across pods, OTLP metrics streaming, and HTTP liveness/readiness probes.
 */
public class ClusterNodeSample {

  private static final Logger log = LoggerFactory.getLogger(ClusterNodeSample.class);

  /**
   * Main entry point for the clustered container node.
   *
   * @param args command line arguments
   * @throws Exception if server startup fails
   */
  public static void main(@NonNull String[] args) throws Exception {
    final int quicPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "4433"));
    final int healthPort = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", "8080"));
    final String nodeName = System.getenv().getOrDefault("POD_NAME", "wt-node-local");
    final String otlpEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    final String hmacKeyStr = System.getenv().getOrDefault("CLUSTER_HMAC_KEY", "default-cluster-secret-key-32b-min");

    log.info("🚀 Starting WebTransport4J Clustered Node '{}' on QUIC port {} (Health on {})",
        nodeName, quicPort, healthPort);

    // 1. Configure WebTransport server
    WebTransportConfig config = new WebTransportConfig();
    config.setPort(quicPort);

    // Shared QUIC HMAC token handler for stateless resumption across pods
    byte[] hmacKey = hmacKeyStr.getBytes(StandardCharsets.UTF_8);
    HmacQuicTokenHandler tokenHandler = new HmacQuicTokenHandler(hmacKey, 60000L);
    config.setQuicTokenHandler(tokenHandler);

    WebTransportServer server = new WebTransportServer(new DefaultPathHandler(), config);
    server.registerHandler("/echo", new EchoWebTransportHandler());
    server.registerHandler("/chat", new WebTransportChatHandler());
    server.registerHandler("/test", new WebTransportTestHandler());

    // 2. Attach OTLP metrics listener if collector endpoint is configured
    WebTransportOtlpMetricsListener otlpListener = null;
    if (otlpEndpoint != null && !otlpEndpoint.isEmpty()) {
      log.info("📡 Streaming OTLP metrics to {}", otlpEndpoint);
      WebTransportOtlpConfig otlpConfig =
          WebTransportOtlpConfig.builder()
              .url(otlpEndpoint)
              .step(Duration.ofSeconds(5))
              .resourceAttribute("service.name", "webtransport4j-cluster")
              .resourceAttribute("k8s.pod.name", nodeName)
              .build();
      otlpListener = new WebTransportOtlpMetricsListener(otlpConfig);
      server.setMetricsListener(otlpListener);
    }

    // 3. Start auxiliary HTTP server for Kubernetes probes
    HttpServer healthHttpServer = HttpServer.create(new InetSocketAddress(healthPort), 0);
    healthHttpServer.createContext("/healthz", exchange -> {
      byte[] resp = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, resp.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(resp);
      }
    });

    healthHttpServer.createContext("/readyz", exchange -> {
      byte[] resp = "{\"ready\":true}".getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, resp.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(resp);
      }
    });

    healthHttpServer.setExecutor(null);
    healthHttpServer.start();
    log.info("✅ Kubernetes liveness/readiness probes listening on port {}", healthPort);

    // 4. Graceful shutdown hook
    final WebTransportOtlpMetricsListener finalOtlp = otlpListener;
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("🛑 Shutting down WebTransport node '{}'...", nodeName);
      healthHttpServer.stop(1);
      server.stop();
      if (finalOtlp != null) {
        finalOtlp.close();
      }
      log.info("👋 WebTransport node '{}' stopped cleanly.", nodeName);
    }));

    // 5. Start QUIC server
    server.start();
  }
}
