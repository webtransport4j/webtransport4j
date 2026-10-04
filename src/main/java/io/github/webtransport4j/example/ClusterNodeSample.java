package io.github.webtransport4j.example;

import com.sun.net.httpserver.HttpServer;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStreamSummary;
import io.github.webtransport4j.observability.otlp.WebTransportOtlpConfig;
import io.github.webtransport4j.observability.otlp.WebTransportOtlpMetricsListener;
import io.github.webtransport4j.server.HmacQuicTokenHandler;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production-ready containerized cluster node entrypoint for WebTransport4J deployments in
 * Kubernetes.
 *
 * <p>Supports shared QUIC token keys across pods, OTLP metrics streaming, and HTTP
 * liveness/readiness probes.
 */
public class ClusterNodeSample {

  private static final Logger log = LoggerFactory.getLogger(ClusterNodeSample.class);
  private static final int MAX_MANAGEMENT_REQUEST_BYTES = 16 * 1024;

  private static String escapeJson(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  private static byte[] readBytes(InputStream is) throws java.io.IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] data = new byte[1024];
    int bytesRead;
    while ((bytesRead = is.read(data, 0, data.length)) != -1) {
      if (buffer.size() + bytesRead > MAX_MANAGEMENT_REQUEST_BYTES) {
        throw new java.io.IOException("Management request exceeds maximum size");
      }
      buffer.write(data, 0, bytesRead);
    }
    return buffer.toByteArray();
  }

  static boolean isAuthorized(com.sun.net.httpserver.HttpExchange exchange, String token)
      throws java.io.IOException {
    if (token == null || token.trim().isEmpty()) {
      exchange.sendResponseHeaders(503, -1);
      return false;
    }
    String authorization = exchange.getRequestHeaders().getFirst("Authorization");
    String expected = "Bearer " + token;
    if (authorization != null
        && MessageDigest.isEqual(
            expected.getBytes(StandardCharsets.UTF_8),
            authorization.getBytes(StandardCharsets.UTF_8))) {
      return true;
    }
    exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
    exchange.sendResponseHeaders(401, -1);
    return false;
  }

  static String requireSecret(String name) {
    String value = System.getenv(name);
    if (value == null || value.trim().isEmpty()
        || ("CLUSTER_HMAC_KEY".equals(name) && value.getBytes(StandardCharsets.UTF_8).length < 32)) {
      throw new IllegalStateException(name + " must be provisioned explicitly (HMAC: at least 32 bytes)");
    }
    return value;
  }

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
    final String managementBindHost =
        System.getenv().getOrDefault("MANAGEMENT_BIND_HOST", "127.0.0.1");
    final String managementToken =
        requireSecret("MANAGEMENT_AUTH_TOKEN");
    final String otlpEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    final String hmacKeyStr =
        requireSecret("CLUSTER_HMAC_KEY");

    log.info(
        "🚀 Starting WebTransport4J Clustered Node '{}' on QUIC port {} (Health on {})",
        nodeName,
        quicPort,
        healthPort);

    // 1. Configure WebTransport server builder
    byte[] hmacKey = hmacKeyStr.getBytes(StandardCharsets.UTF_8);
    HmacQuicTokenHandler tokenHandler = new HmacQuicTokenHandler(hmacKey, 60000L);

    final WebTransportServerBuilder serverBuilder =
        WebTransportServer.builder()
            .port(quicPort)
            .host("0.0.0.0")
            .quicTokenHandler(tokenHandler)
            .defaultHandler(new DefaultPathHandler())
            .handler("/echo", new EchoWebTransportHandler())
            .handler("/chat", new WebTransportChatHandler())
            .handler("/test", new WebTransportTestHandler());

    // Auto-detect Server ID for QUIC Connection ID Routing (QUIC-LB)
    final String serverIdStr = System.getenv("SERVER_ID");
    int serverId = -1;
    if (serverIdStr != null && !serverIdStr.trim().isEmpty()) {
      try {
        serverId = Integer.parseInt(serverIdStr.trim());
      } catch (NumberFormatException expected) {
      }
    } else if (nodeName.contains("-")) {
      String suffix = nodeName.substring(nodeName.lastIndexOf('-') + 1);
      try {
        serverId = Integer.parseInt(suffix);
      } catch (NumberFormatException expected) {
      }
    }
    if (serverId < 0) {
      serverId = Math.abs(nodeName.hashCode()) % 254 + 1;
    }
    log.info(
        "🎯 Configured QUIC Connection ID Routing (QUIC-LB) for node '{}' with Server ID: {}",
        nodeName,
        serverId);
    serverBuilder.serverId(serverId);
    final int configuredServerId = serverId;

    // Optional Production TLS Certificate Configuration
    final String sslKeyPath =
        System.getenv("SSL_KEY_PATH") != null
            ? System.getenv("SSL_KEY_PATH")
            : System.getProperty("webtransport4j.ssl.key.path");
    final String sslCertPath =
        System.getenv("SSL_CERT_PATH") != null
            ? System.getenv("SSL_CERT_PATH")
            : System.getProperty("webtransport4j.ssl.cert.path");
    if (sslKeyPath != null && sslCertPath != null && !sslKeyPath.trim().isEmpty() && !sslCertPath.trim().isEmpty()) {
      log.info("🔒 Loaded TLS certificate: key={}, cert={}", sslKeyPath, sslCertPath);
      serverBuilder.ssl(sslKeyPath.trim(), sslCertPath.trim());
    }

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
      serverBuilder.metricsListener(otlpListener);
    }

    final WebTransportServer server = serverBuilder.build();

    // 3. Start auxiliary HTTP server for Kubernetes probes and live management API
    HttpServer healthHttpServer =
        HttpServer.create(new InetSocketAddress(managementBindHost, healthPort), 0);
    if (managementToken == null || managementToken.trim().isEmpty()) {
      log.warn(
          "Management mutation endpoints are disabled: MANAGEMENT_AUTH_TOKEN is not configured.");
    }
    healthHttpServer.createContext(
        "/healthz",
        exchange -> {
          byte[] resp =
              ("{\"status\":\"UP\",\"node\":\"" + escapeJson(nodeName) + "\"}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(resp);
          }
        });

    healthHttpServer.createContext(
        "/readyz",
        exchange -> {
          boolean ready = server.isAcceptingSessions();
          byte[] resp = ("{\"ready\":" + ready + "}").getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(ready ? 200 : 503, resp.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(resp);
          }
        });

    // Kubernetes preStop hook; remote callers use the node management credential.
    healthHttpServer.createContext(
        "/prestop",
        exchange -> {
          if (!isAuthorized(exchange, managementToken)) {
            return;
          }
          log.info("🛑 Received Kubernetes preStop hook: draining all active sessions on node '{}'", nodeName);
          server.drain();
          byte[] resp = "{\"status\":\"DRAINING\",\"ready\":false}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(resp);
          }
        });

    // JVM graceful shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info(
                      "🛑 JVM shutdown hook triggered for node '{}': draining and stopping server",
                      nodeName);
                  try {
                    server.close();
                    healthHttpServer.stop(1);
                  } catch (Exception ignored) {
                    // Continue best-effort diagnostics or cleanup if this operation is unavailable.
                  }
                }));

    // Jolokia / Hawtio JMX management HTTP handlers
    JolokiaHttpHandler jolokiaHandler = new JolokiaHttpHandler(nodeName);
    healthHttpServer.createContext("/jolokia", exchange -> {
      if (isAuthorized(exchange, managementToken)) {
        jolokiaHandler.handle(exchange);
      }
    });
    healthHttpServer.createContext("/api/node/jmx", exchange -> {
      if (isAuthorized(exchange, managementToken)) {
        jolokiaHandler.handle(exchange);
      }
    });

    // Node runtime info endpoint: strictly real values from JVM and Netty server
    healthHttpServer.createContext(
        "/api/node/info",
        exchange -> {
          Runtime rt = Runtime.getRuntime();
          long totalMem = rt.totalMemory();
          long freeMem = rt.freeMemory();
          final long usedMem = totalMem - freeMem;
          final long maxMem = rt.maxMemory();
          final long uptime = ManagementFactory.getRuntimeMXBean().getUptime();

          // Real-time Garbage Collector & Generational ZGC introspection from JVM
          java.util.List<java.lang.management.GarbageCollectorMXBean> gcBeans =
              ManagementFactory.getGarbageCollectorMXBeans();
          boolean isZgcMinor = false;
          boolean isZgcMajor = false;
          boolean isZgcSingle = false;
          boolean isG1 = false;
          boolean isParallel = false;
          boolean isShenandoah = false;
          StringBuilder gcBeansJson = new StringBuilder("[");
          for (int i = 0; i < gcBeans.size(); i++) {
            java.lang.management.GarbageCollectorMXBean b = gcBeans.get(i);
            String name = b.getName();
            if (i > 0) {
              gcBeansJson.append(",");
            }
            gcBeansJson.append(
                String.format(
                    "{\"name\":\"%s\",\"collections\":%d,\"timeMs\":%d}",
                    escapeJson(name), b.getCollectionCount(), b.getCollectionTime()));
            String lower = name.toLowerCase();
            if (lower.contains("zgc minor")) {
              isZgcMinor = true;
            }
            if (lower.contains("zgc major")) {
              isZgcMajor = true;
            }
            if (lower.contains("zgc") && !lower.contains("minor") && !lower.contains("major")) {
              isZgcSingle = true;
            }
            if (lower.contains("g1")) {
              isG1 = true;
            }
            if (lower.contains("parallel")) {
              isParallel = true;
            }
            if (lower.contains("shenandoah")) {
              isShenandoah = true;
            }
          }
          gcBeansJson.append("]");

          String gcType;
          if (isZgcMinor || isZgcMajor) {
            gcType = "Generational ZGC";
          } else if (isZgcSingle) {
            gcType = "ZGC";
          } else if (isG1) {
            gcType = "G1 GC";
          } else if (isParallel) {
            gcType = "Parallel GC";
          } else if (isShenandoah) {
            gcType = "Shenandoah GC";
          } else if (!gcBeans.isEmpty()) {
            gcType = gcBeans.get(0).getName();
          } else {
            gcType = "Serial GC";
          }

          int javaFeature = io.netty.util.internal.PlatformDependent.javaVersion();
          String javaVersion = System.getProperty("java.version", String.valueOf(javaFeature));
          String javaVmName = System.getProperty("java.vm.name", "Java HotSpot");
          String javaVendor = System.getProperty("java.vendor", "Oracle Corporation");
          String jvmDisplayName = String.format("Java %d · %s", javaFeature, gcType);

          String envGc = System.getenv("JAVA_GC");
          if (envGc == null || envGc.trim().isEmpty()) {
            envGc = System.getenv("WT4J_GC");
          }
          if (envGc == null || envGc.trim().isEmpty()) {
            envGc = "ZGC";
          }

          String json =
              String.format(
                  "{\"status\":\"HEALTHY\",\"nodeId\":\"%s\",\"nodeName\":\"%s\",\"quicPort\":%d,\"healthPort\":%d,"
                      + "\"isStarted\":%b,\"activeSessions\":%d,\"uptimeMs\":%d,\"uptimeSeconds\":%d,"
                      + "\"jolokiaUrl\":\"http://127.0.0.1:%d/jolokia\","
                      + "\"jvm\":{\"displayName\":\"%s\",\"gc\":\"%s\",\"gcType\":\"%s\",\"isGenerationalZgc\":%b,"
                      + "\"configuredGc\":\"%s\","
                      + "\"version\":\"%s\",\"feature\":%d,\"vendor\":\"%s\",\"vmName\":\"%s\",\"gcBeans\":%s},"
                      + "\"os\":{\"name\":\"%s\",\"arch\":\"%s\",\"processors\":%d},"
                      + "\"memory\":{\"used\":%d,\"total\":%d,\"max\":%d,\"free\":%d}}",
                  escapeJson(nodeName),
                  escapeJson(nodeName),
                  quicPort,
                  healthPort,
                  server.isStarted(),
                  server.getActiveSessionCount(),
                  uptime,
                  uptime / 1000L,
                  healthPort,
                  escapeJson(jvmDisplayName),
                  escapeJson(gcType),
                  escapeJson(gcType),
                  (isZgcMinor || isZgcMajor),
                  escapeJson(envGc),
                  escapeJson(javaVersion),
                  javaFeature,
                  escapeJson(javaVendor),
                  escapeJson(javaVmName),
                  gcBeansJson,
                  escapeJson(System.getProperty("os.name", "unknown")),
                  escapeJson(System.getProperty("os.arch", "unknown")),
                  rt.availableProcessors(),
                  usedMem,
                  totalMem,
                  maxMem,
                  freeMem);

          byte[] resp = json.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(resp);
          }
        });

    // Real active sessions and streams endpoint: directly from server.getActiveSessions()
    healthHttpServer.createContext(
        "/api/node/sessions",
        exchange -> {
          StringBuilder sb = new StringBuilder();
          sb.append("{\"sessions\":[");
          boolean firstSess = true;
          for (WebTransportSession session : server.getActiveSessions()) {
            if (!firstSess) {
              sb.append(",");
            }
            firstSess = false;

            final long sid = session.getSessionStreamId();
            final String path = session.path();
            final String subproto =
                session.getSubprotocol() != null ? session.getSubprotocol() : "webtransport";
            final SocketAddress remoteAddr = session.getRemoteAddress();
            final String remote =
                remoteAddr != null ? remoteAddr.toString().replaceFirst("^/", "") : "unknown";
            final SocketAddress localAddr = session.getLocalAddress();
            final String local =
                localAddr != null
                    ? localAddr.toString().replaceFirst("^/", "")
                    : "0.0.0.0:" + quicPort;

            final long uniqueSessionId = session.getUniqueSessionId();
            String uniqueId = String.valueOf(uniqueSessionId);
            sb.append("{");
            sb.append("\"id\":\"wt-sess-").append(uniqueId).append("\",");
            sb.append("\"nodeId\":\"").append(escapeJson(nodeName)).append("\",");
            sb.append("\"nodeName\":\"").append(escapeJson(nodeName)).append("\",");
            sb.append("\"serverId\":").append(configuredServerId).append(",");
            sb.append("\"quicPort\":").append(quicPort).append(",");
            sb.append("\"serverNode\":\"")
                .append(escapeJson(nodeName))
                .append(" (Server ID: ")
                .append(configuredServerId)
                .append(" · Port ")
                .append(quicPort)
                .append(")\",");
            sb.append("\"sessionId\":").append(uniqueId).append(",");
            sb.append("\"uniqueSessionId\":").append(uniqueId).append(",");
            sb.append("\"rawSessionId\":").append(sid).append(",");
            sb.append("\"path\":\"").append(escapeJson(path)).append("\",");
            sb.append("\"subprotocol\":\"").append(escapeJson(subproto)).append("\",");
            sb.append("\"remoteEndpoint\":\"").append(escapeJson(remote)).append("\",");
            sb.append("\"clientEndpoint\":\"").append(escapeJson(remote)).append("\",");
            sb.append("\"localEndpoint\":\"").append(escapeJson(local)).append("\",");
            final String sessStatus;
            if (!session.isOpen()) {
              sessStatus = "CLOSED";
            } else if (session.isDraining()) {
              sessStatus = "DRAINING";
            } else {
              sessStatus = "CONNECTED";
            }
            sb.append("\"status\":\"").append(sessStatus).append("\",");
            sb.append("\"isDraining\":").append(session.isDraining()).append(",");
            sb.append("\"bytesSent\":").append(session.getCumulativeBytesSent()).append(",");
            sb.append("\"bytesReceived\":")
                .append(session.getCumulativeBytesReceived())
                .append(",");
            sb.append("\"flowControl\":{");
            sb.append("\"enabled\":").append(session.isFlowControlEnabled()).append(",");
            sb.append("\"maxData\":").append(session.getSettingsMaxData()).append(",");
            sb.append("\"maxStreamsBidi\":")
                .append(session.getSettingsMaxStreamsBidi())
                .append(",");
            sb.append("\"maxStreamsUni\":").append(session.getSettingsMaxStreamsUni()).append(",");
            sb.append("\"peerMaxData\":").append(session.getPeerSettingsMaxData()).append(",");
            sb.append("\"peerMaxStreamsBidi\":")
                .append(session.getPeerSettingsMaxStreamsBidi())
                .append(",");
            sb.append("\"peerMaxStreamsUni\":").append(session.getPeerSettingsMaxStreamsUni());
            sb.append("},");

            sb.append("\"streams\":[");
            // Extended CONNECT stream
            final String connectStreamStatus;
            if (!session.isOpen()) {
              connectStreamStatus = "CLOSED";
            } else if (session.isDraining()) {
              connectStreamStatus = "DRAINING";
            } else {
              connectStreamStatus = "ESTABLISHED";
            }
            sb.append("{");
            sb.append("\"streamId\":").append(sid).append(",");
            sb.append("\"type\":\"connect\",");
            sb.append("\"initiator\":\"client\",");
            sb.append("\"status\":\"").append(connectStreamStatus).append("\",");
            sb.append("\"bytesSent\":").append(session.getCumulativeBytesSent()).append(",");
            sb.append("\"bytesReceived\":")
                .append(session.getCumulativeBytesReceived())
                .append(",");
            sb.append("\"lastMessage\":\"CONNECT ")
                .append(escapeJson(path))
                .append(" HTTP/3 :protocol=webtransport\"");
            sb.append("}");

            // Active bidirectional and unidirectional streams
            for (WebTransportStreamSummary st : session.getActiveStreams()) {
              sb.append(",{");
              sb.append("\"streamId\":").append(st.streamId()).append(",");
              sb.append("\"type\":\"").append(st.isBidirectional() ? "bidi" : "uni").append("\",");
              sb.append("\"initiator\":\"")
                  .append(st.isLocalCreated() ? "server" : "client")
                  .append("\",");
              sb.append("\"status\":\"").append(st.isActive() ? "OPEN" : "CLOSED").append("\",");
              sb.append("\"isOpen\":").append(st.isOpen()).append(",");
              sb.append("\"isWritable\":").append(st.isWritable());
              sb.append("}");
            }
            sb.append("]");
            sb.append("}");
          }
          sb.append("]}");

          byte[] resp = sb.toString().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(resp);
          }
        });

    // Drain session endpoint (RFC 9297 Section 5.3)
    healthHttpServer.createContext(
        "/api/node/sessions/drain",
        exchange -> {
          if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
            exchange
                .getResponseHeaders()
                .set("Access-Control-Allow-Headers", "Content-Type, Authorization");
            exchange.sendResponseHeaders(204, -1);
            return;
          }
          if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            if (!isAuthorized(exchange, managementToken)) {
              return;
            }
            String body;
            try {
              body = new String(readBytes(exchange.getRequestBody()), StandardCharsets.UTF_8);
            } catch (java.io.IOException e) {
              exchange.sendResponseHeaders(413, -1);
              return;
            }
            long targetSessionId = -1;
            Matcher m = Pattern.compile("\"sessionId\"\\s*:\\s*(\\d+)").matcher(body);
            if (m.find()) {
              targetSessionId = Long.parseLong(m.group(1));
            } else {
              Matcher m2 = Pattern.compile("\"id\"\\s*:\\s*\"wt-sess-(\\d+)\"").matcher(body);
              if (m2.find()) {
                targetSessionId = Long.parseLong(m2.group(1));
              }
            }
            boolean drainAll =
                body.contains("\"all\"")
                    || body.contains("\"drainAll\":true")
                    || body.contains("\"drainAll\": true");
            boolean drained = false;
            if (drainAll) {
              drained = true;
              for (WebTransportSession s : server.getActiveSessions()) {
                try {
                  s.drain();
                } catch (Exception expected) {
                  drained = false;
                }
              }
            } else if (targetSessionId >= 0) {
              WebTransportSession sess = server.getSession(targetSessionId);
              if (sess == null) {
                for (WebTransportSession s : server.getActiveSessions()) {
                  if (s.getUniqueSessionId() == targetSessionId
                      || Math.abs(System.identityHashCode(s)) == targetSessionId
                      || s.getSessionStreamId() == targetSessionId) {
                    sess = s;
                    break;
                  }
                }
              }
              if (sess != null) {
                sess.drain();
                drained = true;
              }
            }
            String res = "{\"success\":" + drained + ",\"drained\":" + drained + "}";
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(drained ? 200 : drainAll ? 503 : 404, res.length());
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(res.getBytes(StandardCharsets.UTF_8));
            }
          }
        });

    // Close session endpoint
    healthHttpServer.createContext(
        "/api/node/sessions/close",
        exchange -> {
          if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
            exchange
                .getResponseHeaders()
                .set("Access-Control-Allow-Headers", "Content-Type, Authorization");
            exchange.sendResponseHeaders(204, -1);
            return;
          }
          if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            if (!isAuthorized(exchange, managementToken)) {
              return;
            }
            String body;
            try {
              body = new String(readBytes(exchange.getRequestBody()), StandardCharsets.UTF_8);
            } catch (java.io.IOException e) {
              exchange.sendResponseHeaders(413, -1);
              return;
            }
            long targetSessionId = -1;
            Matcher m = Pattern.compile("\"sessionId\"\\s*:\\s*(\\d+)").matcher(body);
            if (m.find()) {
              targetSessionId = Long.parseLong(m.group(1));
            } else {
              Matcher m2 = Pattern.compile("\"id\"\\s*:\\s*\"wt-sess-(\\d+)\"").matcher(body);
              if (m2.find()) {
                targetSessionId = Long.parseLong(m2.group(1));
              }
            }
            boolean closeAll =
                body.contains("\"all\"")
                    || body.contains("\"closeAll\":true")
                    || body.contains("\"closeAll\": true");
            boolean closed = false;
            if (closeAll) {
              java.util.List<WebTransportSession> allSessions =
                  new java.util.ArrayList<>(server.getActiveSessions());
              for (WebTransportSession s : allSessions) {
                try {
                  s.close();
                  server.unregisterSession(s);
                } catch (Exception expected) {
                }
              }
              closed = true;
            } else if (targetSessionId >= 0) {
              WebTransportSession sess = server.getSession(targetSessionId);
              if (sess == null) {
                for (WebTransportSession s : server.getActiveSessions()) {
                  if (s.getUniqueSessionId() == targetSessionId
                      || Math.abs(System.identityHashCode(s)) == targetSessionId
                      || s.getSessionStreamId() == targetSessionId) {
                    sess = s;
                    break;
                  }
                }
              }
              if (sess != null) {
                try {
                  sess.close();
                  server.unregisterSession(sess);
                  closed = true;
                } catch (Exception expected) {
                }
              }
            }
            String respJson =
                String.format(
                    "{\"success\":%b,\"sessionId\":%d,\"closeAll\":%b}",
                    closed, targetSessionId, closeAll);
            byte[] resp = respJson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(resp);
            }
          } else {
            exchange.sendResponseHeaders(405, -1);
          }
        });

    healthHttpServer.setExecutor(null);
    healthHttpServer.start();
    log.info(
        "✅ Kubernetes liveness/readiness probes & management API listening on port {}", healthPort);

    // 4. Graceful shutdown hook
    final WebTransportOtlpMetricsListener finalOtlp = otlpListener;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
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
