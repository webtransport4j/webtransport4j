package io.github.webtransport4j.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import org.junit.Test;

/** Regression coverage for authenticated and restricted node management. */
public class NodeManagementSecurityTest {
  @Test
  public void requiresTokenEvenForLoopbackAndRestrictsOperations() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    JolokiaHttpHandler handler = new JolokiaHttpHandler("test");
    for (String path : new String[] {"/jolokia", "/api/node/jmx"}) {
      server.createContext(path, exchange -> {
        if (ClusterNodeSample.isAuthorized(exchange, "test-secret")) {
          handler.handle(exchange);
        }
        exchange.close();
      });
    }
    server.start();
    try {
      for (String path : new String[] {"/jolokia", "/api/node/jmx"}) {
        HttpURLConnection connection = open(server, path + "/version");
        assertEquals(401, connection.getResponseCode());
        connection.disconnect();
        connection = open(server, path + "/version");
        connection.setRequestProperty("Authorization", "Bearer wrong");
        assertEquals(401, connection.getResponseCode());
        connection.disconnect();
        connection = open(server, path + "/version");
        connection.setRequestProperty("Authorization", "Bearer test-secret");
        assertEquals(200, connection.getResponseCode());
        assertNull(connection.getHeaderField("Access-Control-Allow-Origin"));
        connection.disconnect();
        connection = open(server, path + "/exec/java.lang:type=Threading/resetPeakThreadCount");
        connection.setRequestProperty("Authorization", "Bearer test-secret");
        connection.setRequestMethod("POST");
        java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream();
        try (java.io.InputStream input = connection.getInputStream()) {
          byte[] buffer = new byte[1024];
          int count;
          while ((count = input.read(buffer)) != -1) {
            body.write(buffer, 0, count);
          }
        }
        assertTrue(body.toString("UTF-8").contains("\"status\":403"));
        connection.disconnect();
      }
    } finally {
      server.stop(0);
    }
  }

  private HttpURLConnection open(HttpServer server, String path) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + server.getAddress().getPort() + path).openConnection();
    connection.setConnectTimeout(3000);
    connection.setReadTimeout(3000);
    return connection;
  }

  @Test(expected = IllegalStateException.class)
  public void missingSecretFailsClosed() {
    ClusterNodeSample.requireSecret("WT4J_TEST_SECRET_THAT_IS_NOT_CONFIGURED");
  }

  @Test
  public void requiresTokenForNodeInfoAndSessions() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    for (String path : new String[] {"/api/node/info", "/api/node/sessions"}) {
      server.createContext(path, exchange -> {
        if (!ClusterNodeSample.isAuthorized(exchange, "test-secret")) {
          return;
        }
        byte[] resp = "{\"status\":\"ok\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, resp.length);
        try (java.io.OutputStream os = exchange.getResponseBody()) {
          os.write(resp);
        }
      });
    }
    server.start();
    try {
      for (String path : new String[] {"/api/node/info", "/api/node/sessions"}) {
        HttpURLConnection connection = open(server, path);
        assertEquals(401, connection.getResponseCode());
        assertEquals("Bearer", connection.getHeaderField("WWW-Authenticate"));
        connection.disconnect();

        connection = open(server, path);
        connection.setRequestProperty("Authorization", "Bearer invalid-token");
        assertEquals(401, connection.getResponseCode());
        connection.disconnect();

        connection = open(server, path);
        connection.setRequestProperty("Authorization", "Bearer test-secret");
        assertEquals(200, connection.getResponseCode());
        connection.disconnect();
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void jolokiaOptionsPreflightDoesNotRequireAuth() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    JolokiaHttpHandler handler = new JolokiaHttpHandler("test");
    for (String path : new String[] {"/jolokia", "/api/node/jmx"}) {
      server.createContext(path, exchange -> {
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
          handler.handle(exchange);
          return;
        }
        if (ClusterNodeSample.isAuthorized(exchange, "test-secret")) {
          handler.handle(exchange);
        }
        exchange.close();
      });
    }
    server.start();
    try {
      for (String path : new String[] {"/jolokia", "/api/node/jmx"}) {
        HttpURLConnection connection = open(server, path + "/version");
        connection.setRequestMethod("OPTIONS");
        assertEquals(204, connection.getResponseCode());
        assertEquals("GET, POST, OPTIONS", connection.getHeaderField("Access-Control-Allow-Methods"));
        assertNotNull(connection.getHeaderField("Access-Control-Allow-Headers"));
        connection.disconnect();
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void drainAndCloseEndpointsIncludeCorsAllowOrigin() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    for (String path : new String[] {"/api/node/sessions/drain", "/api/node/sessions/close"}) {
      server.createContext(path, exchange -> {
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
          exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
          exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
          exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
          exchange.sendResponseHeaders(204, -1);
          return;
        }
        if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
          exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
          if (!ClusterNodeSample.isAuthorized(exchange, "test-secret")) {
            return;
          }
          byte[] resp = "{\"success\":true}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          try (java.io.OutputStream os = exchange.getResponseBody()) {
            os.write(resp);
          }
        }
      });
    }
    server.start();
    try {
      for (String path : new String[] {"/api/node/sessions/drain", "/api/node/sessions/close"}) {
        HttpURLConnection optionsConn = open(server, path);
        optionsConn.setRequestMethod("OPTIONS");
        assertEquals(204, optionsConn.getResponseCode());
        assertEquals("*", optionsConn.getHeaderField("Access-Control-Allow-Origin"));
        optionsConn.disconnect();

        HttpURLConnection postConn = open(server, path);
        postConn.setRequestMethod("POST");
        postConn.setRequestProperty("Authorization", "Bearer test-secret");
        postConn.setDoOutput(true);
        try (java.io.OutputStream os = postConn.getOutputStream()) {
          os.write("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        assertEquals(200, postConn.getResponseCode());
        assertEquals("*", postConn.getHeaderField("Access-Control-Allow-Origin"));
        postConn.disconnect();
      }
    } finally {
      server.stop(0);
    }
  }
}
