package io.github.webtransport4j.example;

import static org.junit.Assert.assertEquals;
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
}
