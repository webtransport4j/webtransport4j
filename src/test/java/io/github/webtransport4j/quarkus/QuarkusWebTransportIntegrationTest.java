package io.github.webtransport4j.quarkus;

import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.server.WebTransportServer;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Integration test for Quarkus and CDI WebTransport manager lifecycle. */
public class QuarkusWebTransportIntegrationTest {

  /** Sample endpoint component for Quarkus test. */
  @WebTransportEndpoint(path = "/quarkus-stream")
  public static class QuarkusEndpoint implements WebTransportHandler {}

  @Test
  public void testQuarkusManagerLifecycleAndDiscovery() throws Exception {
    QuarkusEndpoint endpoint = new QuarkusEndpoint();

    QuarkusWebTransportManager manager =
        QuarkusWebTransportManager.create(
            WebTransportServer.builder().port(0), Collections.singletonList(endpoint));

    Assertions.assertNotNull(manager);
    WebTransportServer server = manager.getServer();
    Assertions.assertNotNull(server);

    WebTransportHandler registered = server.getHandler("/quarkus-stream");
    Assertions.assertEquals(endpoint, registered);

    manager.onStartup();
    Assertions.assertTrue(server.isStarted());
    Assertions.assertTrue(server.getPort() > 0);

    manager.onShutdown();
    Assertions.assertFalse(server.isStarted());
  }
}
