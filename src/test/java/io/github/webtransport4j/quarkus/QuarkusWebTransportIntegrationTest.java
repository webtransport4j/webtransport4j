package io.github.webtransport4j.quarkus;

import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.server.WebTransportServer;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertNotNull(manager);
    WebTransportServer server = manager.getServer();
    Assert.assertNotNull(server);

    WebTransportHandler registered = server.getHandler("/quarkus-stream");
    Assert.assertEquals(endpoint, registered);

    manager.onStartup();
    Assert.assertTrue(server.isStarted());
    Assert.assertTrue(server.getPort() > 0);

    manager.onShutdown();
    Assert.assertFalse(server.isStarted());
  }
}
