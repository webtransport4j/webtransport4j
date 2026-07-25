package io.github.webtransport4j.server;

import io.github.webtransport4j.api.NoOpWebTransportMetricsListener;
import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/** Test for WebTransportServerBuilder functionality. */
public class WebTransportServerBuilderTest {

  @Test
  public void testBuilderConfigurationDefaultsAndOverrides() {
    WebTransportHandler customHandler = new WebTransportHandler() {};
    ReactiveWebTransportHandler customReactiveHandler = new ReactiveWebTransportHandler() {};

    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .host("127.0.0.1")
            .ssl("key.pem", "cert.pem")
            .allowedOrigins("https://example.com", "https://localhost")
            .transportType("nio")
            .idleTimeout(30, TimeUnit.SECONDS)
            .maxStreams(50, 50)
            .maxData(100000)
            .metricsListener(NoOpWebTransportMetricsListener.INSTANCE)
            .quicTokenHandler(InsecureQuicTokenHandler.INSTANCE)
            .businessExecutor(Executors.newSingleThreadExecutor())
            .defaultHandler(customHandler)
            .reactiveHandler("/chat", customReactiveHandler)
            .build();

    Assert.assertNotNull(server);
    Assert.assertEquals(8443, server.getPort());
    Assert.assertEquals("127.0.0.1", server.getHost());
    Assert.assertFalse(server.isStarted());
    Assert.assertNotNull(server.getHandler("/"));
    Assert.assertNotNull(server.getHandler("/chat"));
  }
}
