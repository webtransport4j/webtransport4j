package io.github.webtransport4j.example;

import io.github.webtransport4j.api.LoggingWebTransportMetricsListener;
import io.github.webtransport4j.server.WebTransportServer;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.NonNull;

/** Sample server entry point for WebTransport. */
public class ServerSample {
  /** Main. */
  public static void main(@NonNull String[] args) throws Exception {
    WebTransportServer server = new WebTransportServer(new DefaultPathHandler());
    LoggingWebTransportMetricsListener listener =
        new LoggingWebTransportMetricsListener(60, TimeUnit.SECONDS);
    server.setMetricsListener(listener);
    server.registerHandler("/test", new WebTransportTestHandler());
    server.registerHandler("/chat", new WebTransportChatHandler());
    server.registerHandler("/echo", new EchoWebTransportHandler());
    try {
      server.start();
    } catch (Exception e) {
      listener.close();
      throw e;
    }
  }
}
