package io.github.webtransport4j.example.lichess.server;

import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server wrapper hosting the Lichess WebTransport reproduction on WebTransport4J.
 */
public class LilaWebTransportServer {

  private static final Logger log = LoggerFactory.getLogger(LilaWebTransportServer.class);

  private final String host;
  private final int requestedPort;
  private final LilaWebTransportHandler handler;
  private WebTransportServer server;

  /**
   * Constructs a Lila WebTransport server.
   *
   * @param host host address
   * @param port port number (0 for dynamic port)
   */
  public LilaWebTransportServer(@NonNull String host, int port) {
    this.host = host;
    this.requestedPort = port;
    this.handler = new LilaWebTransportHandler();
  }

  /**
   * Starts the WebTransport server.
   *
   * @throws Exception if startup fails
   */
  public void start() throws Exception {
    System.setProperty("webtransport4j.dev_mode", "true");
    System.setProperty("webtransport4j.dispatch.execution.mode", "NETTY_EVENT_LOOP");
    System.setProperty("webtransport4j.quic.active.migration.enabled", "true");

    server = new WebTransportServerBuilder()
        .host(host)
        .port(requestedPort)
        .defaultHandler(handler)
        .build();

    server.registerHandler("/round/play", handler);
    server.registerHandler("/round/watch", handler);
    server.registerHandler("/lobby", handler);
    server.registerHandler("/chess", handler);

    server.start();
    log.info("Started Lila WebTransport Server on {}:{}", host, server.getPort());
  }

  /**
   * Stops the server.
   */
  public void stop() {
    if (server != null) {
      server.stop();
    }
  }

  public int getPort() {
    return server != null ? server.getPort() : 0;
  }

  public @NonNull LilaWebTransportHandler getHandler() {
    return handler;
  }
}
