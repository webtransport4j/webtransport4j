package io.github.webtransport4j.example;

import io.github.webtransport4j.api.LoggingWebTransportMetricsListener;
import io.github.webtransport4j.server.WebTransportServer;

import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;

public class ServerSample {

    public static void main(@NonNull String[] args) throws Exception {
        WebTransportServer server = new WebTransportServer(new DefaultPathHandler());
        server.setMetricsListener(new LoggingWebTransportMetricsListener(1, TimeUnit.SECONDS));
        server.registerHandler("/test", new WebTransportTestHandler());
        server.registerHandler("/chat", new WebTransportChatHandler());
        server.start();
    }
}
