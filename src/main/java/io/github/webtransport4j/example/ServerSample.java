package io.github.webtransport4j.example;

import io.github.webtransport4j.server.WebTransportServer;
import org.jspecify.annotations.NonNull;

public class ServerSample {

    public static void main(@NonNull String[] args) throws Exception {
        WebTransportServer server = new WebTransportServer(new DefaultPathHandler());
        server.registerHandler("/test", new WebTransportTestHandler());
        server.registerHandler("/chat", new WebTransportChatHandler());
        server.start();
    }
}
