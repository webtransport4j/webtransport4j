package io.github.webtransport4j.server;

import io.github.webtransport4j.api.BinarySources;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.client.WebTransportClientTestSuite;
import io.netty.util.concurrent.Future;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class WebTransportClientTestSuiteTest {

    private static final Logger logger = LoggerFactory.getLogger(WebTransportClientTestSuiteTest.class);

    private WebTransportServer server;
    private int port;

    private static class CleanWebTransportTestHandler implements WebTransportHandler {
        @Override
        public void onSessionReady(@NonNull WebTransportSession session) {
            logger.info("🟢 [CLEAN TEST HANDLER] Session Ready: {}", session.path());

            // 1. Initiate server-to-client unidirectional stream with text greeting
            session.createUniStream().thenAccept(stream -> {
                stream.writeText("Hello from Server-Initiated Unidirectional Stream! [ID: " + stream.streamId() + "]");
            });

            // 2. Initiate server-to-client bidirectional stream with text greeting
            session.createBiStream().thenAccept(stream -> {
                stream.onData(data -> {
                    logger.info("   📩 Received response on server-initiated bidi stream {}: {}",
                            stream.streamId(), new String(data.readBytes(), StandardCharsets.UTF_8));
                });
                stream.writeText("Hello from Server-Initiated Bidirectional Stream! [ID: " + stream.streamId() + "]");
            });
        }

        @Override
        public void onSessionClosed(@NonNull WebTransportSession session) {
            logger.info("🔴 [CLEAN TEST HANDLER] Session Closed: {}", session.path());
        }

        @Override
        public void onIncomingStream(@NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
            boolean isBidi = stream.isBidirectional();
            stream.onData(data -> {
                byte[] bytes = data.readBytes();
                String content = new String(bytes, StandardCharsets.UTF_8);

                if (content.startsWith("SleepServer_")) {
                    logger.info("😴 Server sleeping on stream {}...", stream.streamId());
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    logger.info("⏰ Server awake on stream {}!", stream.streamId());
                }

                if (isBidi) {
                    if (!stream.hasAttribute("prefixed")) {
                        stream.setAttribute("prefixed", true);
                        byte[] prefix = "ACK BI: ".getBytes(StandardCharsets.UTF_8);
                        byte[] resp = new byte[prefix.length + bytes.length];
                        System.arraycopy(prefix, 0, resp, 0, prefix.length);
                        System.arraycopy(bytes, 0, resp, prefix.length, bytes.length);
                        stream.write(resp);
                    } else {
                        stream.write(bytes);
                    }
                } else {
                    session.createUniStream().thenAccept(ackStream -> {
                        byte[] prefix = "ACK UNI: ".getBytes(StandardCharsets.UTF_8);
                        byte[] resp = new byte[prefix.length + bytes.length];
                        System.arraycopy(prefix, 0, resp, 0, prefix.length);
                        System.arraycopy(bytes, 0, resp, prefix.length, bytes.length);
                        ackStream.write(resp).thenRun(ackStream::close);
                    });
                }
            });
        }

        @Override
        public void onDatagramReceived(@NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
            String content = new String(data.readBytes(), StandardCharsets.UTF_8);
            String replyText = "ACK DG: " + content;
            session.createUniStream().thenAccept(ackStream -> {
                ackStream.writeText(replyText).thenRun(ackStream::close);
            });
        }
    }

    private Thread serverThread;

    @Before
    public void setUp() throws Exception {
        System.setProperty("webtransport4j.webtransport.enable_server_push", "false");
        System.setProperty("webtransport4j.server.port", "0");
        server = new WebTransportServer();
        server.registerHandler("/test", new CleanWebTransportTestHandler());
        serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                logger.error("Server start error", e);
            }
        });
        serverThread.start();

        long timeout = System.currentTimeMillis() + 15000;
        while (server.getPort() == 0 && System.currentTimeMillis() < timeout) {
            Thread.sleep(50);
        }
        port = server.getPort();
        logger.info("Server asynchronously started on port: {}", port);
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (serverThread != null) {
            serverThread.join(2000);
        }
    }

    @Test
    public void testClientTestSuiteFullRun() throws Exception {
        // Execute the replicated interop suite against the locally running test server
        WebTransportClientTestSuite.main("https://127.0.0.1:" + port + "/test");
    }
}
