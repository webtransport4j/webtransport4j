package io.github.webtransport4j.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class WebTransportServerTransportTest {

    private WebTransportServer server;

    @Before
    public void setUp() {
        server = new WebTransportServer();
        // Register a dummy handler so the server can start
        server.registerHandler("/test", new io.github.webtransport4j.api.WebTransportHandler() {});
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    private void startServerAsyncAndVerify() throws Exception {
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.start();
        
        long timeout = System.currentTimeMillis() + 15000;
        while (server.getPort() == 0 && System.currentTimeMillis() < timeout) {
            Thread.sleep(50);
        }
        assertTrue("Server should start successfully", server.getPort() > 0);
        server.stop();
        t.join(2000);
    }

    @Test
    public void testAutoTransport() throws Exception {
        System.setProperty("webtransport4j.server.transport", "auto");
        System.setProperty("webtransport4j.server.port", "0"); // Use random port
        try {
            startServerAsyncAndVerify();
        } finally {
            System.clearProperty("webtransport4j.server.transport");
            System.clearProperty("webtransport4j.server.port");
        }
    }

    @Test
    public void testEpollTransportFallback() throws Exception {
        System.setProperty("webtransport4j.server.transport", "epoll");
        System.setProperty("webtransport4j.server.port", "0"); // Use random port
        try {
            startServerAsyncAndVerify();
        } finally {
            System.clearProperty("webtransport4j.server.transport");
            System.clearProperty("webtransport4j.server.port");
        }
    }

    @Test
    public void testKQueueTransportFallback() throws Exception {
        System.setProperty("webtransport4j.server.transport", "kqueue");
        System.setProperty("webtransport4j.server.port", "0"); // Use random port
        try {
            startServerAsyncAndVerify();
        } finally {
            System.clearProperty("webtransport4j.server.transport");
            System.clearProperty("webtransport4j.server.port");
        }
    }

    @Test
    public void testIOUringTransportFallback() throws Exception {
        System.setProperty("webtransport4j.server.transport", "iouring");
        System.setProperty("webtransport4j.server.port", "0"); // Use random port
        try {
            startServerAsyncAndVerify();
        } finally {
            System.clearProperty("webtransport4j.server.transport");
            System.clearProperty("webtransport4j.server.port");
        }
    }
}
