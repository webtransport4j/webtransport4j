package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import static org.junit.Assert.*;

public class WebTransportDispatchModeTest {

    @Before
    public void setUp() {
        System.setProperty("webtransport4j.dispatch.execution.mode", "NETTY_EVENT_EXECUTOR_GROUP");
        System.setProperty("webtransport4j.netty.executor.group.size", "8");
        System.setProperty("webtransport4j.server.port", "0"); // random port
    }

    @After
    public void tearDown() {
        System.clearProperty("webtransport4j.dispatch.execution.mode");
        System.clearProperty("webtransport4j.netty.executor.group.size");
        System.clearProperty("webtransport4j.server.port");
    }

    @Test
    public void testNettyEventExecutorGroupConfig() throws Exception {
        WebTransportServer server = new WebTransportServer(new WebTransportHandler() {});
        try {
            ExecutorService executor = server.getBusinessExecutor();
            assertNotNull("Executor should not be null in NETTY_EVENT_EXECUTOR_GROUP mode", executor);
            assertTrue("Executor should be an instance of DefaultEventExecutorGroup", 
                       executor instanceof DefaultEventExecutorGroup);
        } finally {
            server.stop();
        }
    }
}
